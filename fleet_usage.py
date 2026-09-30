"""Fleet usage statistics for Reachy Mini Central.

``UsageTracker`` aggregates producer / session activity into UTC-aligned
windows plus a daily rollup; ``FleetUsagePublisher`` pushes the frozen
rows to a public HF dataset (or to a local directory in dev) together
with a small chart-only ``summary.json``, which the status page fetches
straight from huggingface.co. Metric definitions, dataset layout and
operator setup live in ``docs/FLEET_USAGE.md``.

Nothing published is derived from raw ``meta`` strings: robots are
counted per bounded robot kind (``robot_kinds.robot_kind_of``) and
identified in memory only, by a fixed-size digest.

``app`` owns the wiring: the ``SignalingServer`` hooks (each wrapped so a
tracking failure never reaches signalling), the lifespan tasks, the dev
summary route and the ``$usage_section`` substitution.
"""

import asyncio
import bisect
import functools
import hashlib
import json
import logging
import operator
import os
import re
import tempfile
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from string import Template
from typing import Callable, Optional

from robot_kinds import PUBLIC_ROBOT_KINDS, ROBOT_KIND_LABELS, robot_kind_of

logger = logging.getLogger(__name__)


# --- Constants ----------------------------------------------------------

USAGE_SCHEMA_VERSION = 1
# Aggregation window. Fixed at 600 s whenever the HF dataset is the sink
# (one window length per dataset); only the dev local-dir sink honours
# ``FLEET_USAGE_WINDOW_SECONDS``.
DEFAULT_USAGE_WINDOW_SECONDS = 600
DEFAULT_USAGE_PUBLISH_SECONDS = 1800.0
MIN_USAGE_PUBLISH_SECONDS = 5.0
# Upper bounds (inclusive) of the session-duration histogram buckets; the
# histogram has one extra trailing bucket for durations above the last.
SESSION_DURATION_BUCKETS_S = (10, 60, 300, 900, 3600)
# Bounded session end-reason categories. The ``reason`` string on an
# ``endSession`` message is client-controlled and never published; the
# category comes from which server code path ended the session.
SESSION_END_ENDED = "ended"  # explicit endSession from a peer
SESSION_END_WITHDRAWN = "withdrawn"  # producer sent setPeerStatus(roles=[])
SESSION_END_PEER_DISCONNECTED = "peer_disconnected"  # an SSE channel closed
SESSION_END_SWEPT = "swept"  # stale-producer sweep eviction
SESSION_END_REPLACED = "replaced"  # stable-id collision evicted the producer
SESSION_END_OTHER = "other"
SESSION_END_REASONS = (
    SESSION_END_ENDED,
    SESSION_END_WITHDRAWN,
    SESSION_END_PEER_DISCONNECTED,
    SESSION_END_SWEPT,
    SESSION_END_REPLACED,
    SESSION_END_OTHER,
)
# ``meta.hardware_id`` is client-controlled. A value longer than this (or
# not a string) is not trusted as a hardware id: the robot falls back to
# the legacy owner+name identity.
USAGE_MAX_HARDWARE_ID_LEN = 128
# Caps on the distinct-robot key sets, per window and per day, so one
# account cycling fake hardware ids cannot grow memory or the public
# charts: at most this many distinct robots per HF username, and at most
# ``USAGE_MAX_KEYS_PER_SET`` robots per kind. Keys over a cap are counted
# as dropped and logged once per window / day.
USAGE_MAX_KEYS_PER_USER = 20
USAGE_MAX_KEYS_PER_SET = 20000
# Frozen rows waiting to be published. 2000 rows is ~13 days of 10-minute
# windows: a long HF outage drops the oldest rows instead of growing
# memory without bound.
USAGE_PENDING_MAX_ROWS = 2000
# ``summary.json`` horizons: window resolution over the last 24 h, hourly
# resolution over the last 7 days, and the last 365 daily rows.
USAGE_SUMMARY_RECENT_SECONDS = 86400
USAGE_SUMMARY_HOURLY_DAYS = 7
USAGE_SUMMARY_DAILY_ROWS = 365
# Window rows kept in memory (and downloaded at bootstrap): the hourly
# horizon plus one day of slack.
USAGE_WINDOW_RETENTION_DAYS = USAGE_SUMMARY_HOURLY_DAYS + 1
USAGE_PUBLISH_TIMEOUT_SECONDS = 120.0
USAGE_FINAL_PUBLISH_TIMEOUT_SECONDS = 10.0
# Per-request HTTP timeout for the huggingface_hub client (its shared
# client has no timeout by default).
USAGE_HF_HTTP_TIMEOUT_SECONDS = 30.0
# A failing tracker hook logs its traceback at most this often, so a bug
# hit on every heartbeat cannot flood the logs.
USAGE_HOOK_ERROR_LOG_INTERVAL_SECONDS = 60.0
FLEET_USAGE_DATASET_RE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*$"
)
USAGE_WINDOWS_DIR = "data/windows"
USAGE_DAILY_PATH = "data/daily.jsonl"
USAGE_SUMMARY_PATH = "summary.json"
DEV_USAGE_SUMMARY_ROUTE = "/dev/fleet-usage/summary.json"


# --- Helpers ------------------------------------------------------------


def _iso_utc(epoch: float) -> str:
    """Wall-clock epoch seconds -> ``YYYY-MM-DDTHH:MM:SSZ``."""
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_day(epoch: float) -> str:
    """Wall-clock epoch seconds -> UTC calendar day ``YYYY-MM-DD``."""
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%d")


def _parse_iso_utc(value: str) -> float:
    """``YYYY-MM-DDTHH:MM:SSZ`` -> epoch seconds."""
    return (
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
        .replace(tzinfo=timezone.utc)
        .timestamp()
    )


def usage_robot_key(username: str, meta: dict) -> bytes:
    """In-memory identity of a physical robot for distinct counting.

    An 8-byte BLAKE2b digest of ``meta.hardware_id`` when it is a string
    of 1..``USAGE_MAX_HARDWARE_ID_LEN`` characters (daemons >= v1.7.2 and
    Micro Duck). Otherwise the robot is a legacy Reachy Mini daemon and
    is identified by its owner plus its advertised name. The fixed size
    bounds memory whatever the client sends. Never published.
    """
    hardware_id = meta.get("hardware_id")
    if isinstance(hardware_id, str) and 0 < len(hardware_id) <= USAGE_MAX_HARDWARE_ID_LEN:
        material = "hw\0" + hardware_id
    else:
        material = f"legacy\0{username}\0{meta.get('name', '')}"
    return hashlib.blake2b(
        material.encode("utf-8", "surrogatepass"), digest_size=8
    ).digest()


def _kind_zeros() -> dict[str, int]:
    return {kind: 0 for kind in PUBLIC_ROBOT_KINDS}


# --- Tracker ------------------------------------------------------------


@dataclass
class _DistinctKeys:
    """Distinct robot keys per kind, capped per owner and per kind."""

    by_kind: dict[str, set[bytes]] = field(
        default_factory=lambda: {kind: set() for kind in PUBLIC_ROBOT_KINDS}
    )
    per_user: dict[str, int] = field(default_factory=dict)
    dropped: int = 0

    def add(self, kind: str, key: bytes, username: str) -> None:
        keys = self.by_kind[kind]
        if key in keys:
            return
        owned = self.per_user.get(username, 0)
        if owned >= USAGE_MAX_KEYS_PER_USER or len(keys) >= USAGE_MAX_KEYS_PER_SET:
            self.dropped += 1
            return
        keys.add(key)
        self.per_user[username] = owned + 1

    def counts(self) -> dict[str, int]:
        return {kind: len(keys) for kind, keys in self.by_kind.items()}


@dataclass
class _DurationAgg:
    """Count / sum / max / histogram of session durations (seconds, 0.1 s)."""

    count: int = 0
    sum_s: float = 0.0
    max_s: float = 0.0
    hist: list[int] = field(
        default_factory=lambda: [0] * (len(SESSION_DURATION_BUCKETS_S) + 1)
    )

    def add(self, duration_s: float) -> None:
        """Record one duration, already rounded to 0.1 s by the caller.

        Buckets are upper-bound inclusive: exactly 10 s lands in the
        first bucket, exactly 3600 s in the last bounded one.
        """
        self.count += 1
        self.sum_s += duration_s
        self.max_s = max(self.max_s, duration_s)
        self.hist[bisect.bisect_left(SESSION_DURATION_BUCKETS_S, duration_s)] += 1

    def as_dict(self) -> dict:
        return {
            "count": self.count,
            "sum_s": round(self.sum_s, 1),
            "max_s": round(self.max_s, 1),
            "hist": list(self.hist),
        }


def _duration_aggs() -> dict[str, _DurationAgg]:
    return {kind: _DurationAgg() for kind in PUBLIC_ROBOT_KINDS}


@dataclass
class _WindowAgg:
    """Accumulators for the currently open usage window."""

    start: int
    coverage_start: float
    distinct: _DistinctKeys = field(default_factory=_DistinctKeys)
    robots_peak: dict[str, int] = field(default_factory=_kind_zeros)
    sessions_started: dict[str, int] = field(default_factory=_kind_zeros)
    sessions_peak: dict[str, int] = field(default_factory=_kind_zeros)
    durations: dict[str, _DurationAgg] = field(default_factory=_duration_aggs)
    end_reasons: dict[str, int] = field(
        default_factory=lambda: {reason: 0 for reason in SESSION_END_REASONS}
    )


@dataclass
class _DayAgg:
    """Accumulators for the current UTC day.

    ``coverage_s`` sums the coverage of the day's already-closed windows;
    the open window's coverage is added when it closes (or when a partial
    snapshot is taken).
    """

    day: str
    coverage_s: float = 0.0
    distinct: _DistinctKeys = field(default_factory=_DistinctKeys)
    sessions_started: dict[str, int] = field(default_factory=_kind_zeros)
    durations: dict[str, _DurationAgg] = field(default_factory=_duration_aggs)


class UsageTracker:
    """Aggregate fleet usage into UTC-aligned windows and daily rows.

    Pure bookkeeping driven by ``SignalingServer`` hooks; it never touches
    the signalling structures. Window boundaries come from ``wall_clock``
    aligned on multiples of ``window_seconds`` since the epoch (UTC);
    session durations come from ``mono_clock``. Both are injectable so
    tests can drive time explicitly.

    Per window: distinct robots seen as connected producers, peak
    concurrent robots, sessions started, peak concurrent sessions (all
    per robot kind), durations of sessions that ended in the window and
    their end-reason categories. Per UTC day: distinct robots, sessions
    started and durations. See ``docs/FLEET_USAGE.md``.

    Rollover (``maybe_roll``) freezes the open window into ``pending`` and
    opens the next one seeded with the current state: currently connected
    robots count as seen, and the peaks start at the current concurrency.
    If whole windows were skipped (the loop did not run for longer than a
    window, e.g. the process was paused) nothing is emitted for them - a
    gap in the series means "not observed" - and the new window's
    coverage starts at the moment the rollover was noticed. A wall clock
    stepping backwards never reopens a past window; events keep
    accumulating into the open one.
    """

    def __init__(
        self,
        window_seconds: int = DEFAULT_USAGE_WINDOW_SECONDS,
        *,
        wall_clock: Callable[[], float] = time.time,
        mono_clock: Callable[[], float] = time.monotonic,
        pending_max_rows: int = USAGE_PENDING_MAX_ROWS,
    ):
        if (
            isinstance(window_seconds, bool)
            or not isinstance(window_seconds, int)
            or window_seconds <= 0
            or 86400 % window_seconds != 0
        ):
            raise ValueError(
                f"window_seconds must be a positive integer dividing 86400, got {window_seconds!r}"
            )
        self.window_seconds = window_seconds
        self._wall = wall_clock
        self._mono = mono_clock
        # peer_id -> (robot key, kind, username) for every registered producer.
        self._producers: dict[str, tuple[bytes, str, str]] = {}
        # session_id -> (monotonic start, producer kind at start).
        self._sessions: dict[str, tuple[float, str]] = {}
        self._active_sessions = _kind_zeros()
        # ("window" | "daily", row) tuples frozen by rollover, oldest first.
        self.pending: deque[tuple[str, dict]] = deque(maxlen=pending_max_rows)
        self.dropped_rows = 0
        now = self._wall()
        self._window = _WindowAgg(start=self._window_floor(now), coverage_start=now)
        self._day = _DayAgg(day=_utc_day(now))

    # -- time ----------------------------------------------------------

    def _window_floor(self, epoch: float) -> int:
        return int(epoch // self.window_seconds) * self.window_seconds

    def maybe_roll(self) -> None:
        """Close the open window (and day) if the wall clock has passed its end."""
        now = self._wall()
        prev_end = self._window.start + self.window_seconds
        if now < prev_end:
            return
        self._close_window(coverage_end=prev_end)
        new_start = self._window_floor(now)
        new_day = _utc_day(new_start)
        if new_day != self._day.day:
            self._log_dropped_keys(self._day.distinct, f"day {self._day.day}")
            self._emit("daily", self._day_row(self._day, self._day.coverage_s))
            self._day = _DayAgg(day=new_day)
            self._seed_distinct(self._day.distinct)
        skipped = new_start > prev_end
        self._window = _WindowAgg(
            start=new_start, coverage_start=now if skipped else new_start
        )
        self._seed_distinct(self._window.distinct)
        self._window.robots_peak.update(self._current_robot_counts())
        self._window.sessions_peak.update(self._active_sessions)

    def _close_window(self, coverage_end: float) -> None:
        coverage = max(0.0, coverage_end - self._window.coverage_start)
        self._day.coverage_s += coverage
        self._log_dropped_keys(self._window.distinct, f"window {_iso_utc(self._window.start)}")
        self._emit("window", self._window_row(self._window, coverage))

    @staticmethod
    def _log_dropped_keys(distinct: _DistinctKeys, label: str) -> None:
        if distinct.dropped:
            logger.warning(
                "Fleet usage: %d robot sighting(s) over the distinct-key caps "
                "(%d per owner, %d per kind) ignored in %s",
                distinct.dropped,
                USAGE_MAX_KEYS_PER_USER,
                USAGE_MAX_KEYS_PER_SET,
                label,
            )

    def _emit(self, row_type: str, row: dict) -> None:
        if len(self.pending) == self.pending.maxlen:
            self.dropped_rows += 1
            logger.warning(
                "Fleet usage pending buffer full (%d rows): dropping the oldest row",
                self.pending.maxlen,
            )
        self.pending.append((row_type, row))

    # -- current state -------------------------------------------------

    def _seed_distinct(self, target: _DistinctKeys) -> None:
        for key, kind, username in self._producers.values():
            target.add(kind, key, username)

    def _current_robot_counts(self) -> dict[str, int]:
        """Currently connected robots per kind, counted by distinct robot key.

        Counting keys rather than peer ids keeps a legacy daemon that
        briefly holds two registrations (half-open old socket + fresh
        one) at one robot.
        """
        counts = _kind_zeros()
        for _key, kind in {(key, kind) for key, kind, _user in self._producers.values()}:
            counts[kind] += 1
        return counts

    # -- hooks (called by SignalingServer) -----------------------------

    def producer_seen(self, peer_id: str, username: str, meta: dict) -> None:
        """A producer registered or re-sent its ``setPeerStatus`` heartbeat."""
        self.maybe_roll()
        key = usage_robot_key(username, meta)
        kind = robot_kind_of(meta)
        self._window.distinct.add(kind, key, username)
        self._day.distinct.add(kind, key, username)
        entry = (key, kind, username)
        if self._producers.get(peer_id) != entry:
            # Membership (or the robot's identity / kind) changed: the
            # only moment concurrency can rise, so the only moment peaks
            # move.
            self._producers[peer_id] = entry
            for k, count in self._current_robot_counts().items():
                if count > self._window.robots_peak[k]:
                    self._window.robots_peak[k] = count

    def producer_gone(self, peer_id: str) -> None:
        """A producer left ``producers`` (withdraw, disconnect, sweep, takeover).

        A departure can only lower concurrency, so peaks are untouched.
        """
        self.maybe_roll()
        self._producers.pop(peer_id, None)

    def session_started(self, session_id: str, producer_meta: dict) -> None:
        """A session was created; its kind is the producer's kind right now."""
        self.maybe_roll()
        if session_id in self._sessions:
            return
        kind = robot_kind_of(producer_meta)
        self._sessions[session_id] = (self._mono(), kind)
        self._active_sessions[kind] += 1
        self._window.sessions_started[kind] += 1
        self._day.sessions_started[kind] += 1
        if self._active_sessions[kind] > self._window.sessions_peak[kind]:
            self._window.sessions_peak[kind] = self._active_sessions[kind]

    def session_ended(self, session_id: str, cause: str) -> None:
        """A session was removed; ``cause`` is one of ``SESSION_END_REASONS``.

        Unknown sessions (started before this tracker existed) are
        ignored. Durations are rounded to 0.1 s before aggregation.
        """
        self.maybe_roll()
        entry = self._sessions.pop(session_id, None)
        if entry is None:
            return
        started_mono, kind = entry
        self._active_sessions[kind] = max(0, self._active_sessions[kind] - 1)
        duration = round(max(0.0, self._mono() - started_mono), 1)
        self._window.durations[kind].add(duration)
        self._day.durations[kind].add(duration)
        category = cause if cause in SESSION_END_REASONS else SESSION_END_OTHER
        self._window.end_reasons[category] += 1

    # -- output --------------------------------------------------------

    def _window_row(self, window: _WindowAgg, coverage: float) -> dict:
        return {
            "schema_version": USAGE_SCHEMA_VERSION,
            "window_start": _iso_utc(window.start),
            "window_seconds": self.window_seconds,
            "coverage_s": int(round(coverage)),
            "robots_distinct": window.distinct.counts(),
            "robots_peak": dict(window.robots_peak),
            "sessions_started": dict(window.sessions_started),
            "sessions_peak": dict(window.sessions_peak),
            "session_durations": {k: v.as_dict() for k, v in window.durations.items()},
            "session_end_reasons": dict(window.end_reasons),
        }

    def _day_row(self, day: _DayAgg, coverage: float) -> dict:
        coverage_s = int(round(coverage))
        return {
            "schema_version": USAGE_SCHEMA_VERSION,
            "day": day.day,
            "coverage_s": coverage_s,
            "partial": coverage_s < 86400,
            "robots_distinct": day.distinct.counts(),
            "sessions_started": dict(day.sessions_started),
            "session_durations": {k: v.as_dict() for k, v in day.durations.items()},
        }

    def pending_snapshot(self) -> list[tuple[str, dict]]:
        """Copy of the frozen rows awaiting publication, oldest first."""
        return list(self.pending)

    def ack_pending(self, rows: list[tuple[str, dict]]) -> None:
        """Drop ``rows`` (a prefix returned by ``pending_snapshot``) once published.

        Matches by identity, so rows appended meanwhile - or a prefix
        already evicted by the bounded deque - are handled correctly.
        """
        published = {id(row) for row in rows}
        while self.pending and id(self.pending[0]) in published:
            self.pending.popleft()

    def snapshot_current(self) -> list[tuple[str, dict]]:
        """Rows for the still-open window and day, covering up to now.

        Used for the final publish on shutdown; both rows come out
        partial (``coverage_s`` below the full span).
        """
        self.maybe_roll()
        window_coverage = max(0.0, self._wall() - self._window.coverage_start)
        return [
            ("window", self._window_row(self._window, window_coverage)),
            ("daily", self._day_row(self._day, self._day.coverage_s + window_coverage)),
        ]


# --- Row merge / file formats -------------------------------------------


def _usage_row_key(row_type: str, row: dict) -> Optional[str]:
    """``window_start`` / ``day`` of a row, or ``None`` if the row is malformed."""
    key = row.get("window_start") if row_type == "window" else row.get("day")
    if not isinstance(key, str):
        return None
    pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z" if row_type == "window" else r"\d{4}-\d{2}-\d{2}"
    return key if re.fullmatch(pattern, key) else None


def _usage_row_is_partial(row_type: str, row: dict) -> bool:
    if row_type == "window":
        return row.get("coverage_s", 0) < row.get("window_seconds", 0)
    return bool(row.get("partial"))


def _as_dict(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _num(value: object) -> float:
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else 0


def _merge_kind_dicts(a: object, b: object, op: Callable[[int, int], int]) -> dict:
    a, b = _as_dict(a), _as_dict(b)
    return {k: op(_num(a.get(k)), _num(b.get(k))) for k in list(a) + [k for k in b if k not in a]}


def _merge_durations(a: object, b: object) -> dict:
    """Combine two ``{count, sum_s, max_s, hist}`` aggregates."""
    a, b = _as_dict(a), _as_dict(b)
    ha = [_num(v) for v in a.get("hist") or []]
    hb = [_num(v) for v in b.get("hist") or []]
    size = max(len(ha), len(hb))
    ha += [0] * (size - len(ha))
    hb += [0] * (size - len(hb))
    return {
        "count": _num(a.get("count")) + _num(b.get("count")),
        "sum_s": round(_num(a.get("sum_s")) + _num(b.get("sum_s")), 1),
        "max_s": max(_num(a.get("max_s")), _num(b.get("max_s"))),
        "hist": [x + y for x, y in zip(ha, hb)],
    }


def _merge_duration_dicts(a: object, b: object) -> dict:
    a, b = _as_dict(a), _as_dict(b)
    return {kind: _merge_durations(a.get(kind), b.get(kind)) for kind in list(a) + [k for k in b if k not in a]}


def merge_usage_rows(row_type: str, older: dict, newer: dict) -> dict:
    """Combine two partial rows for the same window / day from two processes.

    Happens across a restart: the old process publishes its partial
    window and day on shutdown, the new one starts mid-window. Event
    counts (sessions started, durations, end reasons) are disjoint and
    are summed; distinct and peak robot / session counts cannot be
    combined exactly, so the larger value is kept (a lower bound).
    ``coverage_s`` is summed, capped at the full span. A merged daily row
    stays ``partial``.
    """
    merged = dict(newer)
    span = newer.get("window_seconds", 0) if row_type == "window" else 86400
    merged["coverage_s"] = min(span, _num(older.get("coverage_s")) + _num(newer.get("coverage_s")))
    merged["robots_distinct"] = _merge_kind_dicts(older.get("robots_distinct"), newer.get("robots_distinct"), max)
    merged["sessions_started"] = _merge_kind_dicts(older.get("sessions_started"), newer.get("sessions_started"), operator.add)
    merged["session_durations"] = _merge_duration_dicts(older.get("session_durations"), newer.get("session_durations"))
    if row_type == "window":
        merged["robots_peak"] = _merge_kind_dicts(older.get("robots_peak"), newer.get("robots_peak"), max)
        merged["sessions_peak"] = _merge_kind_dicts(older.get("sessions_peak"), newer.get("sessions_peak"), max)
        merged["session_end_reasons"] = _merge_kind_dicts(
            older.get("session_end_reasons"), newer.get("session_end_reasons"), operator.add
        )
    else:
        merged["partial"] = True
    return merged


def _parse_jsonl(text: Optional[str], row_type: str, source: str) -> list[dict]:
    """Well-formed rows of a JSONL file; malformed lines are skipped with a warning."""
    rows: list[dict] = []
    if not text:
        return rows
    skipped = 0
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError:
            skipped += 1
            continue
        if isinstance(row, dict) and _usage_row_key(row_type, row) is not None:
            rows.append(row)
        else:
            skipped += 1
    if skipped:
        logger.warning("Fleet usage: skipped %d malformed row(s) in %s", skipped, source)
    return rows


def _to_jsonl(rows: list[dict]) -> str:
    return "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows)


def _window_day_path(day: str) -> str:
    return f"{USAGE_WINDOWS_DIR}/{day}.jsonl"


# --- summary.json -------------------------------------------------------


def _aggregate_slot(rows: list[dict]) -> Optional[dict]:
    """Collapse the window rows of one chart slot (one window or one hour).

    Distinct robots and both peaks take the max over the slot's windows
    (distinct robots over an hour cannot be derived from windows, so the
    hourly value is "the busiest window's distinct count"); sessions
    started, durations and coverage are summed. Durations are merged
    across robot kinds.
    """
    if not rows:
        return None
    out = {
        "coverage_s": 0,
        "robots_distinct": _kind_zeros(),
        "robots_peak": _kind_zeros(),
        "sessions_started": _kind_zeros(),
        "sessions_peak": _kind_zeros(),
        "durations": {"count": 0, "sum_s": 0.0, "max_s": 0.0, "hist": [0] * (len(SESSION_DURATION_BUCKETS_S) + 1)},
    }
    for row in rows:
        out["coverage_s"] += _num(row.get("coverage_s"))
        for field_name, op in (
            ("robots_distinct", max),
            ("robots_peak", max),
            ("sessions_started", operator.add),
            ("sessions_peak", max),
        ):
            values = _as_dict(row.get(field_name))
            for kind in PUBLIC_ROBOT_KINDS:
                out[field_name][kind] = op(out[field_name][kind], _num(values.get(kind)))
        for kind_durations in _as_dict(row.get("session_durations")).values():
            merged = _merge_durations(out["durations"], kind_durations)
            merged["hist"] = merged["hist"][: len(SESSION_DURATION_BUCKETS_S) + 1]
            out["durations"] = merged
    return out


def _columns(slots: list[Optional[dict]], start: float, step: int) -> dict:
    """Columnar series for the page: one array per metric, ``null`` = not observed."""

    def col(get: Callable[[dict], object]) -> list:
        return [get(slot) if slot is not None else None for slot in slots]

    return {
        "start": _iso_utc(start),
        "step_seconds": step,
        "count": len(slots),
        "coverage_s": col(lambda s: s["coverage_s"]),
        **{
            field_name: {kind: col(lambda s, f=field_name, k=kind: s[f][k]) for kind in PUBLIC_ROBOT_KINDS}
            for field_name in ("robots_distinct", "robots_peak", "sessions_started", "sessions_peak")
        },
        "durations": {
            "count": col(lambda s: s["durations"]["count"]),
            "sum_s": col(lambda s: round(s["durations"]["sum_s"], 1)),
            "max_s": col(lambda s: s["durations"]["max_s"]),
            "hist": col(lambda s: s["durations"]["hist"]),
        },
    }


def build_summary(windows: dict[str, dict], daily: dict[str, dict], now: float, window_seconds: int) -> dict:
    """The chart-only ``summary.json``: the one file the status page fetches.

    - ``recent``: last 24 h at window resolution (slots up to the open
      window, exclusive).
    - ``hourly``: last 7 days by UTC hour, up to and including the
      current (partial) hour; see ``_aggregate_slot`` for the semantics.
    - ``daily``: the last 365 daily rows, reduced to what the daily chart
      shows.

    Full rows stay in ``data/windows/*.jsonl`` and ``data/daily.jsonl``.
    """
    by_epoch: dict[int, dict] = {}
    for key, row in windows.items():
        try:
            by_epoch[int(_parse_iso_utc(key))] = row
        except ValueError:
            continue

    recent_end = int(now // window_seconds) * window_seconds
    recent_start = recent_end - USAGE_SUMMARY_RECENT_SECONDS
    recent_slots = [
        _aggregate_slot([by_epoch[t]] if t in by_epoch else [])
        for t in range(recent_start, recent_end, window_seconds)
    ]

    hourly_end = int(now // 3600) * 3600 + 3600
    hourly_start = hourly_end - USAGE_SUMMARY_HOURLY_DAYS * 86400
    by_hour: dict[int, list[dict]] = {}
    for epoch, row in by_epoch.items():
        if hourly_start <= epoch < hourly_end:
            by_hour.setdefault(epoch // 3600 * 3600, []).append(row)
    hourly_slots = [
        _aggregate_slot(sorted(by_hour.get(h, []), key=lambda r: r["window_start"]))
        for h in range(hourly_start, hourly_end, 3600)
    ]

    days = [daily[k] for k in sorted(daily)][-USAGE_SUMMARY_DAILY_ROWS:]
    return {
        "schema_version": USAGE_SCHEMA_VERSION,
        "generated_at": _iso_utc(now),
        "window_seconds": window_seconds,
        "kinds": list(PUBLIC_ROBOT_KINDS),
        "duration_buckets_s": list(SESSION_DURATION_BUCKETS_S),
        "recent": _columns(recent_slots, recent_start, window_seconds),
        "hourly": _columns(hourly_slots, hourly_start, 3600),
        "daily": {
            "days": [d["day"] for d in days],
            "coverage_s": [_num(d.get("coverage_s")) for d in days],
            "partial": [bool(d.get("partial")) for d in days],
            "robots_distinct": {
                kind: [_num(_as_dict(d.get("robots_distinct")).get(kind)) for d in days]
                for kind in PUBLIC_ROBOT_KINDS
            },
        },
    }


# --- Sinks --------------------------------------------------------------


class LocalDirUsageSink:
    """Dev sink: the dataset file layout written to a local directory."""

    def __init__(self, root: str):
        self.root = os.path.abspath(root)

    def describe(self) -> str:
        return f"local dir {self.root}"

    def redact(self, text: str) -> str:
        return text

    def _path(self, path: str) -> str:
        full = os.path.abspath(os.path.join(self.root, path))
        if os.path.commonpath([self.root, full]) != self.root:
            raise ValueError(f"path escapes the sink root: {path!r}")
        return full

    def read(self, path: str) -> Optional[str]:
        """File content, or ``None`` if it does not exist."""
        try:
            with open(self._path(path), encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def write(self, files: dict[str, str], message: str) -> None:
        """Write every file (atomic per file via rename)."""
        for path, content in files.items():
            full = self._path(path)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            tmp = full + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(content)
            os.replace(tmp, full)


_hub_client_bounded = False


def _install_bounded_hub_client() -> None:
    """Give huggingface_hub's shared HTTP client a finite per-request timeout.

    In huggingface_hub 2.0.0 (pinned) the shared ``httpx2.Client`` is
    built with ``timeout=None`` and ``create_commit`` passes no per-call
    timeout, so a half-dead connection would block forever. The hub's
    own factory is reused (it installs the request-id / offline-mode
    event hook) with the timeout set to ``USAGE_HF_HTTP_TIMEOUT_SECONDS``.
    Process-wide, installed once; this server makes no other hub calls.
    """
    global _hub_client_bounded
    if _hub_client_bounded:
        return
    import httpx2
    from huggingface_hub import set_client_factory
    from huggingface_hub.utils import _http

    def bounded_client_factory():
        client = _http.default_client_factory()
        client.timeout = httpx2.Timeout(USAGE_HF_HTTP_TIMEOUT_SECONDS)
        return client

    set_client_factory(bounded_client_factory)
    _hub_client_bounded = True


class HfDatasetUsageSink:
    """Sink backed by a HF dataset repo: ``hf_hub_download`` + one ``create_commit``.

    ``api`` is injectable (tests pass a fake exposing ``hf_hub_download``
    and ``create_commit``); by default an ``HfApi`` is built lazily, with
    a bounded HTTP timeout, so the import only happens when publishing is
    enabled. The token is passed explicitly on every call, kept out of
    ``repr`` and scrubbed from error text via ``redact``.
    """

    def __init__(self, repo_id: str, token: str, api: object = None):
        self.repo_id = repo_id
        self._token = token
        self._api = api

    def __repr__(self) -> str:
        return f"HfDatasetUsageSink(repo_id={self.repo_id!r})"

    def describe(self) -> str:
        return f"dataset {self.repo_id}"

    def redact(self, text: str) -> str:
        return text.replace(self._token, "***") if self._token else text

    def _client(self):
        if self._api is None:
            from huggingface_hub import HfApi
            from huggingface_hub.utils import disable_progress_bars

            _install_bounded_hub_client()
            disable_progress_bars()
            self._api = HfApi(token=self._token)
        return self._api

    def read(self, path: str) -> Optional[str]:
        """File content at ``main``, or ``None`` only if the Hub says 404.

        ``RemoteEntryNotFoundError`` is the Hub's answer for a missing
        file. Everything else raises - notably ``LocalEntryNotFoundError``,
        which huggingface_hub 2.0.0 raises for connection errors, timeouts
        and 5xx when nothing is cached (always the case here: every read
        uses a fresh cache dir). Treating that as "missing" would let the
        next commit overwrite the remote files with near-empty content.
        """
        from huggingface_hub.errors import RemoteEntryNotFoundError

        with tempfile.TemporaryDirectory() as cache_dir:
            try:
                local = self._client().hf_hub_download(
                    repo_id=self.repo_id,
                    filename=path,
                    repo_type="dataset",
                    cache_dir=cache_dir,
                    token=self._token,
                )
            except RemoteEntryNotFoundError:
                return None
            with open(local, encoding="utf-8") as f:
                return f.read()

    def write(self, files: dict[str, str], message: str) -> None:
        """Commit every file in one ``create_commit``."""
        from huggingface_hub import CommitOperationAdd

        operations = [
            CommitOperationAdd(path_in_repo=path, path_or_fileobj=content.encode("utf-8"))
            for path, content in files.items()
        ]
        self._client().create_commit(
            repo_id=self.repo_id,
            repo_type="dataset",
            operations=operations,
            commit_message=message,
            token=self._token,
        )


# --- Publisher ----------------------------------------------------------


def _run_in_daemon_thread(fn: Callable, *args) -> asyncio.Future:
    """Run ``fn(*args)`` in a new daemon thread; return a future for its result.

    Not the default executor: a call wedged on the network must neither
    occupy a shared worker nor block interpreter exit (the default
    executor is joined at shutdown).
    """
    loop = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()

    def resolve(value: object, error: Optional[BaseException]) -> None:
        if future.done():
            return
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(value)

    def target() -> None:
        try:
            value, error = fn(*args), None
        except BaseException as exc:  # delivered to the awaiting side
            value, error = None, exc
        try:
            loop.call_soon_threadsafe(resolve, value, error)
        except RuntimeError:
            pass  # the loop is gone (process shutting down): nobody is waiting

    threading.Thread(target=target, name="fleet-usage-publish", daemon=True).start()
    return future


class FleetUsagePublisher:
    """Publish the tracker's frozen rows to a sink, in one commit per cycle.

    The process has no persistent disk, so the first publish bootstraps
    from the sink: ``data/daily.jsonl`` and the window files of the last
    ``USAGE_WINDOW_RETENTION_DAYS`` days (needed to rebuild the hourly
    series of ``summary.json``); any other day file is downloaded the
    first time a row for that day is written. A file the sink reports
    missing means "start empty"; any other read error fails the whole
    attempt before anything is adopted or written (retried next cycle),
    so a transient Hub error can never make us overwrite remote data.

    Dedupe on ``window_start`` / ``day``: a row this process already
    published is replaced by the newer one (idempotent retries); a row
    from an earlier process is merged with ours when both are partial
    (restart mid-window / mid-day, see ``merge_usage_rows``), otherwise
    ours wins.

    Blocking sink I/O runs in a dedicated daemon thread under a timeout;
    only one attempt is in flight at a time. On failure the rows stay in
    the tracker's pending buffer and are retried next cycle. The in-memory
    copy of the published state is only replaced after a successful
    write. Nothing here raises into the event loop.
    """

    def __init__(
        self,
        tracker: UsageTracker,
        sink,
        *,
        publish_seconds: float = DEFAULT_USAGE_PUBLISH_SECONDS,
        wall_clock: Callable[[], float] = time.time,
    ):
        self.tracker = tracker
        self.sink = sink
        self.publish_seconds = publish_seconds
        self._wall = wall_clock
        self._bootstrapped = False
        # window_start -> row, for the retention horizon plus loaded days.
        self._windows: dict[str, dict] = {}
        self._loaded_days: set[str] = set()
        self._daily: dict[str, dict] = {}
        # (row_type, key) this process has successfully published.
        self._own: set[tuple[str, str]] = set()
        self._inflight: Optional[asyncio.Future] = None
        self.last_published_at: Optional[float] = None

    # -- blocking part (worker thread) ---------------------------------

    def _read_remote(self, rows: list[tuple[str, dict]], now: float) -> None:
        """Download everything this attempt needs, then adopt it all at once."""
        days = {row["window_start"][:10] for t, row in rows if t == "window" and _usage_row_key(t, row)}
        daily_text: Optional[str] = None
        if not self._bootstrapped:
            days |= {_utc_day(now - i * 86400) for i in range(USAGE_WINDOW_RETENTION_DAYS + 1)}
            daily_text = self.sink.read(USAGE_DAILY_PATH)
        day_texts = {
            day: self.sink.read(_window_day_path(day))
            for day in sorted(days - self._loaded_days)
        }
        # Every read succeeded: only now touch the state.
        if not self._bootstrapped:
            for row in _parse_jsonl(daily_text, "daily", USAGE_DAILY_PATH):
                self._daily.setdefault(row["day"], row)
        for day, text in day_texts.items():
            path = _window_day_path(day)
            for row in _parse_jsonl(text, "window", path):
                self._windows.setdefault(row["window_start"], row)
            self._loaded_days.add(day)
        if not self._bootstrapped:
            self._bootstrapped = True
            logger.info(
                "Fleet usage bootstrapped from %s: %d window row(s), %d daily row(s)",
                self.sink.describe(),
                len(self._windows),
                len(self._daily),
            )

    def _combine(self, row_type: str, key: str, existing: Optional[dict], new: dict, own: set) -> dict:
        if existing is None or (row_type, key) in own:
            return new
        if _usage_row_is_partial(row_type, existing) and _usage_row_is_partial(row_type, new):
            try:
                return merge_usage_rows(row_type, existing, new)
            except Exception:
                logger.exception("Fleet usage: could not merge %s row %s, keeping ours", row_type, key)
        return new

    def _publish_blocking(self, rows: list[tuple[str, dict]]) -> list[str]:
        """Merge ``rows`` into the published state and write it. Returns written paths."""
        now = self._wall()
        self._read_remote(rows, now)

        windows = dict(self._windows)
        daily = dict(self._daily)
        own = set(self._own)
        touched_days: set[str] = set()
        n_windows = n_daily = 0
        for row_type, row in rows:
            key = _usage_row_key(row_type, row)
            if key is None:
                continue
            if row_type == "window":
                windows[key] = self._combine(row_type, key, windows.get(key), row, own)
                touched_days.add(key[:10])
                n_windows += 1
            else:
                daily[key] = self._combine(row_type, key, daily.get(key), row, own)
                n_daily += 1
            own.add((row_type, key))

        files: dict[str, str] = {}
        for day in sorted(touched_days):
            day_rows = [windows[k] for k in sorted(windows) if k.startswith(day)]
            files[_window_day_path(day)] = _to_jsonl(day_rows)
        if n_daily:
            files[USAGE_DAILY_PATH] = _to_jsonl([daily[k] for k in sorted(daily)])
        files[USAGE_SUMMARY_PATH] = json.dumps(
            build_summary(windows, daily, now, self.tracker.window_seconds),
            separators=(",", ":"),
        )

        self.sink.write(
            files,
            f"Fleet usage: {n_windows} window row(s), {n_daily} daily row(s)",
        )

        # Committed: adopt the new state within the retention horizon.
        keep_from = _iso_utc(now - USAGE_WINDOW_RETENTION_DAYS * 86400)
        self._windows = {k: v for k, v in windows.items() if k >= keep_from}
        self._loaded_days = {d for d in self._loaded_days if d >= keep_from[:10]}
        self._daily = daily
        self._own = {(t, k) for t, k in own if t == "daily" or k >= keep_from}
        self.last_published_at = now
        return list(files)

    # -- async part (event loop) ---------------------------------------

    def _on_done(self, rows: list[tuple[str, dict]], future: asyncio.Future) -> None:
        if future.cancelled():
            return
        exc = future.exception()
        if exc is not None:
            logger.warning(
                "Fleet usage publish to %s failed (%s: %s); %d row(s) kept for retry",
                self.sink.describe(),
                type(exc).__name__,
                self.sink.redact(str(exc)),
                len(self.tracker.pending),
            )
            return
        self.tracker.ack_pending(rows)
        logger.info(
            "Fleet usage published to %s: %s",
            self.sink.describe(),
            ", ".join(future.result()),
        )

    def busy(self) -> bool:
        """A previous attempt is still running (possibly wedged on the network)."""
        return self._inflight is not None and not self._inflight.done()

    async def publish_once(self, *, timeout: float, include_current: bool = False) -> bool:
        """One publish attempt. Returns ``True`` on success or nothing to do."""
        if self.busy():
            logger.warning("Fleet usage: previous publish still running, skipping this cycle")
            return False
        try:
            self.tracker.maybe_roll()
            rows = self.tracker.pending_snapshot()
            extra = self.tracker.snapshot_current() if include_current else []
        except Exception:
            logger.exception("Fleet usage: could not snapshot tracker")
            return False
        if not rows and not extra:
            return True
        future = _run_in_daemon_thread(self._publish_blocking, rows + extra)
        future.add_done_callback(functools.partial(self._on_done, rows))
        self._inflight = future
        try:
            await asyncio.wait_for(asyncio.shield(future), timeout)
        except asyncio.TimeoutError:
            logger.warning(
                "Fleet usage publish to %s still running after %.0fs; rows kept until it completes",
                self.sink.describe(),
                timeout,
            )
            return False
        except Exception:
            return False  # logged by _on_done
        return True

    async def run(self) -> None:
        """Background task: publish every ``publish_seconds``."""
        while True:
            await asyncio.sleep(self.publish_seconds)
            try:
                await self.publish_once(timeout=USAGE_PUBLISH_TIMEOUT_SECONDS)
            except Exception:
                logger.exception("Fleet usage publish cycle failed")

    async def final_publish(self, timeout: float = USAGE_FINAL_PUBLISH_TIMEOUT_SECONDS) -> bool:
        """Shutdown: publish pending rows plus the partial current window and day.

        Bounded by ``timeout`` overall: an attempt still wedged from a
        previous cycle is waited for within that budget, then abandoned
        (its daemon thread does not hold up process exit).
        """
        deadline = time.monotonic() + timeout
        try:
            if self.busy():
                try:
                    await asyncio.wait_for(asyncio.shield(self._inflight), timeout)
                except Exception:
                    pass
                if self.busy():
                    logger.warning("Fleet usage: previous publish still running, skipping final publish")
                    return False
            remaining = max(0.1, deadline - time.monotonic())
            return await self.publish_once(timeout=remaining, include_current=True)
        except Exception:
            logger.exception("Fleet usage final publish failed")
            return False


def publisher_health(tracker: UsageTracker, publisher: Optional[FleetUsagePublisher]) -> dict:
    """``/health`` ``usage_publisher`` block: aggregate counters only."""
    last = publisher.last_published_at if publisher is not None else None
    return {
        "enabled": publisher is not None,
        "last_published_at": _iso_utc(last) if last is not None else None,
        "pending_rows": len(tracker.pending),
        "dropped_rows": tracker.dropped_rows,
    }


# --- Configuration ------------------------------------------------------


@dataclass(frozen=True)
class FleetUsageConfig:
    """Operator configuration of the fleet usage feature (all optional)."""

    dataset: Optional[str] = None
    token: Optional[str] = field(default=None, repr=False)
    local_dir: Optional[str] = None
    publish_seconds: float = DEFAULT_USAGE_PUBLISH_SECONDS
    window_seconds: int = DEFAULT_USAGE_WINDOW_SECONDS

    @property
    def summary_url(self) -> Optional[str]:
        """Where the status page fetches ``summary.json``, or ``None`` (section hidden)."""
        if self.local_dir:
            return DEV_USAGE_SUMMARY_ROUTE
        if self.dataset:
            return f"https://huggingface.co/datasets/{self.dataset}/resolve/main/{USAGE_SUMMARY_PATH}"
        return None

    @property
    def dataset_url(self) -> Optional[str]:
        if self.local_dir:
            return DEV_USAGE_SUMMARY_ROUTE
        if self.dataset:
            return f"https://huggingface.co/datasets/{self.dataset}"
        return None


def fleet_usage_config_from_env(env) -> FleetUsageConfig:
    """Parse ``FLEET_USAGE_*`` settings; invalid values are logged and ignored.

    ``FLEET_USAGE_LOCAL_DIR`` is a dev-only sink and is refused whenever
    ``SPACE_ID`` is set (same guard as ``DEV_TOKEN_SEED``); when active it
    takes precedence over the dataset for both publishing and the page.
    ``FLEET_USAGE_WINDOW_SECONDS`` is only honoured together with the
    local dir: the dataset always uses 600 s windows.
    """
    dataset = env.get("FLEET_USAGE_DATASET", "").strip() or None
    if dataset is not None and not FLEET_USAGE_DATASET_RE.fullmatch(dataset):
        logger.warning("FLEET_USAGE_DATASET is not a valid repo id; fleet usage dataset disabled")
        dataset = None
    token = env.get("FLEET_USAGE_HF_TOKEN", "").strip() or None

    local_dir = env.get("FLEET_USAGE_LOCAL_DIR", "").strip() or None
    if local_dir is not None and env.get("SPACE_ID"):
        logger.warning(
            "FLEET_USAGE_LOCAL_DIR is set but ignored: the local usage sink is "
            "disabled on a deployed Space."
        )
        local_dir = None

    window_seconds = DEFAULT_USAGE_WINDOW_SECONDS
    raw_window = env.get("FLEET_USAGE_WINDOW_SECONDS", "").strip()
    if raw_window and not local_dir:
        logger.warning(
            "FLEET_USAGE_WINDOW_SECONDS is only honoured with FLEET_USAGE_LOCAL_DIR; using %d",
            DEFAULT_USAGE_WINDOW_SECONDS,
        )
    elif raw_window:
        try:
            value = int(raw_window)
            if value <= 0 or 86400 % value:
                raise ValueError
            window_seconds = value
        except ValueError:
            logger.warning(
                "FLEET_USAGE_WINDOW_SECONDS=%r must be a positive integer dividing 86400; using %d",
                raw_window,
                DEFAULT_USAGE_WINDOW_SECONDS,
            )

    publish_seconds = DEFAULT_USAGE_PUBLISH_SECONDS
    raw_publish = env.get("FLEET_USAGE_PUBLISH_SECONDS", "").strip()
    if raw_publish:
        try:
            value = float(raw_publish)
            if not value >= MIN_USAGE_PUBLISH_SECONDS:
                raise ValueError
            publish_seconds = value
        except ValueError:
            logger.warning(
                "FLEET_USAGE_PUBLISH_SECONDS=%r must be a number >= %.0f; using %.0f",
                raw_publish,
                MIN_USAGE_PUBLISH_SECONDS,
                DEFAULT_USAGE_PUBLISH_SECONDS,
            )

    if dataset and not token and not local_dir:
        logger.warning(
            "FLEET_USAGE_DATASET is set without FLEET_USAGE_HF_TOKEN: the status "
            "page shows the dataset but nothing is published."
        )
    return FleetUsageConfig(
        dataset=dataset,
        token=token,
        local_dir=local_dir,
        publish_seconds=publish_seconds,
        window_seconds=window_seconds,
    )


def build_usage_publisher(
    config: FleetUsageConfig, tracker: UsageTracker, *, hf_api: object = None
) -> Optional[FleetUsagePublisher]:
    """The publisher for ``config``, or ``None`` when publishing is disabled."""
    if config.local_dir:
        sink = LocalDirUsageSink(config.local_dir)
    elif config.dataset and config.token:
        sink = HfDatasetUsageSink(config.dataset, config.token, api=hf_api)
    else:
        return None
    logger.info("Fleet usage publishing to %s every %.0fs", sink.describe(), config.publish_seconds)
    return FleetUsagePublisher(tracker, sink, publish_seconds=config.publish_seconds)


# --- Status page section ------------------------------------------------
#
# "Fleet usage" section of the status page, rendered once at import and
# only when a usage source is configured (``FleetUsageConfig.summary_url``).
# The browser fetches ``summary.json`` straight from huggingface.co (or
# the dev route) once on load and at most every 10 minutes, only while
# the tab is visible, so charting costs this server nothing. ``$usage_config`` is the only placeholder: a
# JSON object built from operator config (the regex-validated dataset
# id) and server-side constants - nothing client- or meta-derived. The
# rendered section is itself substituted into the page Template, whose
# output is not re-scanned; still, write a literal dollar sign as ``$$``.
USAGE_CHART_JS_URL = "https://cdn.jsdelivr.net/npm/chart.js@4.5.1/dist/chart.umd.min.js"
USAGE_CHART_JS_SRI = "sha384-jb8JQMbMoBUzgWatfe6COACi2ljcDdZQ2OxczGA3bGNeWe+6DChMTBJemed7ZnvJ"

_USAGE_SECTION_SOURCE = """        <section class="card usage" id="usage">
            <style>
                .usage-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; }
                .seg { display: inline-flex; border: 1px solid var(--divider); border-radius: 999px; padding: 2px; }
                .seg button {
                    font: inherit; font-size: 12px; font-weight: 600;
                    color: var(--text-secondary); background: none;
                    border: 0; border-radius: 999px; padding: 3px 12px; cursor: pointer;
                }
                .seg button[aria-pressed="true"] { background: var(--accent); color: #111111; }
                .usage-status { margin: 12px 0 0; font-size: 14px; color: var(--text-secondary); }
                .usage-status.error { color: #d93025; }
                .chart-block { margin-top: 20px; }
                .chart-title { font-size: 14px; font-weight: 600; }
                .chart-note { font-size: 12px; color: var(--text-secondary); }
                .chart-box { position: relative; height: 220px; margin-top: 8px; }
                .chart-box.small { height: 160px; }
                .chart-empty { margin: 8px 0 0; font-size: 13px; color: var(--text-secondary); }
                .tiles { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-top: 20px; }
                .tile { border: 1px solid var(--divider); border-radius: var(--radius); padding: 12px 14px; }
                .tile-value { font-size: 22px; font-weight: 700; letter-spacing: -0.3px; margin-top: 2px; }
                .usage-foot { margin: 16px 0 0; font-size: 12px; color: var(--text-secondary); }
                .usage-foot a { color: var(--accent); }
                @media (max-width: 520px) { .tiles { grid-template-columns: repeat(2, 1fr); } }
            </style>
            <div class="usage-head">
                <div class="overline">Fleet usage</div>
                <div class="seg" role="group" aria-label="Time range">
                    <button type="button" data-range="24h" aria-pressed="true">24 h</button>
                    <button type="button" data-range="7d" aria-pressed="false">7 days</button>
                </div>
            </div>
            <p class="usage-status" id="usage-status">Loading usage data...</p>
            <div id="usage-body" hidden>
                <div class="chart-block">
                    <div class="chart-title">Robots online <span class="usage-resolution"></span></div>
                    <div class="chart-note" id="usage-robots-note"></div>
                    <div class="chart-box"><canvas id="usage-robots" aria-label="Robots online"></canvas></div>
                </div>
                <div class="chart-block">
                    <div class="chart-title">Sessions <span class="usage-resolution"></span></div>
                    <div class="chart-note" id="usage-sessions-note"></div>
                    <div class="chart-box"><canvas id="usage-sessions" aria-label="Sessions"></canvas></div>
                </div>
                <div class="tiles">
                    <div class="tile"><div class="overline">Sessions ended</div><div class="tile-value" id="usage-ended">-</div></div>
                    <div class="tile"><div class="overline">Mean duration</div><div class="tile-value" id="usage-mean">-</div></div>
                    <div class="tile"><div class="overline">Median (approx.)</div><div class="tile-value" id="usage-median">-</div></div>
                    <div class="tile"><div class="overline">Under 10 s</div><div class="tile-value" id="usage-short">-</div></div>
                </div>
                <div class="chart-block">
                    <div class="chart-title">Session durations in range</div>
                    <div class="chart-note">Sessions that ended in the selected range, all robot kinds.</div>
                    <div class="chart-box small"><canvas id="usage-durations" aria-label="Session duration histogram"></canvas></div>
                </div>
                <div class="chart-block">
                    <div class="chart-title">Distinct robots per day</div>
                    <div class="chart-note">UTC days, last 365. Partial days (server not up all day) are flagged in the tooltip.</div>
                    <p class="chart-empty" id="usage-daily-empty" hidden>No daily data yet.</p>
                    <div class="chart-box" id="usage-daily-box"><canvas id="usage-daily" aria-label="Distinct robots per day"></canvas></div>
                </div>
            </div>
            <p class="usage-foot">All times UTC. Source: <a id="usage-source" rel="noopener" target="_blank">dataset</a><span id="usage-generated"></span>.</p>
        </section>
        <script src="__CHART_JS_URL__" integrity="__CHART_JS_SRI__" crossorigin="anonymous" defer></script>
        <script>
        document.addEventListener("DOMContentLoaded", function () {
            "use strict";
            var CFG = $usage_config;
            var REFRESH_MS = 10 * 60 * 1000;
            // Fixed colour per kind, in PUBLIC_ROBOT_KINDS order (validated
            // categorical palette, one step per colour scheme).
            var COLORS = {
                light: ["#D97706", "#2563EB", "#0F9F8F"],
                dark: ["#C98010", "#4F86EE", "#19968A"]
            };
            var DURATION_LABELS = ["<= 10 s", "10-60 s", "1-5 min", "5-15 min", "15-60 min", "> 60 min"];
            var NOTES = {
                "24h": {
                    robots: "Solid: distinct robots seen in the window. Dashed: peak concurrent robots.",
                    sessions: "Solid: sessions started in the window. Dashed: peak concurrent sessions."
                },
                "7d": {
                    robots: "Solid: highest per-window distinct-robot count within the hour (not distinct over the hour). Dashed: peak concurrent robots.",
                    sessions: "Solid: sessions started in the hour. Dashed: peak concurrent sessions."
                }
            };
            var state = { range: "24h", data: null, charts: {} };
            var statusEl = document.getElementById("usage-status");
            var bodyEl = document.getElementById("usage-body");
            document.getElementById("usage-source").href = CFG.datasetUrl;

            function showStatus(text, isError) {
                statusEl.textContent = text;
                statusEl.className = "usage-status" + (isError ? " error" : "");
                statusEl.hidden = false;
                bodyEl.hidden = true;
            }
            function isDark() {
                return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
            }
            function kindColor(i) {
                var palette = isDark() ? COLORS.dark : COLORS.light;
                return palette[i] || "#8E8E93";
            }
            function cssVar(name) {
                return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
            }
            function num(v) { return typeof v === "number" && isFinite(v) ? v : 0; }
            function arr(v) { return Array.isArray(v) ? v : []; }
            function observed(block) {
                return !!block && arr(block.coverage_s).some(function (v) { return v !== null; });
            }
            function fmtDuration(s) {
                if (s === null || !isFinite(s)) return "-";
                if (s < 60) return s.toFixed(1) + " s";
                if (s < 3600) return (s / 60).toFixed(1) + " min";
                return (s / 3600).toFixed(1) + " h";
            }
            function fmtLabel(t, withDate) {
                var iso = new Date(t).toISOString();
                return withDate ? iso.slice(5, 10) + " " + iso.slice(11, 16) : iso.slice(11, 16);
            }
            // Median interpolated linearly inside the histogram bucket that
            // holds the middle session; the open last bucket is capped by
            // the largest observed duration.
            function approxMedian(hist, maxS) {
                var total = 0, i;
                for (i = 0; i < hist.length; i++) total += hist[i];
                if (!total) return null;
                var half = total / 2, cum = 0;
                for (i = 0; i < hist.length; i++) {
                    var n = hist[i];
                    if (n > 0 && cum + n >= half) {
                        var lo = i === 0 ? 0 : CFG.buckets[i - 1];
                        var bound = i < CFG.buckets.length ? CFG.buckets[i] : Infinity;
                        var hi = Math.min(bound, Math.max(maxS, lo));
                        return lo + (hi - lo) * (half - cum) / n;
                    }
                    cum += n;
                }
                return null;
            }
            function destroy(id) {
                if (state.charts[id]) { state.charts[id].destroy(); delete state.charts[id]; }
            }
            function makeChart(id, type, labels, datasets, extra) {
                destroy(id);
                var text = cssVar("--text-secondary");
                var grid = cssVar("--divider");
                var options = {
                    animation: false, responsive: true, maintainAspectRatio: false,
                    interaction: { mode: "index", intersect: false },
                    elements: { point: { radius: 0, hoverRadius: 4 }, line: { borderWidth: 2, tension: 0 } },
                    scales: {
                        x: {
                            stacked: !!(extra && extra.stacked),
                            ticks: { color: text, maxTicksLimit: 8, maxRotation: 0, autoSkip: true },
                            grid: { display: false },
                            title: { display: !!(extra && extra.xTitle), text: extra && extra.xTitle, color: text }
                        },
                        y: {
                            stacked: !!(extra && extra.stacked),
                            beginAtZero: true,
                            ticks: { color: text, precision: 0 },
                            grid: { color: grid }
                        }
                    },
                    plugins: {
                        legend: { display: datasets.length > 1, labels: { color: text, boxWidth: 12, boxHeight: type === "line" ? 2 : 12 } },
                        tooltip: { callbacks: (extra && extra.tooltip) || {} }
                    }
                };
                state.charts[id] = new window.Chart(document.getElementById(id), {
                    type: type, data: { labels: labels, datasets: datasets }, options: options
                });
            }
            // A value with no observed neighbour draws no line segment, so
            // it gets a marker instead (otherwise it would be invisible).
            function isolatedRadius(ctx) {
                var d = ctx.dataset.data, i = ctx.dataIndex;
                if (d[i] === null || d[i] === undefined) return 0;
                var prev = i > 0 ? d[i - 1] : null, next = i < d.length - 1 ? d[i + 1] : null;
                return (prev === null || prev === undefined) && (next === null || next === undefined) ? 4 : 0;
            }
            function lineSeries(label, values, color, dashed) {
                return {
                    label: label, data: values, borderColor: color, backgroundColor: color,
                    borderDash: dashed ? [5, 4] : [], spanGaps: false, pointRadius: isolatedRadius
                };
            }
            function anyNonZero(values) { return values.some(function (v) { return v; }); }

            function renderDaily(daily) {
                var days = arr(daily && daily.days);
                var emptyEl = document.getElementById("usage-daily-empty");
                var boxEl = document.getElementById("usage-daily-box");
                if (!days.length) {
                    destroy("usage-daily");
                    emptyEl.hidden = false;
                    boxEl.hidden = true;
                    return;
                }
                emptyEl.hidden = true;
                boxEl.hidden = false;
                var partial = arr(daily.partial), coverage = arr(daily.coverage_s);
                var byKind = daily.robots_distinct || {};
                var sets = [];
                CFG.kinds.forEach(function (k, i) {
                    var values = arr(byKind[k[0]]).map(num);
                    if (i === 0 || anyNonZero(values)) {
                        sets.push({ label: k[1], data: values, backgroundColor: kindColor(i), borderRadius: 2, maxBarThickness: 24 });
                    }
                });
                makeChart("usage-daily", "bar", days.map(String), sets, {
                    stacked: true, xTitle: "UTC day",
                    tooltip: {
                        footer: function (items) {
                            var i = items.length ? items[0].dataIndex : -1;
                            if (i < 0 || !partial[i]) return "";
                            return "Partial day: " + (num(coverage[i]) / 3600).toFixed(1) + " h observed";
                        }
                    }
                });
            }

            function render() {
                var data = state.data || {};
                if (!observed(data.recent) && !observed(data.hourly) && !arr(data.daily && data.daily.days).length) {
                    showStatus("No usage data yet.", false);
                    return;
                }
                if (typeof window.Chart !== "function") { showStatus("Could not load the chart library.", true); return; }

                var block = state.range === "24h" ? data.recent : data.hourly;
                block = block || { count: 0, step_seconds: 3600, start: null };
                var step = num(block.step_seconds) || 600;
                var t0 = Date.parse(block.start);
                var labels = [];
                for (var i = 0; i < num(block.count); i++) labels.push(fmtLabel(t0 + i * step * 1000, state.range !== "24h"));
                var resolution = state.range === "24h"
                    ? (step % 60 === 0 ? "(per " + (step / 60) + "-min window)" : "(per " + step + "-s window)")
                    : "(hourly)";
                document.querySelectorAll(".usage-resolution").forEach(function (el) { el.textContent = resolution; });
                document.getElementById("usage-robots-note").textContent = NOTES[state.range].robots;
                document.getElementById("usage-sessions-note").textContent = NOTES[state.range].sessions;

                function series(field, kind) { return arr((block[field] || {})[kind]); }
                var robotSets = [], sessionSets = [];
                CFG.kinds.forEach(function (k, i) {
                    var kind = k[0], label = k[1], color = kindColor(i);
                    var distinct = series("robots_distinct", kind), rpeak = series("robots_peak", kind);
                    var started = series("sessions_started", kind), speak = series("sessions_peak", kind);
                    if (i === 0 || anyNonZero(distinct) || anyNonZero(rpeak)) {
                        robotSets.push(lineSeries(label + " - distinct", distinct, color, false));
                        robotSets.push(lineSeries(label + " - peak", rpeak, color, true));
                    }
                    if (i === 0 || anyNonZero(started) || anyNonZero(speak)) {
                        sessionSets.push(lineSeries(label + " - started", started, color, false));
                        sessionSets.push(lineSeries(label + " - peak", speak, color, true));
                    }
                });
                makeChart("usage-robots", "line", labels, robotSets, { xTitle: "UTC" });
                makeChart("usage-sessions", "line", labels, sessionSets, { xTitle: "UTC" });

                var d = block.durations || {};
                var count = 0, sum = 0, maxS = 0, hist = CFG.buckets.map(function () { return 0; }).concat([0]);
                arr(d.count).forEach(function (v) { count += num(v); });
                arr(d.sum_s).forEach(function (v) { sum += num(v); });
                arr(d.max_s).forEach(function (v) { maxS = Math.max(maxS, num(v)); });
                arr(d.hist).forEach(function (h) {
                    arr(h).forEach(function (v, j) { if (j < hist.length) hist[j] += num(v); });
                });
                document.getElementById("usage-ended").textContent = String(count);
                document.getElementById("usage-mean").textContent = count ? fmtDuration(sum / count) : "-";
                document.getElementById("usage-median").textContent = count ? fmtDuration(approxMedian(hist, maxS)) : "-";
                document.getElementById("usage-short").textContent = count ? Math.round(100 * hist[0] / count) + " %" : "-";
                makeChart("usage-durations", "bar", DURATION_LABELS, [{
                    label: "Sessions", data: hist, backgroundColor: kindColor(0), borderRadius: 4, borderSkipped: "bottom"
                }], { xTitle: "Duration" });

                renderDaily(data.daily);
                statusEl.hidden = true;
                bodyEl.hidden = false;
            }

            var lastLoadAt = 0;
            function load() {
                lastLoadAt = Date.now();
                fetch(CFG.summaryUrl, { cache: "no-cache" })
                    .then(function (r) {
                        if (r.status === 404) return null;
                        if (!r.ok) throw new Error("HTTP " + r.status);
                        return r.json();
                    })
                    .then(function (data) {
                        state.data = data;
                        document.getElementById("usage-generated").textContent =
                            data && data.generated_at ? ", updated " + String(data.generated_at).replace("T", " ").replace("Z", " UTC") : "";
                        render();
                    })
                    .catch(function (err) {
                        showStatus("Could not load usage data (" + (err && err.message ? err.message : "network error") + ").", true);
                    });
            }

            document.querySelectorAll(".seg button").forEach(function (btn) {
                btn.addEventListener("click", function () {
                    state.range = btn.getAttribute("data-range");
                    document.querySelectorAll(".seg button").forEach(function (b) {
                        b.setAttribute("aria-pressed", b === btn ? "true" : "false");
                    });
                    if (state.data) render();
                });
            });
            if (window.matchMedia) {
                var mq = window.matchMedia("(prefers-color-scheme: dark)");
                if (mq.addEventListener) mq.addEventListener("change", function () { if (state.data) render(); });
            }
            // Same rule as the live counters: fetch only while the tab is
            // visible. On becoming visible, reload at once if the data is
            // older than REFRESH_MS (so tab switching does not refetch),
            // then resume the periodic refresh.
            var refreshTimer = null;
            function resume() {
                if (Date.now() - lastLoadAt >= REFRESH_MS) load();
                if (refreshTimer === null) refreshTimer = setInterval(load, REFRESH_MS);
            }
            document.addEventListener("visibilitychange", function () {
                if (document.visibilityState === "visible") {
                    resume();
                } else if (refreshTimer !== null) {
                    clearInterval(refreshTimer);
                    refreshTimer = null;
                }
            });
            if (document.visibilityState === "visible") resume();
        });
        </script>
""".replace("__CHART_JS_URL__", USAGE_CHART_JS_URL).replace("__CHART_JS_SRI__", USAGE_CHART_JS_SRI)
_USAGE_SECTION = Template(_USAGE_SECTION_SOURCE)


def render_usage_section(config: FleetUsageConfig) -> str:
    """The "Fleet usage" section for ``config``, or ``""`` when not configured.

    The injected JSON carries only operator config (the regex-validated
    dataset id, via ``summary_url`` / ``dataset_url``) and server-side
    constants; ``<`` is escaped so it can never close the script tag.
    """
    if config.summary_url is None:
        return ""
    usage_config = json.dumps(
        {
            "summaryUrl": config.summary_url,
            "datasetUrl": config.dataset_url,
            "kinds": [[kind, ROBOT_KIND_LABELS[kind]] for kind in PUBLIC_ROBOT_KINDS],
            "buckets": list(SESSION_DURATION_BUCKETS_S),
        }
    ).replace("<", "\\u003c")
    return _USAGE_SECTION.substitute(usage_config=usage_config)
