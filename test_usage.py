"""Tests for fleet usage statistics (``UsageTracker`` + publisher).

The tracker is driven with fake wall / monotonic clocks, so window
boundaries are exact. The publisher is exercised against a fake HF
client that records downloads and commits in memory and against the
local-directory sink: nothing here talks to huggingface.co.

Run with::

    python -m pytest test_usage.py -v
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

import httpx2
import pytest
from huggingface_hub.errors import LocalEntryNotFoundError, RemoteEntryNotFoundError

import app as app_module
import fleet_usage
from app import SignalingServer, token_cache
from fleet_usage import (
    PUBLIC_ROBOT_KINDS,
    SESSION_DURATION_BUCKETS_S,
    SESSION_END_ENDED,
    SESSION_END_OTHER,
    SESSION_END_PEER_DISCONNECTED,
    SESSION_END_REASONS,
    SESSION_END_REPLACED,
    SESSION_END_SWEPT,
    SESSION_END_WITHDRAWN,
    USAGE_DAILY_PATH,
    USAGE_SUMMARY_PATH,
    USAGE_MAX_KEYS_PER_USER,
    FleetUsageConfig,
    FleetUsagePublisher,
    HfDatasetUsageSink,
    LocalDirUsageSink,
    UsageTracker,
    build_summary,
    build_usage_publisher,
    fleet_usage_config_from_env,
    merge_usage_rows,
    usage_robot_key,
)

# 2026-09-30T00:00:00Z
DAY0 = datetime(2026, 9, 30, tzinfo=timezone.utc).timestamp()
MINI = {"name": "mini1", "transport": "usb", "hardware_id": "hw-mini-1"}
MINI2 = {"name": "mini2", "transport": "wifi", "hardware_id": "hw-mini-2"}
DUCK = {"name": "duck1", "kind": "microduck", "hardware_id": "hw-duck-1"}
LEGACY = {"name": "reachy_mini"}  # pre-1.7.2 daemon: no hardware_id, no kind


class FakeClock:
    """Wall and monotonic clocks advancing together."""

    def __init__(self, wall: float):
        self.wall = wall
        self.mono = 1000.0

    def advance(self, seconds: float) -> None:
        self.wall += seconds
        self.mono += seconds

    def wall_clock(self) -> float:
        return self.wall

    def mono_clock(self) -> float:
        return self.mono


def _tracker(start: float = DAY0, window_seconds: int = 600):
    clock = FakeClock(start)
    tracker = UsageTracker(
        window_seconds, wall_clock=clock.wall_clock, mono_clock=clock.mono_clock
    )
    return tracker, clock


def _rows(tracker: UsageTracker, row_type: str) -> list[dict]:
    return [row for t, row in tracker.pending if t == row_type]


def _roll_to(tracker: UsageTracker, clock: FakeClock, wall: float) -> None:
    """Advance to ``wall`` like a live process: rolling at every boundary on the way."""
    ws = tracker.window_seconds
    boundary = (int(clock.wall // ws) + 1) * ws
    while boundary <= wall:
        clock.advance(boundary - clock.wall)
        tracker.maybe_roll()
        boundary += ws
    clock.advance(wall - clock.wall)
    tracker.maybe_roll()


def _jump_to(tracker: UsageTracker, clock: FakeClock, wall: float) -> None:
    """Advance to ``wall`` without intermediate rollovers (a paused process)."""
    clock.advance(wall - clock.wall)
    tracker.maybe_roll()


# ----------------------------------------------------------------------
# Construction / identity
# ----------------------------------------------------------------------


@pytest.mark.parametrize("bad", [0, -600, 7, 601, 600.0, True, "600"])
def test_window_seconds_must_divide_a_day(bad):
    with pytest.raises(ValueError):
        UsageTracker(bad)


def _digest(material: str) -> bytes:
    return hashlib.blake2b(material.encode(), digest_size=8).digest()


def test_robot_key_is_fixed_size_digest_of_hardware_id():
    key = usage_robot_key("alice", MINI)
    assert key == _digest("hw\0hw-mini-1") and len(key) == 8
    # Owner-independent: the same robot under another account is the same robot.
    assert usage_robot_key("bob", MINI) == key


@pytest.mark.parametrize("hardware_id", [None, "", 123, "x" * 129])
def test_robot_key_legacy_fallback(hardware_id):
    meta = {"name": "reachy_mini", "hardware_id": hardware_id}
    assert usage_robot_key("alice", meta) == _digest("legacy\0alice\0reachy_mini")


def test_robot_key_accepts_128_char_hardware_id_and_odd_strings():
    assert usage_robot_key("a", {"hardware_id": "x" * 128}) == _digest("hw\0" + "x" * 128)
    assert len(usage_robot_key("a", {"name": "\ud800 lone surrogate"})) == 8


def test_legacy_robot_without_kind_counts_as_reachy_mini():
    tracker, clock = _tracker()
    tracker.producer_seen("p1", "alice", LEGACY)
    _roll_to(tracker, clock, DAY0 + 600)
    (row,) = _rows(tracker, "window")
    assert row["robots_distinct"] == {"reachy_mini": 1, "microduck": 0, "other": 0}


def test_two_legacy_robots_same_name_different_owners_count_twice():
    tracker, clock = _tracker()
    tracker.producer_seen("p1", "alice", LEGACY)
    tracker.producer_seen("p2", "bob", LEGACY)
    _roll_to(tracker, clock, DAY0 + 600)
    (row,) = _rows(tracker, "window")
    assert row["robots_distinct"]["reachy_mini"] == 2
    assert row["robots_peak"]["reachy_mini"] == 2


# ----------------------------------------------------------------------
# Robots: distinct vs peak
# ----------------------------------------------------------------------


def test_robot_connected_one_second_counts_once():
    tracker, clock = _tracker()
    clock.advance(100)
    tracker.producer_seen("p1", "alice", MINI)
    clock.advance(1)
    tracker.producer_gone("p1")
    _roll_to(tracker, clock, DAY0 + 600)
    (row,) = _rows(tracker, "window")
    assert row["robots_distinct"]["reachy_mini"] == 1
    assert row["robots_peak"]["reachy_mini"] == 1


def test_flapping_robot_counts_once():
    tracker, clock = _tracker()
    for i in range(50):
        tracker.producer_seen(f"peer-{i}", "alice", MINI)  # fresh peer id per reconnect
        clock.advance(2)
        tracker.producer_gone(f"peer-{i}")
        clock.advance(3)
    _roll_to(tracker, clock, DAY0 + 600)
    rows = _rows(tracker, "window")
    assert rows[0]["robots_distinct"]["reachy_mini"] == 1
    assert rows[0]["robots_peak"]["reachy_mini"] == 1


def test_heartbeats_do_not_inflate_counts():
    tracker, clock = _tracker()
    for _ in range(59):
        tracker.producer_seen("p1", "alice", MINI)
        clock.advance(10)
    _roll_to(tracker, clock, DAY0 + 600)
    (row,) = _rows(tracker, "window")
    assert row["robots_distinct"]["reachy_mini"] == 1
    assert row["robots_peak"]["reachy_mini"] == 1


def test_peak_is_max_concurrency_per_kind():
    tracker, clock = _tracker()
    tracker.producer_seen("a", "alice", MINI)
    tracker.producer_seen("b", "bob", MINI2)
    tracker.producer_seen("d", "carol", DUCK)
    tracker.producer_gone("a")
    tracker.producer_gone("b")
    tracker.producer_seen("c", "dave", {"name": "x", "hardware_id": "hw-3"})
    _roll_to(tracker, clock, DAY0 + 600)
    (row,) = _rows(tracker, "window")
    assert row["robots_distinct"] == {"reachy_mini": 3, "microduck": 1, "other": 0}
    assert row["robots_peak"] == {"reachy_mini": 2, "microduck": 1, "other": 0}


def test_kind_change_moves_robot_between_kinds():
    tracker, clock = _tracker()
    tracker.producer_seen("p1", "alice", {"hardware_id": "hw", "kind": "microduck"})
    tracker.producer_seen("p1", "alice", {"hardware_id": "hw", "kind": "something-else"})
    _roll_to(tracker, clock, DAY0 + 600)
    _roll_to(tracker, clock, DAY0 + 1200)
    first, second = _rows(tracker, "window")
    assert first["robots_peak"]["microduck"] == 1
    assert first["robots_peak"]["other"] == 1
    # Seeded from current membership: only the latest kind remains.
    assert second["robots_distinct"] == {"reachy_mini": 0, "microduck": 0, "other": 1}


def test_rollover_seeds_next_window_with_connected_state():
    tracker, clock = _tracker()
    tracker.producer_seen("p1", "alice", MINI)
    tracker.producer_seen("p2", "bob", DUCK)
    tracker.session_started("s1", MINI)
    # p1 stays connected across the boundary without sending anything.
    _roll_to(tracker, clock, DAY0 + 600)
    tracker.producer_gone("p2")
    _roll_to(tracker, clock, DAY0 + 1200)
    w1, w2 = _rows(tracker, "window")
    assert w1["window_start"] == "2026-09-30T00:00:00Z"
    assert w2["window_start"] == "2026-09-30T00:10:00Z"
    assert w2["robots_distinct"] == {"reachy_mini": 1, "microduck": 1, "other": 0}
    assert w2["robots_peak"] == {"reachy_mini": 1, "microduck": 1, "other": 0}
    assert w2["sessions_peak"]["reachy_mini"] == 1
    assert w2["sessions_started"]["reachy_mini"] == 0  # started in w1


# ----------------------------------------------------------------------
# Sessions
# ----------------------------------------------------------------------


def test_sessions_started_and_peak():
    tracker, clock = _tracker()
    tracker.session_started("s1", MINI)
    tracker.session_started("s2", MINI2)
    tracker.session_ended("s1", SESSION_END_ENDED)
    tracker.session_started("s3", MINI)
    tracker.session_started("s4", DUCK)
    _roll_to(tracker, clock, DAY0 + 600)
    (row,) = _rows(tracker, "window")
    assert row["sessions_started"] == {"reachy_mini": 3, "microduck": 1, "other": 0}
    assert row["sessions_peak"] == {"reachy_mini": 2, "microduck": 1, "other": 0}


def test_duration_histogram_edges():
    tracker, clock = _tracker()
    for i, duration in enumerate([10, 10.1, 3600, 3600.1, 0.04]):
        tracker.session_started(f"s{i}", MINI)
        _roll_to(tracker, clock, clock.wall + duration)
        tracker.session_ended(f"s{i}", SESSION_END_ENDED)
    _roll_to(tracker, clock, (int(clock.wall // 600) + 1) * 600)
    rows = _rows(tracker, "window")
    total = {"count": 0, "hist": [0] * (len(SESSION_DURATION_BUCKETS_S) + 1)}
    for row in rows:
        d = row["session_durations"]["reachy_mini"]
        total["count"] += d["count"]
        total["hist"] = [a + b for a, b in zip(total["hist"], d["hist"])]
    assert total["count"] == 5
    # <=10: {10, 0.0}; <=60: {10.1}; <=3600: {3600}; >3600: {3600.1}
    assert total["hist"] == [2, 1, 0, 0, 1, 1]


def test_duration_aggregate_shape_and_rounding():
    tracker, clock = _tracker()
    tracker.session_started("s1", DUCK)
    clock.advance(15.04)
    tracker.session_ended("s1", SESSION_END_ENDED)
    tracker.session_started("s2", DUCK)
    clock.advance(4.96)
    tracker.session_ended("s2", SESSION_END_ENDED)
    _roll_to(tracker, clock, DAY0 + 600)
    (row,) = _rows(tracker, "window")
    assert row["session_durations"]["microduck"] == {
        "count": 2, "sum_s": 20.0, "max_s": 15.0, "hist": [1, 1, 0, 0, 0, 0]
    }
    assert row["session_durations"]["reachy_mini"]["count"] == 0


def test_session_ended_in_later_window_counts_there():
    tracker, clock = _tracker()
    clock.advance(590)
    tracker.session_started("s1", MINI)
    clock.advance(20)
    tracker.session_ended("s1", SESSION_END_ENDED)
    _roll_to(tracker, clock, DAY0 + 1200)
    w1, w2 = _rows(tracker, "window")
    assert w1["sessions_started"]["reachy_mini"] == 1
    assert w1["session_durations"]["reachy_mini"]["count"] == 0
    assert w2["session_durations"]["reachy_mini"]["count"] == 1
    assert w2["session_durations"]["reachy_mini"]["sum_s"] == 20.0


def test_unknown_session_end_and_unknown_cause():
    tracker, clock = _tracker()
    tracker.session_ended("never-started", SESSION_END_ENDED)
    tracker.session_started("s1", MINI)
    tracker.session_ended("s1", "attacker-controlled <script>")
    _roll_to(tracker, clock, DAY0 + 600)
    (row,) = _rows(tracker, "window")
    assert set(row["session_end_reasons"]) == set(SESSION_END_REASONS)
    assert row["session_end_reasons"][SESSION_END_OTHER] == 1
    assert sum(row["session_end_reasons"].values()) == 1


# ----------------------------------------------------------------------
# Coverage, gaps, daily rollup
# ----------------------------------------------------------------------


def test_row_schema_is_zero_filled():
    tracker, clock = _tracker()
    _roll_to(tracker, clock, DAY0 + 600)
    (row,) = _rows(tracker, "window")
    assert row["schema_version"] == 1
    assert row["window_seconds"] == 600
    for key in ("robots_distinct", "robots_peak", "sessions_started", "sessions_peak", "session_durations"):
        assert list(row[key]) == list(PUBLIC_ROBOT_KINDS)
    assert list(row["session_end_reasons"]) == list(SESSION_END_REASONS)


def test_first_window_coverage_is_partial():
    tracker, clock = _tracker(start=DAY0 + 120)
    _roll_to(tracker, clock, DAY0 + 600)
    _roll_to(tracker, clock, DAY0 + 1200)
    w1, w2 = _rows(tracker, "window")
    assert w1["coverage_s"] == 480
    assert w2["coverage_s"] == 600


def test_late_rollover_attributes_full_window():
    tracker, clock = _tracker()
    _roll_to(tracker, clock, DAY0 + 604)  # sweeper tick 4 s after the boundary
    _roll_to(tracker, clock, DAY0 + 1200)
    w1, w2 = _rows(tracker, "window")
    assert (w1["coverage_s"], w2["coverage_s"]) == (600, 600)


def test_skipped_windows_emit_nothing_and_new_window_coverage_starts_now():
    tracker, clock = _tracker()
    tracker.producer_seen("p1", "alice", MINI)
    _jump_to(tracker, clock, DAY0 + 1900)  # windows 00:10 and 00:20 never observed
    _roll_to(tracker, clock, DAY0 + 2400)
    starts = [r["window_start"] for r in _rows(tracker, "window")]
    assert starts == ["2026-09-30T00:00:00Z", "2026-09-30T00:30:00Z"]
    assert _rows(tracker, "window")[1]["coverage_s"] == 500


def test_wall_clock_going_backwards_does_not_roll():
    tracker, clock = _tracker(start=DAY0 + 300)
    clock.wall -= 3600  # NTP step backwards
    tracker.producer_seen("p1", "alice", MINI)
    assert not tracker.pending
    _roll_to(tracker, clock, DAY0 + 600)
    assert _rows(tracker, "window")[0]["robots_distinct"]["reachy_mini"] == 1


def test_daily_rollup_full_day():
    tracker, clock = _tracker(start=DAY0, window_seconds=3600)
    tracker.producer_seen("p1", "alice", MINI)
    _roll_to(tracker, clock, DAY0 + 3 * 3600)
    tracker.producer_seen("p1", "alice", MINI)  # heartbeat, other window
    tracker.producer_seen("p2", "bob", DUCK)
    tracker.producer_gone("p2")
    tracker.session_started("s1", MINI)
    clock.advance(30)
    tracker.session_ended("s1", SESSION_END_ENDED)
    for hour in range(4, 25):
        _roll_to(tracker, clock, DAY0 + hour * 3600)
    (day,) = _rows(tracker, "daily")
    assert day["day"] == "2026-09-30"
    assert day["partial"] is False
    assert day["coverage_s"] == 86400
    assert day["robots_distinct"] == {"reachy_mini": 1, "microduck": 1, "other": 0}
    assert day["sessions_started"]["reachy_mini"] == 1
    assert day["session_durations"]["reachy_mini"]["count"] == 1
    # Day row is emitted after the day's last window.
    types = [t for t, _ in tracker.pending]
    assert types[-1] == "daily" and types.count("window") == 24


def test_daily_distinct_is_not_sum_of_windows_and_next_day_is_seeded():
    tracker, clock = _tracker(start=DAY0, window_seconds=21600)
    tracker.producer_seen("p1", "alice", MINI)  # connected all day
    for q in range(1, 5):
        _roll_to(tracker, clock, DAY0 + q * 21600)
    windows, (day,) = _rows(tracker, "window"), _rows(tracker, "daily")
    assert [w["robots_distinct"]["reachy_mini"] for w in windows] == [1, 1, 1, 1]
    assert day["robots_distinct"]["reachy_mini"] == 1
    _roll_to(tracker, clock, DAY0 + 2 * 86400)
    assert _rows(tracker, "daily")[1]["robots_distinct"]["reachy_mini"] == 1


def test_daily_partial_when_process_started_after_midnight():
    tracker, clock = _tracker(start=DAY0 + 6 * 3600 + 30)
    _roll_to(tracker, clock, DAY0 + 86400)
    (day,) = _rows(tracker, "daily")
    assert day["partial"] is True
    assert day["coverage_s"] == 18 * 3600 - 30


def test_snapshot_current_is_partial():
    tracker, clock = _tracker(start=DAY0 + 1200)
    tracker.producer_seen("p1", "alice", MINI)
    clock.advance(90)
    window, daily = tracker.snapshot_current()
    assert window[0] == "window" and window[1]["coverage_s"] == 90
    assert window[1]["robots_distinct"]["reachy_mini"] == 1
    assert daily[0] == "daily" and daily[1]["partial"] is True
    assert daily[1]["coverage_s"] == 90
    assert not tracker.pending


def test_pending_is_bounded_and_ack_is_identity_based():
    clock = FakeClock(DAY0)
    tracker = UsageTracker(60, wall_clock=clock.wall_clock, mono_clock=clock.mono_clock, pending_max_rows=5)
    for i in range(1, 8):
        _roll_to(tracker, clock, DAY0 + i * 60)
    assert len(tracker.pending) == 5 and tracker.dropped_rows == 2
    snap = tracker.pending_snapshot()
    _roll_to(tracker, clock, DAY0 + 8 * 60)  # evicts snap[0], appends a new row
    tracker.ack_pending(snap)
    assert [r["window_start"] for _, r in tracker.pending] == ["2026-09-30T00:07:00Z"]


# ----------------------------------------------------------------------
# Distinct-key caps (client-controlled hardware_id)
# ----------------------------------------------------------------------


def test_one_user_churning_hardware_ids_stays_capped(caplog):
    tracker, clock = _tracker()
    with caplog.at_level(logging.WARNING):
        for i in range(1000):
            tracker.producer_seen("attacker-peer", "mallory", {"hardware_id": f"fake-{i}"})
        tracker.producer_seen("p-alice", "alice", MINI)
        _roll_to(tracker, clock, DAY0 + 600)
        for i in range(1000, 1500):
            tracker.producer_seen("attacker-peer", "mallory", {"hardware_id": f"fake-{i}"})
        _roll_to(tracker, clock, DAY0 + 1200)
    w1, w2 = _rows(tracker, "window")
    assert w1["robots_distinct"]["reachy_mini"] == USAGE_MAX_KEYS_PER_USER + 1  # + alice
    # Seeded with the attacker's current key and alice, then capped again.
    assert w2["robots_distinct"]["reachy_mini"] == USAGE_MAX_KEYS_PER_USER + 1
    assert tracker._day.distinct.counts()["reachy_mini"] == USAGE_MAX_KEYS_PER_USER + 1
    assert sum(len(v) for v in tracker._day.distinct.by_kind.values()) == USAGE_MAX_KEYS_PER_USER + 1
    # Peak is concurrency, not churn: one attacker peer + alice.
    assert w1["robots_peak"]["reachy_mini"] == 2
    drops = [r for r in caplog.records if "over the distinct-key caps" in r.getMessage()]
    assert len(drops) == 2  # one line per window, not per sighting


def test_per_kind_cap(monkeypatch):
    monkeypatch.setattr(fleet_usage, "USAGE_MAX_KEYS_PER_SET", 5)
    tracker, clock = _tracker()
    for i in range(10):
        tracker.producer_seen(f"p{i}", f"user{i}", {"hardware_id": f"hw{i}"})
    _roll_to(tracker, clock, DAY0 + 600)
    assert _rows(tracker, "window")[0]["robots_distinct"]["reachy_mini"] == 5


# ----------------------------------------------------------------------
# Integration with SignalingServer
# ----------------------------------------------------------------------

_tok = 0


def _peer(server: SignalingServer, username: str = "alice"):
    global _tok
    _tok += 1
    return server.get_or_create_peer(f"tok-usage-{_tok}", username)


async def _producer(server, username="alice", meta=MINI):
    p = _peer(server, username)
    await server.handle_message(p, {"type": "setPeerStatus", "roles": ["producer"], "meta": dict(meta)})
    return p


async def _session(server, producer, username="alice"):
    consumer = _peer(server, username)
    await server.handle_message(consumer, {"type": "setPeerStatus", "roles": ["listener"], "meta": {"name": "app"}})
    resp = await server.handle_message(consumer, {"type": "startSession", "peerId": producer.peer_id})
    assert resp["type"] == "sessionStarted"
    return consumer, resp["sessionId"]


def _end_reasons(tracker, clock):
    _roll_to(tracker, clock, clock.wall + 600)
    return _rows(tracker, "window")[-1]["session_end_reasons"]


async def test_end_reason_explicit_end_session_ignores_client_reason():
    tracker, clock = _tracker()
    server = SignalingServer(usage=tracker)
    producer = await _producer(server)
    consumer, sid = await _session(server, producer)
    # A client-chosen reason that mimics a server one must not leak or steer the category.
    await server.handle_message(consumer, {"type": "endSession", "sessionId": sid, "reason": "install_id_takeover"})
    reasons = _end_reasons(tracker, clock)
    assert reasons[SESSION_END_ENDED] == 1 and sum(reasons.values()) == 1


async def test_end_reason_withdraw():
    tracker, clock = _tracker()
    server = SignalingServer(usage=tracker)
    producer = await _producer(server)
    await _session(server, producer)
    await server.handle_message(producer, {"type": "setPeerStatus", "roles": [], "meta": dict(MINI)})
    assert _end_reasons(tracker, clock)[SESSION_END_WITHDRAWN] == 1


async def test_end_reason_swept(monkeypatch):
    tracker, clock = _tracker()
    server = SignalingServer(usage=tracker)
    producer = await _producer(server)
    await _session(server, producer)
    producer.last_seen -= app_module.PRODUCER_LEASE_SECONDS + 1
    assert await server.sweep_stale_producers() == [producer.peer_id]
    assert _end_reasons(tracker, clock)[SESSION_END_SWEPT] == 1
    assert tracker._producers == {}


async def test_end_reason_stable_id_takeover():
    tracker, clock = _tracker()
    server = SignalingServer(usage=tracker)
    old = await _producer(server)
    await _session(server, old)
    await _producer(server)  # same hardware_id, same user: evicts old
    reasons = _end_reasons(tracker, clock)
    assert reasons[SESSION_END_REPLACED] == 1 and sum(reasons.values()) == 1
    row = _rows(tracker, "window")[0]
    assert row["robots_distinct"]["reachy_mini"] == 1
    assert row["robots_peak"]["reachy_mini"] == 1


async def test_end_reason_direct_call_is_other():
    tracker, clock = _tracker()
    server = SignalingServer(usage=tracker)
    producer = await _producer(server)
    _, sid = await _session(server, producer)
    await server.handle_end_session(sid, reason="whatever")
    assert _end_reasons(tracker, clock)[SESSION_END_OTHER] == 1


class _ClosedRequest:
    """Minimal Request stand-in whose client has already gone away."""

    client = None

    async def is_disconnected(self) -> bool:
        return True


async def test_end_reason_sse_drop_through_events_route(monkeypatch):
    """The real ``/events`` generator: its close path categorises as peer_disconnected.

    Grace disabled: the close evicts at once (the SSE grace variant is
    ``test_sse_grace.py::test_route_sse_close_then_expiry_records_peer_disconnected``).
    """
    tracker, clock = _tracker()
    server = SignalingServer(usage=tracker, sse_grace_seconds=0)
    monkeypatch.setattr(app_module, "signaling", server)
    monkeypatch.setitem(token_cache, "tok-sse-producer", ("alice", float("inf")))
    producer = server.get_or_create_peer("tok-sse-producer", "alice")
    await server.handle_message(producer, {"type": "setPeerStatus", "roles": ["producer"], "meta": dict(MINI)})
    await _session(server, producer)

    response = await app_module.events(_ClosedRequest(), token="tok-sse-producer")
    async for _ in response.body_iterator:
        pass

    assert producer.peer_id not in server.peers
    assert _end_reasons(tracker, clock)[SESSION_END_PEER_DISCONNECTED] == 1


async def test_tracker_exceptions_never_break_signalling(monkeypatch, caplog):
    tracker, clock = _tracker()
    server = SignalingServer(usage=tracker)

    def boom(*args, **kwargs):
        raise RuntimeError("tracker bug")

    for name in ("producer_seen", "producer_gone", "session_started", "session_ended", "maybe_roll"):
        monkeypatch.setattr(tracker, name, boom)

    with caplog.at_level(logging.ERROR):
        producer = await _producer(server)
        await server.handle_message(producer, {"type": "setPeerStatus", "roles": ["producer"], "meta": dict(MINI)})
        consumer, sid = await _session(server, producer)
        assert (await server.handle_message(consumer, {"type": "list"}))["producers"][0]["busy"] is True
        await server.handle_message(consumer, {"type": "endSession", "sessionId": sid})
        assert server.sessions == {}
        await server.handle_message(producer, {"type": "setPeerStatus", "roles": [], "meta": dict(MINI)})
        await server.disconnect_peer(producer.peer_id)
        server._track("maybe_roll")

    assert producer.peer_id not in server.peers
    # One traceback per hook name, not one per call.
    failures = [r for r in caplog.records if "Fleet usage hook" in r.getMessage()]
    assert len(failures) == 5


async def test_server_without_tracker_works():
    server = SignalingServer()
    producer = await _producer(server)
    await _session(server, producer)
    assert len(server.sessions) == 1




# ----------------------------------------------------------------------
# Publisher
# ----------------------------------------------------------------------

REPO = "pollen-robotics/fleet_usage_test"
SECRET = "hf_SUPERSECRETTOKEN123"
TODAY_FILE = "data/windows/2026-09-30.jsonl"


def _not_found(filename: str) -> RemoteEntryNotFoundError:
    """What huggingface_hub 2.0.0 raises for a real 404 on a file."""
    url = f"https://huggingface.co/datasets/{REPO}/resolve/main/{filename}"
    return RemoteEntryNotFoundError(
        f"404 Client Error. Entry Not Found for url: {url}",
        response=httpx2.Response(404, request=httpx2.Request("HEAD", url)),
    )


class FakeHfApi:
    """In-memory stand-in for ``HfApi`` (``hf_hub_download`` + ``create_commit``).

    Raises the real huggingface_hub exception classes: ``RemoteEntryNotFoundError``
    for a missing file, and whatever is queued in ``download_errors`` (per
    filename) or ``fail_commit``.
    """

    def __init__(self, files: dict[str, str] | None = None):
        self.files = dict(files or {})
        self.commits: list[dict] = []
        self.downloads: list[str] = []
        self.fail_commit: Exception | None = None
        self.download_errors: dict[str, Exception] = {}

    def hf_hub_download(self, *, repo_id, filename, repo_type, cache_dir, token):
        assert repo_id == REPO and repo_type == "dataset" and token == SECRET
        self.downloads.append(filename)
        error = self.download_errors.get(filename) or self.download_errors.get("*")
        if error is not None:
            raise error
        if filename not in self.files:
            raise _not_found(filename)
        path = os.path.join(cache_dir, filename.replace("/", "_"))
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.files[filename])
        return path

    def create_commit(self, *, repo_id, repo_type, operations, commit_message, token):
        assert repo_id == REPO and repo_type == "dataset" and token == SECRET
        if self.fail_commit:
            raise self.fail_commit
        paths = []
        for op in operations:
            self.files[op.path_in_repo] = op.path_or_fileobj.decode("utf-8")
            paths.append(op.path_in_repo)
        self.commits.append({"paths": sorted(paths), "message": commit_message})


def _jsonl_rows(text: str) -> list[dict]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _publisher(api: FakeHfApi, start: float = DAY0 + 3 * 600 + 180):
    tracker, clock = _tracker(start=start)
    sink = HfDatasetUsageSink(REPO, SECRET, api=api)
    publisher = FleetUsagePublisher(tracker, sink, publish_seconds=600, wall_clock=clock.wall_clock)
    return publisher, tracker, clock


def _window_row(start: str, coverage: int, distinct: int, started: int, peak: int = 1) -> dict:
    zeros = {k: 0 for k in PUBLIC_ROBOT_KINDS}
    mini = lambda k, v, other: v if k == "reachy_mini" else other  # noqa: E731
    return {
        "schema_version": 1,
        "window_start": start,
        "window_seconds": 600,
        "coverage_s": coverage,
        "robots_distinct": {**zeros, "reachy_mini": distinct},
        "robots_peak": {**zeros, "reachy_mini": peak},
        "sessions_started": {**zeros, "reachy_mini": started},
        "sessions_peak": {**zeros, "reachy_mini": 1},
        "session_durations": {
            k: {"count": mini(k, started, 0), "sum_s": mini(k, 12.5 * started, 0.0),
                "max_s": mini(k, 12.5 if started else 0.0, 0.0), "hist": [0, mini(k, started, 0), 0, 0, 0, 0]}
            for k in PUBLIC_ROBOT_KINDS
        },
        "session_end_reasons": {r: (started if r == "ended" else 0) for r in SESSION_END_REASONS},
    }


OLD_DAY = {"schema_version": 1, "day": "2026-09-29", "coverage_s": 86400, "partial": False,
           "robots_distinct": {"reachy_mini": 9, "microduck": 1, "other": 0},
           "sessions_started": {"reachy_mini": 40, "microduck": 0, "other": 0},
           "session_durations": {}}


def _remote_state() -> dict[str, str]:
    """Remote files left by a previous process that shut down at 00:33:00."""
    remote_w0 = _window_row("2026-09-30T00:20:00Z", 600, 5, 2)
    remote_partial = _window_row("2026-09-30T00:30:00Z", 180, 3, 2, peak=3)
    yesterday = _window_row("2026-09-29T23:50:00Z", 600, 8, 4)
    return {
        TODAY_FILE: _to_jsonl_rows([remote_w0, remote_partial]),
        "data/windows/2026-09-29.jsonl": _to_jsonl_rows([yesterday]),
        USAGE_DAILY_PATH: json.dumps(OLD_DAY) + "\n",
        USAGE_SUMMARY_PATH: '{"schema_version":1}',
    }


def _to_jsonl_rows(rows: list[dict]) -> str:
    return "".join(json.dumps(r) + "\n" for r in rows)


async def test_bootstrap_merges_partial_rows_and_commits_once():
    api = FakeHfApi(_remote_state())
    publisher, tracker, clock = _publisher(api)  # restarted at 00:33:00
    tracker.producer_seen("p1", "alice", MINI)
    tracker.producer_seen("p2", "bob", MINI2)
    tracker.session_started("s1", MINI)
    clock.advance(30)
    tracker.session_ended("s1", SESSION_END_ENDED)
    _roll_to(tracker, clock, DAY0 + 2400)

    assert await publisher.publish_once(timeout=5)
    assert len(api.commits) == 1
    assert api.commits[0]["paths"] == [TODAY_FILE, USAGE_SUMMARY_PATH]
    assert not tracker.pending
    # Bootstrap reads the daily file and the retention horizon of day files,
    # never summary.json (it is derived).
    assert USAGE_SUMMARY_PATH not in api.downloads
    assert USAGE_DAILY_PATH in api.downloads and "data/windows/2026-09-22.jsonl" in api.downloads

    rows = _jsonl_rows(api.files[TODAY_FILE])
    assert [r["window_start"] for r in rows] == ["2026-09-30T00:20:00Z", "2026-09-30T00:30:00Z"]
    merged = rows[1]
    assert merged["coverage_s"] == 600  # 180 + 420
    assert merged["robots_distinct"]["reachy_mini"] == 3  # max(3, 2)
    assert merged["robots_peak"]["reachy_mini"] == 3
    assert merged["sessions_started"]["reachy_mini"] == 3  # 2 + 1
    assert merged["session_durations"]["reachy_mini"]["count"] == 3
    assert merged["session_durations"]["reachy_mini"]["hist"] == [0, 3, 0, 0, 0, 0]
    assert merged["session_end_reasons"]["ended"] == 3
    assert api.files[USAGE_DAILY_PATH] == json.dumps(OLD_DAY) + "\n"  # untouched

    summary = json.loads(api.files[USAGE_SUMMARY_PATH])
    assert summary["generated_at"] == "2026-09-30T00:40:00Z"
    recent = summary["recent"]
    assert recent["step_seconds"] == 600 and recent["count"] == 144
    assert recent["start"] == "2026-09-29T00:40:00Z"
    # Remote rows from yesterday's file and today's merged rows all feed the series.
    by_start = {i: v for i, v in enumerate(recent["robots_distinct"]["reachy_mini"]) if v is not None}
    assert list(by_start.values()) == [8, 5, 3]
    assert summary["daily"]["days"] == ["2026-09-29"]


async def test_complete_remote_row_is_replaced_by_ours():
    remote = _window_row("2026-09-30T00:30:00Z", 600, 7, 7)
    api = FakeHfApi({TODAY_FILE: json.dumps(remote) + "\n"})
    publisher, tracker, clock = _publisher(api, start=DAY0 + 1800)
    tracker.producer_seen("p1", "alice", MINI)
    _roll_to(tracker, clock, DAY0 + 2400)
    assert await publisher.publish_once(timeout=5)
    (row,) = _jsonl_rows(api.files[TODAY_FILE])
    assert row["robots_distinct"]["reachy_mini"] == 1 and row["sessions_started"]["reachy_mini"] == 0


async def test_empty_remote_and_nothing_pending():
    api = FakeHfApi()
    publisher, tracker, clock = _publisher(api)
    assert await publisher.publish_once(timeout=5)  # nothing closed yet
    assert api.commits == [] and api.downloads == []
    _roll_to(tracker, clock, DAY0 + 2400)
    assert await publisher.publish_once(timeout=5)
    assert len(api.downloads) == 1 + fleet_usage.USAGE_WINDOW_RETENTION_DAYS + 1
    assert len(_jsonl_rows(api.files[TODAY_FILE])) == 1


@pytest.mark.parametrize("failing", [USAGE_DAILY_PATH, TODAY_FILE, "data/windows/2026-09-25.jsonl"])
async def test_transient_error_during_bootstrap_publishes_nothing(failing):
    """huggingface_hub maps connection errors / timeouts / 5xx to LocalEntryNotFoundError."""
    api = FakeHfApi(_remote_state())
    before = dict(api.files)
    publisher, tracker, clock = _publisher(api)
    _roll_to(tracker, clock, DAY0 + 2400)
    api.download_errors[failing] = LocalEntryNotFoundError("Connection error (HF 502), no cached file")

    assert not await publisher.publish_once(timeout=5)
    assert api.commits == [] and api.files == before
    assert len(tracker.pending) == 1
    assert not publisher._bootstrapped
    assert publisher._daily == {} and publisher._windows == {} and publisher._loaded_days == set()

    api.download_errors.clear()
    assert await publisher.publish_once(timeout=5)
    assert not tracker.pending and len(api.commits) == 1
    assert api.files[USAGE_DAILY_PATH] == before[USAGE_DAILY_PATH]
    starts = [r["window_start"] for r in _jsonl_rows(api.files[TODAY_FILE])]
    assert starts == ["2026-09-30T00:20:00Z", "2026-09-30T00:30:00Z"]


async def test_transient_error_on_late_day_file_publishes_nothing():
    old_file = "data/windows/2026-09-10.jsonl"
    old_row = _window_row("2026-09-10T12:00:00Z", 600, 4, 1)
    api = FakeHfApi({old_file: json.dumps(old_row) + "\n"})
    publisher, tracker, clock = _publisher(api)
    _roll_to(tracker, clock, DAY0 + 2400)
    assert await publisher.publish_once(timeout=5)  # bootstrapped

    # A row for a day outside the bootstrap horizon (e.g. a long-stuck buffer).
    late = _window_row("2026-09-10T12:10:00Z", 600, 2, 0)
    tracker.pending.append(("window", late))
    api.download_errors[old_file] = LocalEntryNotFoundError("timeout")
    n_commits = len(api.commits)
    assert not await publisher.publish_once(timeout=5)
    assert len(api.commits) == n_commits and "2026-09-10" not in publisher._loaded_days
    assert len(tracker.pending) == 1

    api.download_errors.clear()
    assert await publisher.publish_once(timeout=5)
    starts = [r["window_start"] for r in _jsonl_rows(api.files[old_file])]
    assert starts == ["2026-09-10T12:00:00Z", "2026-09-10T12:10:00Z"]


async def test_failed_commit_keeps_rows_and_retry_is_idempotent(caplog):
    remote_partial = _window_row("2026-09-30T00:30:00Z", 180, 3, 2)
    api = FakeHfApi({TODAY_FILE: json.dumps(remote_partial) + "\n"})
    publisher, tracker, clock = _publisher(api)
    tracker.session_started("s1", MINI)
    _roll_to(tracker, clock, DAY0 + 2400)
    api.fail_commit = RuntimeError(f"502 Bad Gateway (Authorization: Bearer {SECRET})")

    with caplog.at_level(logging.DEBUG):
        assert not await publisher.publish_once(timeout=5)
    assert len(tracker.pending) == 1 and api.commits == []
    assert any("kept for retry" in r.getMessage() for r in caplog.records)

    api.fail_commit = None
    _roll_to(tracker, clock, DAY0 + 3000)
    assert await publisher.publish_once(timeout=5)
    assert not tracker.pending
    rows = _jsonl_rows(api.files[TODAY_FILE])
    assert [r["window_start"] for r in rows] == ["2026-09-30T00:30:00Z", "2026-09-30T00:40:00Z"]
    assert rows[0]["sessions_started"]["reachy_mini"] == 3  # merged exactly once

    # Re-publishing our own row (e.g. a commit that landed after a timeout)
    # replaces it instead of merging it a second time.
    await asyncio.to_thread(publisher._publish_blocking, [("window", rows[0])])
    again = _jsonl_rows(api.files[TODAY_FILE])
    assert again[0]["sessions_started"]["reachy_mini"] == 3


async def test_publish_timeout_keeps_rows_until_the_attempt_completes():
    api = FakeHfApi()
    gate = threading.Event()
    real_commit = api.create_commit

    def slow_commit(**kwargs):
        gate.wait(5)
        real_commit(**kwargs)

    api.create_commit = slow_commit
    publisher, tracker, clock = _publisher(api)
    _roll_to(tracker, clock, DAY0 + 2400)
    assert not await publisher.publish_once(timeout=0.05)
    assert len(tracker.pending) == 1
    assert not await publisher.publish_once(timeout=0.05)  # still in flight: skipped
    gate.set()
    await publisher._inflight
    await asyncio.sleep(0)
    assert not tracker.pending and len(api.commits) == 1


async def test_wedged_commit_never_blocks_cycles_or_shutdown():
    api = FakeHfApi()
    never = threading.Event()
    api.create_commit = lambda **kwargs: never.wait()  # a half-dead connection
    publisher, tracker, clock = _publisher(api)
    _roll_to(tracker, clock, DAY0 + 2400)
    try:
        assert not await publisher.publish_once(timeout=0.05)
        for _ in range(3):
            started = time.monotonic()
            assert not await publisher.publish_once(timeout=5)  # skipped, not queued
            assert time.monotonic() - started < 0.1
        started = time.monotonic()
        assert not await publisher.final_publish(timeout=0.3)
        assert time.monotonic() - started < 1.0
        assert len(tracker.pending) == 1
        wedged = [t for t in threading.enumerate() if t.name == "fleet-usage-publish"]
        assert wedged and all(t.daemon for t in wedged)
    finally:
        never.set()


def test_bounded_hub_client_timeout(monkeypatch):
    from huggingface_hub import set_client_factory
    from huggingface_hub.utils import _http, get_session

    monkeypatch.setattr(fleet_usage, "_hub_client_bounded", False)
    fleet_usage._install_bounded_hub_client()
    try:
        timeout = get_session().timeout
        assert timeout.read == timeout.connect == fleet_usage.USAGE_HF_HTTP_TIMEOUT_SECONDS
    finally:
        set_client_factory(_http.default_client_factory)


async def test_daily_rows_and_final_publish_with_partial_window():
    api = FakeHfApi()
    publisher, tracker, clock = _publisher(api, start=DAY0 + 86400 - 600)
    tracker.producer_seen("p1", "alice", DUCK)
    _roll_to(tracker, clock, DAY0 + 86400 + 90)  # crosses midnight
    assert await publisher.publish_once(timeout=5)
    assert api.commits[0]["paths"] == [USAGE_DAILY_PATH, TODAY_FILE, USAGE_SUMMARY_PATH]
    (day,) = _jsonl_rows(api.files[USAGE_DAILY_PATH])
    assert day["day"] == "2026-09-30" and day["partial"] is True and day["coverage_s"] == 600
    assert day["robots_distinct"]["microduck"] == 1

    clock.advance(30)
    assert await publisher.final_publish(timeout=5)
    (partial,) = _jsonl_rows(api.files["data/windows/2026-10-01.jsonl"])
    assert partial["window_start"] == "2026-10-01T00:00:00Z" and partial["coverage_s"] == 120
    assert partial["robots_distinct"]["microduck"] == 1
    days = _jsonl_rows(api.files[USAGE_DAILY_PATH])
    assert [d["day"] for d in days] == ["2026-09-30", "2026-10-01"]
    assert days[1]["partial"] is True and days[1]["coverage_s"] == 120
    summary = json.loads(api.files[USAGE_SUMMARY_PATH])
    assert summary["daily"]["days"] == ["2026-09-30", "2026-10-01"]
    assert summary["daily"]["partial"] == [True, True]
    assert summary["daily"]["robots_distinct"]["microduck"] == [1, 1]


async def test_token_never_logged(caplog):
    api = FakeHfApi()
    api.download_errors["*"] = PermissionError(f"401 for token {SECRET}")
    with caplog.at_level(logging.DEBUG):
        config = fleet_usage_config_from_env({"FLEET_USAGE_DATASET": REPO, "FLEET_USAGE_HF_TOKEN": SECRET})
        publisher = build_usage_publisher(config, UsageTracker(), hf_api=api)
        logging.getLogger("fleet_usage").info("config=%r sink=%r", config, publisher.sink)
        publisher.tracker.pending.append(("window", _window_row("2026-09-30T00:00:00Z", 600, 1, 0)))
        assert not await publisher.publish_once(timeout=5)
        api.download_errors.clear()
        assert await publisher.publish_once(timeout=5)
    assert caplog.records
    assert all(SECRET not in r.getMessage() for r in caplog.records)
    assert SECRET not in caplog.text


async def test_local_dir_sink_round_trip(tmp_path):
    tracker, clock = _tracker(start=DAY0)
    publisher = FleetUsagePublisher(tracker, LocalDirUsageSink(str(tmp_path)), wall_clock=clock.wall_clock)
    tracker.producer_seen("p1", "alice", MINI)
    _roll_to(tracker, clock, DAY0 + 600)
    assert await publisher.publish_once(timeout=5)
    assert (tmp_path / "data" / "windows" / "2026-09-30.jsonl").exists()
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["recent"]["robots_distinct"]["reachy_mini"][-1] == 1

    # A restarted process bootstraps from the same directory.
    tracker2, clock2 = _tracker(start=DAY0 + 1200)
    publisher2 = FleetUsagePublisher(tracker2, LocalDirUsageSink(str(tmp_path)), wall_clock=clock2.wall_clock)
    _roll_to(tracker2, clock2, DAY0 + 1800)
    assert await publisher2.publish_once(timeout=5)
    rows = _jsonl_rows((tmp_path / "data" / "windows" / "2026-09-30.jsonl").read_text())
    assert [r["window_start"] for r in rows] == ["2026-09-30T00:00:00Z", "2026-09-30T00:20:00Z"]
    assert not list(tmp_path.rglob("*.tmp"))


def test_local_dir_sink_refuses_escaping_paths(tmp_path):
    with pytest.raises(ValueError):
        LocalDirUsageSink(str(tmp_path)).read("../outside.json")


def test_merge_daily_rows_stays_partial():
    a = {"day": "2026-09-30", "coverage_s": 40000, "partial": True,
         "robots_distinct": {"reachy_mini": 4}, "sessions_started": {"reachy_mini": 2},
         "session_durations": {"reachy_mini": {"count": 2, "sum_s": 5.0, "max_s": 3.0, "hist": [2, 0, 0, 0, 0, 0]}}}
    b = {**a, "coverage_s": 50000, "robots_distinct": {"reachy_mini": 6}}
    merged = merge_usage_rows("daily", a, b)
    assert merged["coverage_s"] == 86400 and merged["partial"] is True
    assert merged["robots_distinct"]["reachy_mini"] == 6
    assert merged["sessions_started"]["reachy_mini"] == 4
    assert merged["session_durations"]["reachy_mini"]["hist"] == [4, 0, 0, 0, 0, 0]


# ----------------------------------------------------------------------
# summary.json
# ----------------------------------------------------------------------


def _hour_rows(hour_start: str, distinct: list[int], started: list[int]) -> dict[str, dict]:
    base = datetime.strptime(hour_start, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
    rows = {}
    for i, (d, s) in enumerate(zip(distinct, started)):
        key = datetime.fromtimestamp(base + i * 600, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows[key] = _window_row(key, 600, d, s, peak=d)
    return rows


def test_summary_hourly_aggregation_and_gaps():
    rows = _hour_rows("2026-09-30T10:00:00Z", [3, 5, 4], [1, 2, 0])  # 3 of 6 windows observed
    summary = build_summary(rows, {}, DAY0 + 12 * 3600 + 5, 600)
    hourly = summary["hourly"]
    assert hourly["step_seconds"] == 3600 and hourly["count"] == 168
    assert hourly["start"] == "2026-09-23T13:00:00Z"
    i = hourly["count"] - 3  # 10:00 slot; 12:00 is the last (current) hour
    assert hourly["robots_distinct"]["reachy_mini"][i] == 5  # max over windows
    assert hourly["robots_peak"]["reachy_mini"][i] == 5
    assert hourly["sessions_started"]["reachy_mini"][i] == 3  # sum
    assert hourly["coverage_s"][i] == 1800
    assert hourly["durations"]["count"][i] == 3
    assert hourly["durations"]["hist"][i] == [0, 3, 0, 0, 0, 0]
    assert hourly["robots_distinct"]["reachy_mini"][i + 1] is None  # 11:00 not observed
    recent = summary["recent"]
    assert recent["count"] == 144 and recent["start"] == "2026-09-29T12:00:00Z"
    assert [v for v in recent["robots_distinct"]["reachy_mini"] if v is not None] == [3, 5, 4]


def test_summary_size_is_small_with_full_history():
    now = DAY0 + 86400 * 400 + 3600 * 11 + 120
    windows: dict[str, dict] = {}
    start = int(now // 600) * 600 - 8 * 86400
    for t in range(start, int(now // 600) * 600, 600):
        key = datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        row = _window_row(key, 600, 287, 31, peak=214)
        row["robots_distinct"]["microduck"] = 12
        row["session_durations"]["microduck"] = {"count": 3, "sum_s": 1234.5, "max_s": 999.9, "hist": [1, 0, 1, 1, 0, 0]}
        windows[key] = row
    daily = {}
    for d in range(400):
        day = datetime.fromtimestamp(DAY0 + d * 86400, timezone.utc).strftime("%Y-%m-%d")
        daily[day] = {**OLD_DAY, "day": day, "robots_distinct": {"reachy_mini": 301, "microduck": 14, "other": 1}}
    size = len(json.dumps(build_summary(windows, daily, now, 600), separators=(",", ":")))
    assert size < 60_000, size


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------


def test_unconfigured_disables_publishing_and_page_section():
    config = fleet_usage_config_from_env({})
    assert config == FleetUsageConfig()
    assert config.summary_url is None and config.publish_seconds == 1800.0
    assert build_usage_publisher(config, UsageTracker()) is None


def test_dataset_without_token_shows_page_but_does_not_publish():
    config = fleet_usage_config_from_env({"FLEET_USAGE_DATASET": REPO})
    assert config.summary_url == f"https://huggingface.co/datasets/{REPO}/resolve/main/summary.json"
    assert build_usage_publisher(config, UsageTracker()) is None


@pytest.mark.parametrize(
    "repo",
    ["noslash", "a/b/c", "-lead/x", "a/b\n<x>", "a/b<script>", "a b/c", "a/\"b", "/b"],
)
def test_invalid_dataset_ids_are_rejected(repo):
    assert fleet_usage_config_from_env({"FLEET_USAGE_DATASET": repo}).dataset is None


def test_local_dir_refused_on_space(tmp_path):
    env = {"FLEET_USAGE_LOCAL_DIR": str(tmp_path)}
    assert fleet_usage_config_from_env(env).local_dir == str(tmp_path)
    assert fleet_usage_config_from_env(env).summary_url == "/dev/fleet-usage/summary.json"
    assert fleet_usage_config_from_env({**env, "SPACE_ID": "pollen-robotics/x"}).local_dir is None
    publisher = build_usage_publisher(fleet_usage_config_from_env(env), UsageTracker())
    assert isinstance(publisher.sink, LocalDirUsageSink)


def test_numeric_settings_are_validated(tmp_path):
    local = {"FLEET_USAGE_LOCAL_DIR": str(tmp_path)}
    ok = fleet_usage_config_from_env({**local, "FLEET_USAGE_WINDOW_SECONDS": "60", "FLEET_USAGE_PUBLISH_SECONDS": "20"})
    assert (ok.window_seconds, ok.publish_seconds) == (60, 20.0)
    bad = fleet_usage_config_from_env({**local, "FLEET_USAGE_WINDOW_SECONDS": "7", "FLEET_USAGE_PUBLISH_SECONDS": "0"})
    assert (bad.window_seconds, bad.publish_seconds) == (600, 1800.0)


def test_window_seconds_forced_to_600_for_the_dataset(caplog):
    with caplog.at_level(logging.WARNING):
        config = fleet_usage_config_from_env(
            {"FLEET_USAGE_DATASET": REPO, "FLEET_USAGE_HF_TOKEN": SECRET, "FLEET_USAGE_WINDOW_SECONDS": "60"}
        )
    assert config.window_seconds == 600
    assert any("only honoured with FLEET_USAGE_LOCAL_DIR" in r.getMessage() for r in caplog.records)


def test_lifespan_runs_final_publish_on_shutdown(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    tracker, clock = _tracker(start=DAY0 + 120)  # frozen clock: no boundary can be crossed
    publisher = FleetUsagePublisher(
        tracker, LocalDirUsageSink(str(tmp_path)), publish_seconds=3600, wall_clock=clock.wall_clock
    )
    monkeypatch.setattr(app_module, "usage_publisher", publisher)
    with TestClient(app_module.app) as client:
        assert client.get("/health").status_code == 200
        assert not (tmp_path / "summary.json").exists()
        clock.advance(60)
    (row,) = _jsonl_rows((tmp_path / "data" / "windows" / "2026-09-30.jsonl").read_text())
    assert row["window_start"] == "2026-09-30T00:00:00Z" and row["coverage_s"] == 60
    (day,) = _jsonl_rows((tmp_path / "data" / "daily.jsonl").read_text())
    assert day["partial"] is True and day["coverage_s"] == 60
