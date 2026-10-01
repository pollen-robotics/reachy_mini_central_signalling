"""
Reachy Mini Central - WebRTC Signaling Server

This server implements the GStreamer WebRTC signaling protocol using:
- SSE (Server-Sent Events) for server-to-client messages
- HTTP POST for client-to-server messages

This works reliably through HTTP/2 proxies like HuggingFace Spaces.

Design: central is a **stateless matchmaker**. It bootstraps WebRTC
sessions between mobile/desktop consumers and robot-daemon producers,
relays SDP/ICE, and maintains a producer registry keyed by a stable
``token -> peer_id`` mapping. **Session** liveness is owned by the
daemon, which has direct visibility into the peer connection's data
channel and ICE state: when its watchdog decides a session is idle, it
closes its PC and announces availability via ``setPeerStatus`` (or
sends an explicit ``endSession``).

**Registration** liveness, however, is owned by central: a producer
whose process dies without closing its socket (power cut, yanked
Wi-Fi) leaves a half-open SSE channel that ``request.is_disconnected()``
never notices behind an HTTP/2 proxy, so the robot would stay listed
as connectable forever. The producer sweep (see the Liveness section
below) evicts producers with no inbound traffic for
``PRODUCER_LEASE_SECONDS``, keyed exclusively on inbound ``POST /send``
activity - never on SSE delivery, so back-pressure on a healthy
consumer can not tear down a healthy media session (the bug that got
the previous TTL sweeper removed).

Lifecycle responsibilities (this list is the canonical contract; the
``meta`` keys the server interprets are documented in
``docs/META_CONTRACT.md``):

- Forward producer ``meta`` verbatim to listeners (no re-interpretation).
  The only server-side reading of ``meta`` for *public* output is the
  robot-kind classification behind the ``/health`` and status-page
  counters (``producers_by_kind``), which collapses the untrusted
  ``meta.kind`` into a bounded set of labels (see ``robot_kind_of``).
- Honour ``setPeerStatus(roles=[])`` by removing the peer from
  ``producers`` immediately (the SSE channel stays open so the daemon
  can re-register without reconnecting). The daemon also uses
  ``setPeerStatus(roles=["producer"])`` to flip itself back to
  "available" after its watchdog tears down an idle session.
- Detect ``install_id`` collisions inside a single user's fleet and
  evict the older producer (last-writer-wins) so a re-flashed daemon
  never coexists with its own ghost.
- Honour explicit ``endSession`` messages (daemon shutdown, user
  disconnect, ``robot_busy_local_app``, etc.) and clean up on SSE
  channel close - after a short reconnect grace
  (``SSE_RECONNECT_GRACE_SECONDS``) during which a peer whose SSE stream
  was cut stays registered and can resume with the same peerId.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from string import Template
from typing import AsyncGenerator, Callable, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response
from sse_starlette.sse import EventSourceResponse

# Robot-kind classification lives in ``robot_kinds`` (shared with fleet
# usage); every name is re-exported here for existing importers.
from robot_kinds import (  # noqa: F401
    DEFAULT_ROBOT_KIND,
    KNOWN_ROBOT_KINDS,
    OTHER_ROBOT_KIND,
    PUBLIC_ROBOT_KINDS,
    ROBOT_KIND_LABELS,
    ROBOT_KIND_MAX_RAW_LEN,
    robot_kind_of,
)
import hf_auth
from fleet_usage import (
    DEV_USAGE_SUMMARY_ROUTE,
    SESSION_END_CONSUMER_REPLACED,
    SESSION_END_ENDED,
    SESSION_END_OTHER,
    SESSION_END_PEER_DISCONNECTED,
    SESSION_END_REPLACED,
    SESSION_END_SWEPT,
    SESSION_END_WITHDRAWN,
    USAGE_HOOK_ERROR_LOG_INTERVAL_SECONDS,
    USAGE_SUMMARY_PATH,
    UsageTracker,
    build_usage_publisher,
    fleet_usage_config_from_env,
    publisher_health,
    render_usage_section,
)

# --- Logging ----------------------------------------------------------
#
# HF's Space log view has no timestamps of its own and only keeps a short
# tail, so every line carries an ISO-8601 UTC timestamp, e.g.
# ``2026-10-01T08:21:20Z INFO app: ...``. The ``gmtime`` converter (set
# for every formatter, uvicorn's included) makes the trailing ``Z`` true
# regardless of the container's TZ.
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
LOG_DATEFMT = "%Y-%m-%dT%H:%M:%SZ"
logging.Formatter.converter = time.gmtime


def _timestamp_uvicorn_loggers() -> None:
    """Prefix uvicorn's own (access + error) log lines with the UTC timestamp.

    ``uvicorn`` configures its loggers from its default dict config BEFORE
    it imports this module, so the handlers already exist here. Only
    handlers still using uvicorn's stock formatter (no ``asctime``) are
    touched: an operator-supplied ``--log-config`` is left alone.
    """
    from uvicorn.logging import ColourizedFormatter

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        for handler in logging.getLogger(name).handlers:
            current = handler.formatter
            if not isinstance(current, ColourizedFormatter):
                continue
            fmt = getattr(current, "_fmt", None) or "%(message)s"
            if "asctime" in fmt:
                continue
            handler.setFormatter(
                type(current)(
                    fmt="%(asctime)s " + fmt,
                    datefmt=LOG_DATEFMT,
                    use_colors=current.use_colors,
                )
            )


_TOKEN_QUERY_RE = re.compile(r"([?&](?:token|access_token)=)[^&#\s]*", re.IGNORECASE)


def _redact_token_query(path: str) -> str:
    """``/x?token=hf_abc&y=1`` -> ``/x?token=***&y=1``."""
    return _TOKEN_QUERY_RE.sub(r"\1***", path)


class _AccessLogFilter(logging.Filter):
    """Filter on ``uvicorn.access``: redact tokens, drop routine 2xx lines.

    - Legacy clients still send ``?token=hf_...``; the query value is
      replaced by ``***`` so it never reaches the Space logs.
    - Successful heartbeats (``POST /send``) and status polls
      (``GET /api/robot-status``) are ~30 lines/s for the fleet and
      carry no information, so 2xx lines for exactly those two routes
      are dropped (and counted: ``access_log_filtered`` on the per-minute
      auth summary). Every non-2xx line and every other route is kept.

    Relies on uvicorn's access record shape
    ``(client_addr, method, full_path, http_version, status_code)``;
    any other record only gets its rendered message redacted.
    """

    QUIET_2XX_ROUTES = frozenset({("POST", "/send"), ("GET", "/api/robot-status")})

    def __init__(self) -> None:
        super().__init__()
        self.filtered = 0

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if isinstance(args, tuple) and len(args) == 5:
            client_addr, method, full_path, http_version, status_code = args
            path = str(full_path)
            if (
                isinstance(status_code, int)
                and 200 <= status_code < 300
                and (method, path.split("?", 1)[0]) in self.QUIET_2XX_ROUTES
            ):
                self.filtered += 1
                return False
            if "?" in path:
                record.args = (client_addr, method, _redact_token_query(path), http_version, status_code)
            return True
        message = record.getMessage()
        if "token=" in message.lower():
            record.msg, record.args = _redact_token_query(message), None
        return True


logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=LOG_DATEFMT)
_timestamp_uvicorn_loggers()
_access_log_filter = _AccessLogFilter()
logging.getLogger("uvicorn.access").addFilter(_access_log_filter)


class _DropWhoamiRequestLines(logging.Filter):
    """Drop httpx's per-request INFO line for whoami calls.

    httpx logs every request at INFO ("HTTP Request: GET .../whoami-v2
    401"). Those calls are counted by ``hf_auth`` and summarised once a
    minute, so the per-call line is pure noise.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno > logging.INFO or "/api/whoami-v2" not in record.getMessage()


logging.getLogger("httpx").addFilter(_DropWhoamiRequestLines())
logger = logging.getLogger(__name__)


# --- Liveness ---------------------------------------------------------
#
# Split ownership:
#
# - **Sessions**: owned by the daemon (authoritative visibility into
#   the PC's data channel / ICE state). Central never ends a session on
#   idleness; it reacts to ``endSession`` / ``setPeerStatus``.
# - **Producer registrations**: owned by central. ``Peer.last_seen`` is
#   refreshed on every inbound application-level message (POST /send),
#   which includes the daemon's periodic ``setPeerStatus`` heartbeat.
#   The sweep below evicts producers silent for more than
#   ``PRODUCER_LEASE_SECONDS`` - the only way to catch half-open
#   sockets that ``request.is_disconnected()`` never reports.
#
# Legacy guard: daemons older than v1.7.2 connect to central but have
# no heartbeat loop (and no producer health loop to self-heal after an
# eviction). They are exempted from the sweep, keyed on the absence of
# ``meta.hardware_id`` - a field introduced by the same v1.7.2 release
# as the heartbeat, so its presence proves the daemon heartbeats.
# Exempted producers keep today's behaviour verbatim (including the
# ghost-on-power-cut bug the sweep fixes for modern daemons).
#
# Sizing: lease = 30 s with the heartbeat advertised at 10 s via the
# SSE ``welcome`` frame gives 2 missed heartbeats of headroom (daemons
# that predate welcome negotiation fall back to their internal 5 s
# default, which only adds margin). A live robot evicted during a
# >30 s network blackout self-heals in <=90 s: its producer health
# loop (poll 30 s, 2 misses) notices the missing registration and
# force-reconnects.
PRODUCER_LEASE_SECONDS = float(
    os.getenv("REACHY_CENTRAL_PRODUCER_LEASE_SECONDS", "30")
)
PRODUCER_SWEEP_INTERVAL_SECONDS = float(
    os.getenv("REACHY_CENTRAL_PRODUCER_SWEEP_INTERVAL", "5")
)
RECOMMENDED_HEARTBEAT_INTERVAL_SECONDS = 10.0

# SSE reconnect grace. Hugging Face's ingress periodically cuts SSE
# connections from the outside (whole ingress pools at once, roughly
# every 2 h in production); robots come back on the same token after
# their 5 s relay backoff, ~5-9 s later. Evicting on every cut made
# 50-150 robots vanish from listings and counters for those seconds.
#
# Instead, when a peer's CURRENT SSE stream closes it is only *detached*
# (``Peer.detached_at``): it stays registered, listed and counted, and
# messages addressed to it are queued. A reconnect on the same token
# within the grace rebinds to the same Peer (same peerId), clears the
# detach and flushes the queue after the usual welcome + list. If the
# grace runs out (``Peer.grace_deadline``), the sweeper evicts it
# through ``disconnect_peer`` exactly like an SSE close used to (end
# cause ``peer_disconnected``), so expiry lands within grace + one
# sweep interval.
#
# Sessions of a detaching peer (chosen from the clients' reconnect
# behaviour, see the README "Liveness" section): a detaching PRODUCER's
# session ends at detach, exactly as an SSE close ended it before (same
# broadcasts, same ``peer_disconnected`` end cause) - the daemon relay
# tears down its local WebRTC sessions and forgets their ids whenever
# its SSE drops, and never tells central, so keeping the session would
# only leave a phantom busy lock. A detaching CONSUMER's session
# survives the grace: its media is peer-to-peer and outlives the SSE
# channel, and the clients that do restart end their old session
# themselves.
#
# 0 disables the grace: an SSE close evicts immediately, as before. The
# few behaviours that are NOT grace-specific stay active at 0 (see the
# README): a consumer's startSession on the robot it already holds
# replaces its own session, a stream that dies during its welcome/list
# handshake is cleaned up, a peer's POSTed endSession purges stale
# endSession frames for that session from its own queue, and a POST
# racing an eviction no longer re-registers the evicted peer.
SSE_RECONNECT_GRACE_SECONDS = max(
    0.0, float(os.getenv("REACHY_CENTRAL_SSE_GRACE_SECONDS", "15"))
)

# Session ids a peer ended itself, remembered (per peer, bounded) so
# stale ``endSession`` frames for them can be dropped from its queue.
SELF_ENDED_SESSIONS_MAX = 16

# Detach / reattach / expiry are summarised in one INFO line per
# interval (only when something happened); per-peer lines are DEBUG.
SSE_SUMMARY_LOG_INTERVAL_SECONDS = 60.0


# Start time, two clocks on purpose:
#
# - ``STARTED_AT`` is wall clock and is only ever *displayed* (``/health``
#   ``started_at``, the status-page footer) so an operator can tell a
#   fresh redeploy from a quiet fleet. It is never compared to anything.
# - ``_STARTED_MONOTONIC`` drives ``uptime_seconds``. Like every other
#   duration in this module it uses ``time.monotonic()`` so an NTP step
#   cannot make uptime jump or go negative.
STARTED_AT = datetime.now(timezone.utc)
STARTED_AT_ISO = STARTED_AT.isoformat(timespec="seconds").replace("+00:00", "Z")
_STARTED_MONOTONIC = time.monotonic()


def _session_state_changed_payload(
    *,
    producer_id: str,
    busy: bool,
    active_app: Optional[str],
    meta: dict,
) -> dict:
    """Build the SSE message body for a busy/free transition.

    Centralised so ``handle_start_session`` and ``handle_end_session``
    emit the exact same shape; downstream listeners learn the schema
    once. The ``busy``/``activeApp`` keys mirror those returned by
    ``/api/robot-status`` and the ``get_producers_list`` rows so
    clients can share one decoder across all three paths.
    """
    return {
        "type": "sessionStateChanged",
        "peerId": producer_id,
        "busy": busy,
        "activeApp": active_app,
        "meta": meta,
    }


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Run the stale-producer sweeper (and fleet usage publisher) for the app's lifetime.

    On shutdown in-flight whoami calls are cancelled and the shared whoami
    HTTP client is closed (bounded by ``WHOAMI_SHUTDOWN_TIMEOUT_SECONDS``),
    then the publisher always gets one last, short attempt to publish its
    pending rows plus the partial current window and day.
    """
    tasks = [asyncio.create_task(signaling.run_producer_sweeper())]
    if usage_publisher is not None:
        tasks.append(asyncio.create_task(usage_publisher.run()))
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        try:
            await asyncio.wait_for(
                hf_validator.aclose(), timeout=WHOAMI_SHUTDOWN_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            logger.warning(
                "whoami client shutdown timed out after %.0fs; continuing",
                WHOAMI_SHUTDOWN_TIMEOUT_SECONDS,
            )
        except Exception:
            logger.exception("whoami client shutdown failed; continuing")
        if usage_publisher is not None:
            await usage_publisher.final_publish()


app = FastAPI(title="Reachy Mini Central", lifespan=_lifespan)

# Add CORS middleware for browser clients.
#
# allow_credentials=False is intentional: authentication here uses the
# Authorization header (Bearer <HF token>), not cookies. Combining
# allow_credentials=True with allow_origins=["*"] is a CORS spec
# violation (browsers reject credentialed wildcard-origin requests),
# and we don't need credentialed mode for header-based auth to work.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Token validation -------------------------------------------------
#
# Caches, single-flight, the unknown-token budget and the shared whoami
# client live in ``hf_auth`` (see its module docstring). One validator
# per process; the names below are what routes and tests use.
TOKEN_CACHE_TTL_SECONDS = hf_auth.TOKEN_CACHE_TTL_SECONDS
TOKEN_CACHE_STALE_GRACE_SECONDS = hf_auth.TOKEN_CACHE_STALE_GRACE_SECONDS
TokenValidationUnavailable = hf_auth.TokenValidationUnavailable
# Bounds the lifespan's whoami shutdown so fleet usage's final publish
# always gets its turn.
WHOAMI_SHUTDOWN_TIMEOUT_SECONDS = 5.0

hf_validator = hf_auth.TokenValidator(
    summary_extras=lambda: {"access_log_filtered": _access_log_filter.filtered}
)
token_cache = hf_validator.token_cache


async def validate_hf_token(token: str) -> Optional[str]:
    """Username for ``token``, None if HF rejected it (see ``hf_auth``).

    Raises ``TokenValidationUnavailable`` (503 + Retry-After) for a
    never-seen token when no verdict is available right now.
    """
    return await hf_validator.validate(token)


def _prune_token_cache() -> None:
    hf_validator.prune()


# Local-testing escape hatch: pre-seed the token cache from the
# environment so a second client can authenticate without a real HF
# token (mirrors prod topology where robot and phone hold DISTINCT
# tokens of the same user). Format: "token:username[,token:username]".
# Seeded entries never expire.
#
# Defence in depth: hard-disabled on HF Spaces (SPACE_ID is always set
# there) so an accidentally-configured secret can never become an auth
# bypass in a deployed environment.
if not os.environ.get("SPACE_ID"):
    for _seed in os.environ.get("DEV_TOKEN_SEED", "").split(","):
        if ":" in _seed:
            _tok, _user = _seed.split(":", 1)
            hf_validator.seed(_tok.strip(), _user.strip())
elif os.environ.get("DEV_TOKEN_SEED"):
    logger.warning(
        "DEV_TOKEN_SEED is set but ignored: refusing to seed the token "
        "cache on a deployed Space."
    )


# --- Rate limiting --------------------------------------------------
#
# Per-peer (token-keyed), sliding-window. Two intentional choices:
#
# 1. **Per-peer, not per-user.** A single HF account can run several
#    daemons (USB + Wi-Fi + extras). Keying the bucket on ``username``
#    makes them cannibalise each other, so adding a robot to the fleet
#    silently throttles the others. Hashing the token gives every peer
#    its own quota - the multi-tenant SaaS pattern of "composite key
#    per session" applied to robots.
#
# 2. **Sliding window via deque, not fixed 60 s window.** Fixed windows
#    rebound abruptly at the boundary (a peer that hit 100/100 at
#    t=59.9 s instantly gets 100 fresh tokens at t=60.0 s, encouraging
#    bursty clients). A deque of monotonic timestamps drops entries as
#    they age out, which is smoother and preserves the per-second
#    average regardless of clock alignment.
#
# Sizing: 1200 req / 60 s = 20 req/s sustained per peer, aligned with
# typical WebRTC signaling servers (CloudGaming reports 200 msg / 10 s
# per connection). With heartbeat at 10 s as advertised in the welcome
# frame (12 req/min for pre-negotiation daemons on their 5 s default),
# a typical mobile session (offer + answer + ~10 ICE candidates ~ 15
# req over a few seconds), and aggressive reconnects, observed peak
# under load is ~50-100 req/min/peer. We keep a 12-24x headroom so
# adding features (status polls, presence, etc.) does not require
# retuning the limit.
RATE_LIMIT_REQUESTS = 1200
RATE_LIMIT_WINDOW = 60.0
_rate_limit_buckets: dict[str, deque[float]] = {}


def _prune_rate_limit_buckets() -> None:
    """Drop buckets whose newest entry aged out of the window.

    ``check_rate_limit`` only trims buckets it is actively serving, so
    a departed peer's bucket would otherwise pin its timestamps in
    memory forever. Called by the background sweeper.
    """
    cutoff = time.monotonic() - RATE_LIMIT_WINDOW
    for key, bucket in list(_rate_limit_buckets.items()):
        if not bucket or bucket[-1] < cutoff:
            del _rate_limit_buckets[key]


def _rate_limit_key(token: str) -> str:
    """Return a stable, non-reversible per-peer bucket key.

    SHA-256 prefix avoids storing raw tokens in the bucket index. The
    16-hex-char prefix has 2^64 combinations - plenty for collision
    avoidance across simultaneously active peers.
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


def check_rate_limit(token: str) -> bool:
    """Sliding-window per-peer rate limit.

    Drops timestamps older than ``RATE_LIMIT_WINDOW``, then checks the
    current count. Returns ``True`` if the request is allowed and
    appends ``now`` to the bucket; returns ``False`` if the bucket is
    full (the request is then expected to translate to a 429 by the
    caller).
    """
    now = time.monotonic()
    key = _rate_limit_key(token)
    bucket = _rate_limit_buckets.setdefault(key, deque())

    cutoff = now - RATE_LIMIT_WINDOW
    while bucket and bucket[0] < cutoff:
        bucket.popleft()

    if len(bucket) >= RATE_LIMIT_REQUESTS:
        logger.warning(
            "Rate limit exceeded peer_key=%s count=%d window=%.0fs",
            key,
            len(bucket),
            RATE_LIMIT_WINDOW,
        )
        return False

    bucket.append(now)
    return True


# Set of peer-IP strings that have already triggered a query-string
# deprecation warning in this process. Sampled this way so a chatty
# legacy client reconnecting SSE every 30s doesn't flood logs with
# identical WARNINGs. Bounded by natural client cardinality; we also
# cap it to avoid unbounded growth from hostile callers.
_deprecation_warned_ips: set[str] = set()
_DEPRECATION_WARNED_MAX = 1024


def _warn_deprecated_query_once(request: Request) -> None:
    """Emit the ?token= deprecation warning at most once per client IP."""
    client_ip = request.client.host if request.client else "<unknown>"
    if client_ip in _deprecation_warned_ips:
        return
    if len(_deprecation_warned_ips) < _DEPRECATION_WARNED_MAX:
        _deprecation_warned_ips.add(client_ip)
    logger.warning(
        "[deprecation] HF token received via query string from %s. "
        "Switch to Authorization: Bearer <token> — the query form "
        "will be removed in a future release.",
        client_ip,
    )


async def _resolve_hf_token(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    token: str = Query(default=""),
) -> str:
    """FastAPI dependency: return the HF token from the Authorization header or ?token=.

    Accepts **only** ``Authorization: Bearer <token>``. The ``?token=``
    query parameter remains a transitional fallback for older clients
    that predate the header switch; each new client IP triggers one
    deprecation warning. The query fallback will be removed once all
    known clients (reachy_mini relay, reachy-mini.js, daemon
    /api/hf-auth proxy) ship the header form and deprecation logs go
    silent.

    Raises ``HTTPException(401, "Missing token")`` if neither form is
    present. Centralising the 401 here avoids copy-paste post-checks
    in each endpoint (where they would drift).

    Notes on the narrow Bearer-only parse:
    - No "bare string" fallback: returning an arbitrary ``Authorization``
      header value (e.g. ``Basic <b64>``) would still fail token
      validation, but a garbage value would land in ``token_cache`` as a
      cache miss, polluting the cache. There are no known clients that
      send a bare token, so we strictly reject anything that isn't
      RFC 6750-shaped.
    - Trim whitespace between scheme and token but require a non-empty
      token after the scheme — otherwise ``Authorization: Bearer`` with
      nothing after would slip through.
    """
    if authorization:
        # RFC 6750: "Bearer" + single space + non-empty b64token.
        # Case-insensitive scheme per RFC 7235 §2.1.
        scheme, _, value = authorization.partition(" ")
        if scheme.lower() == "bearer" and value.strip():
            return value.strip()
        # Unknown scheme (Basic, Digest, bare token, etc.) — refuse.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header must use Bearer scheme",
        )
    if token:
        _warn_deprecated_query_once(request)
        return token
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token"
    )


@dataclass
class Peer:
    """Represents a connected peer (robot or client).

    ``last_seen`` is refreshed on every inbound application-level
    message (``handle_message``, i.e. POST /send traffic - which
    includes the daemon's periodic heartbeat) and drives the producer
    sweep for heartbeat-capable daemons. It is also surfaced by
    /api/robot-status and /api/debug/peers.

    ``sse_generation`` counts SSE connections bound to this peer. A
    reconnect on the same token supersedes the previous generator:
    the old one compares its captured generation against this counter
    and exits without evicting the peer (see the /events endpoint).

    ``detached_at`` (monotonic, server clock) is set while the peer's
    SSE stream is closed but the reconnect grace has not run out yet
    (see ``SSE_RECONNECT_GRACE_SECONDS``); ``None`` while attached. A
    detached peer is still registered, listed and counted.

    ``grace_deadline`` (same clock) is when the sweeper evicts the peer
    unless an SSE stream of it starts first. Set at detach, and (re)armed
    whenever a new SSE connection is bound but has not started streaming
    yet; cleared when a stream starts. ``None`` while a stream is live.

    ``self_ended_sessions`` remembers (bounded) the session ids this peer
    ended itself, so stale ``endSession`` frames for them are never
    replayed to it after a reconnect.
    """
    peer_id: str
    username: str
    role: Optional[str] = None
    meta: dict = field(default_factory=dict)
    message_queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    connected: bool = True
    session_id: Optional[str] = None
    partner_id: Optional[str] = None
    last_seen: float = field(default_factory=time.monotonic)
    sse_generation: int = 0
    detached_at: Optional[float] = None
    grace_deadline: Optional[float] = None
    self_ended_sessions: deque = field(
        default_factory=lambda: deque(maxlen=SELF_ENDED_SESSIONS_MAX)
    )


class SignalingServer:
    """HTTP-based WebRTC signaling server.

    ``usage`` (optional) receives fleet usage events through ``_track``;
    tracking failures are logged and swallowed, never propagated into
    signalling.

    ``sse_grace_seconds`` defaults to ``SSE_RECONNECT_GRACE_SECONDS``
    read at construction time. ``clock`` (monotonic seconds) drives
    detach timestamps and grace deadlines only; it is injectable for
    tests.
    """

    def __init__(
        self,
        usage: Optional[UsageTracker] = None,
        *,
        sse_grace_seconds: Optional[float] = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.usage = usage
        self.sse_grace_seconds = max(
            0.0,
            SSE_RECONNECT_GRACE_SECONDS if sse_grace_seconds is None else float(sse_grace_seconds),
        )
        self.clock = clock
        # Since-boot SSE grace counters (``/health`` ``sse``), plus the
        # snapshot the per-minute summary line diffs against.
        self.sse_stats: dict[str, float] = {
            "detach_total": 0,
            "reattach_total": 0,
            "grace_expired_total": 0,
            "reattach_latency_s_max": 0.0,
            "sessions_ended_at_detach_total": 0,
            "consumer_session_replaced_total": 0,
        }
        self._sse_summary_at = clock()
        self._sse_summary_snapshot = dict(self.sse_stats)
        self._sse_summary_latency_max = 0.0
        self._usage_error_logged_at: dict[str, float] = {}
        self.peers: dict[str, Peer] = {}
        self.sessions: dict[str, tuple[str, str]] = {}  # session_id -> (producer_id, consumer_id)
        self.producers: dict[str, Peer] = {}  # producer_id -> Peer
        # Token -> peer_id mapping. Lets a daemon recover its peer_id
        # across an SSE reconnect without losing its producer slot, but
        # is purged on disconnect so a hard-evicted peer doesn't come
        # back with the same id.
        self.token_to_peer: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Fleet usage hook
    # ------------------------------------------------------------------

    def _track(self, event: str, *args) -> None:
        """Forward ``event`` to ``self.usage``; never raises.

        A failing hook logs its traceback at most once per
        ``USAGE_HOOK_ERROR_LOG_INTERVAL_SECONDS`` per event name.
        """
        if self.usage is None:
            return
        try:
            getattr(self.usage, event)(*args)
        except Exception:
            now = time.monotonic()
            last = self._usage_error_logged_at.get(event)
            if last is None or now - last >= USAGE_HOOK_ERROR_LOG_INTERVAL_SECONDS:
                self._usage_error_logged_at[event] = now
                logger.exception("Fleet usage hook %s failed (signalling unaffected)", event)

    # ------------------------------------------------------------------
    # Peer lifecycle
    # ------------------------------------------------------------------

    def get_or_create_peer(self, token: str, username: str) -> Peer:
        """Get existing peer or create new one."""
        # Check if this token already has a peer
        if token in self.token_to_peer:
            peer_id = self.token_to_peer[token]
            if peer_id in self.peers:
                peer = self.peers[peer_id]
                peer.connected = True
                peer.last_seen = time.monotonic()
                # A reattach within the SSE grace is routine (summarised
                # once a minute); only a takeover of a live connection
                # is worth an INFO line.
                logger.log(
                    logging.DEBUG if peer.detached_at is not None else logging.INFO,
                    "Peer reconnected: %s",
                    peer_id,
                )
                return peer

        # Create new peer
        peer_id = str(uuid.uuid4())
        peer = Peer(peer_id=peer_id, username=username)
        self.peers[peer_id] = peer
        self.token_to_peer[token] = peer_id
        logger.info(f"New peer created: {peer_id} for user {username}")
        return peer

    # ------------------------------------------------------------------
    # SSE attach / detach (reconnect grace)
    # ------------------------------------------------------------------

    @staticmethod
    def _purge_queued(peer: Peer, session_id: str, types: Optional[tuple] = None) -> int:
        """Drop queued frames for ``session_id`` (optionally only ``types``) from ``peer``'s queue.

        Synchronous (no await between drain and refill), order preserved.
        Returns how many frames were dropped.
        """
        kept, dropped = [], 0
        queue = peer.message_queue
        while True:
            try:
                msg = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if (
                isinstance(msg, dict)
                and msg.get("sessionId") == session_id
                and (types is None or msg.get("type") in types)
            ):
                dropped += 1
            else:
                kept.append(msg)
        for msg in kept:
            queue.put_nowait(msg)
        return dropped

    def remember_self_ended_session(self, peer: Peer, session_id: Optional[str]) -> None:
        """``peer`` ended (or replaced) ``session_id`` itself: never replay an endSession for it."""
        if not isinstance(session_id, str) or not session_id:
            return
        if session_id not in peer.self_ended_sessions:
            peer.self_ended_sessions.append(session_id)
        self._purge_queued(peer, session_id, ("endSession",))

    def attach_sse(self, peer: Peer) -> tuple[int, asyncio.Queue]:
        """Bind a new SSE connection to ``peer``; return ``(generation, queue)``.

        Called by ``/events`` (through ``connect_sse``) before it returns
        the stream. Bumps ``sse_generation`` (superseding any older
        generator) and gives the connection a fresh queue. With the grace
        enabled, any message still sitting in the previous queue -
        typically queued while the peer was detached - is carried over in
        order, so it is delivered right after the new connection's
        welcome + list; ``endSession`` frames for sessions that are over
        and that the peer ended itself are dropped on the way. With the
        grace disabled the old queue is dropped (previous behaviour). The
        carry-over is synchronous: a superseded generator still awaiting
        the old queue can not steal an item from it.

        With the grace enabled this also (re)arms ``grace_deadline`` to
        now + grace: a detached peer stays detached until the new stream
        actually starts (``sse_stream_started``), and an attached peer
        whose stream is superseded by one that never starts is not
        stranded - either way the sweeper evicts it when the deadline
        passes. Re-arming on reattach means a reconnect in progress at the
        very end of the grace is not expired before its first step.
        """
        peer.sse_generation += 1
        old_queue = peer.message_queue
        queue: asyncio.Queue = asyncio.Queue()
        if self.sse_grace_seconds > 0:
            while True:
                try:
                    msg = old_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if (
                    isinstance(msg, dict)
                    and msg.get("type") == "endSession"
                    and msg.get("sessionId") in peer.self_ended_sessions
                    and msg.get("sessionId") not in self.sessions
                ):
                    continue
                queue.put_nowait(msg)
            peer.grace_deadline = self.clock() + self.sse_grace_seconds
        peer.message_queue = queue
        return peer.sse_generation, queue

    async def connect_sse(self, peer: Peer) -> tuple[int, asyncio.Queue]:
        """``attach_sse`` plus the takeover rule, for ``/events``.

        When a new stream supersedes a producer's stream that is still
        attached (its old socket half-open behind the proxy), the daemon
        relay that reconnects has already dropped its local sessions, so
        with the grace enabled its session ends here exactly as at
        detach (see ``_end_session_at_detach``).
        """
        superseded_attached = peer.sse_generation > 0 and peer.detached_at is None
        generation, queue = self.attach_sse(peer)
        if (
            superseded_attached
            and self.sse_grace_seconds > 0
            and self._is_producer_side_of_session(peer)
        ):
            await self._end_session_at_detach(peer)
        return generation, queue

    def sse_stream_started(self, peer: Peer) -> None:
        """The peer's current SSE stream started: reattach it if detached.

        Clears the pending grace deadline. Same Peer, same peerId; for a
        detached peer the reattach latency is recorded.
        """
        peer.grace_deadline = None
        if peer.detached_at is None:
            return
        latency = max(0.0, self.clock() - peer.detached_at)
        peer.detached_at = None
        self.sse_stats["reattach_total"] += 1
        if latency > self.sse_stats["reattach_latency_s_max"]:
            self.sse_stats["reattach_latency_s_max"] = latency
        if latency > self._sse_summary_latency_max:
            self._sse_summary_latency_max = latency
        logger.debug("SSE reattach: %s after %.1fs", peer.peer_id, latency)

    def _is_producer_side_of_session(self, peer: Peer) -> bool:
        producer_id, _consumer_id = self.sessions.get(peer.session_id, (None, None))
        return producer_id is not None and producer_id == peer.peer_id

    async def _end_session_at_detach(self, peer: Peer) -> None:
        """End a producer's session because its SSE stream went away.

        Same outcome as an SSE close before the grace existed (end cause
        ``peer_disconnected``, endSession to the consumer, busy=false
        broadcast), except that nothing about the session is left queued
        for the producer: its relay already dropped the session, and a
        replayed startSession/endSession for it could only confuse it.
        """
        session_id = peer.session_id
        self._purge_queued(peer, session_id)
        self.sse_stats["sessions_ended_at_detach_total"] += 1
        await self.handle_end_session(
            session_id,
            end_cause=SESSION_END_PEER_DISCONNECTED,
            skip_notify=peer.peer_id,
        )

    async def detach_peer(self, peer_id: str) -> None:
        """The peer's current SSE stream closed: start its reconnect grace.

        With the grace disabled (``sse_grace_seconds == 0``) this is the
        previous behaviour: immediate ``disconnect_peer`` with end cause
        ``peer_disconnected``.

        Otherwise the peer stays in ``peers`` / ``producers`` /
        ``token_to_peer`` (still listed and counted, no removal
        broadcast, no fleet usage event) until ``grace_deadline``. If it
        is the producer side of a session, that session ends now (see
        ``_end_session_at_detach``); a consumer's session survives the
        grace.
        """
        peer = self.peers.get(peer_id)
        if peer is None:
            return
        if self.sse_grace_seconds <= 0:
            await self.disconnect_peer(peer_id, end_cause=SESSION_END_PEER_DISCONNECTED)
            return
        if peer.detached_at is not None:
            return
        now = self.clock()
        peer.detached_at = now
        peer.grace_deadline = now + self.sse_grace_seconds
        self.sse_stats["detach_total"] += 1
        logger.debug("SSE detach: %s (grace %.0fs)", peer_id, self.sse_grace_seconds)
        if self._is_producer_side_of_session(peer):
            await self._end_session_at_detach(peer)

    async def expire_detached_peers(self) -> list[str]:
        """Evict peers whose grace deadline passed; return their ids.

        Covers detached peers and peers whose newest SSE connection never
        started streaming. Runs from the sweeper before the stale-producer
        sweep (which skips detached peers), so a detached peer is only
        ever evicted here - or earlier by a stable-id collision that
        removes it outright - and never processed twice. Eviction is the
        regular ``disconnect_peer`` with end cause ``peer_disconnected``:
        the same broadcasts, fleet usage events and cleanup an SSE close
        produced before the grace existed. Each peer is handled in its
        own try/except, and its deadline is cleared (and the expiry
        counted) before ``disconnect_peer`` runs, so a failure can
        neither skip the other peers nor count one twice.
        """
        if self.sse_grace_seconds <= 0:
            return []
        now = self.clock()
        expired = [
            pid
            for pid, p in self.peers.items()
            if p.grace_deadline is not None and now >= p.grace_deadline
        ]
        for pid in expired:
            peer = self.peers.get(pid)
            if peer is None or peer.grace_deadline is None:
                continue
            peer.grace_deadline = None
            self.sse_stats["grace_expired_total"] += 1
            logger.debug("SSE grace expired: %s", pid)
            try:
                await self.disconnect_peer(pid, end_cause=SESSION_END_PEER_DISCONNECTED)
            except Exception:
                logger.exception("SSE grace expiry failed for one peer; continuing")
        return expired

    def count_detached_peers(self) -> int:
        return sum(1 for p in self.peers.values() if p.detached_at is not None)

    def sse_health(self) -> dict:
        """Aggregate SSE grace state for ``/health`` (no identifiers)."""
        return {
            "grace_seconds": self.sse_grace_seconds,
            "detached_now": self.count_detached_peers(),
            "detach_total": int(self.sse_stats["detach_total"]),
            "reattach_total": int(self.sse_stats["reattach_total"]),
            "grace_expired_total": int(self.sse_stats["grace_expired_total"]),
            "reattach_latency_s_max": round(self.sse_stats["reattach_latency_s_max"], 2),
            "sessions_ended_at_detach_total": int(self.sse_stats["sessions_ended_at_detach_total"]),
            "consumer_session_replaced_total": int(self.sse_stats["consumer_session_replaced_total"]),
        }

    def maybe_log_sse_summary(self, now: Optional[float] = None) -> bool:
        """Once per ``SSE_SUMMARY_LOG_INTERVAL_SECONDS``, one INFO line of deltas.

        Silent when nothing was detached, reattached or expired in the
        interval and nobody is detached right now. Returns whether a line
        was logged. Aggregate counts only.
        """
        if now is None:
            now = self.clock()
        if now - self._sse_summary_at < SSE_SUMMARY_LOG_INTERVAL_SECONDS:
            return False
        keys = ("detach_total", "reattach_total", "grace_expired_total")
        delta = {k: int(self.sse_stats[k] - self._sse_summary_snapshot.get(k, 0)) for k in keys}
        window = now - self._sse_summary_at
        latency_max = self._sse_summary_latency_max
        detached_now = self.count_detached_peers()
        self._sse_summary_at = now
        self._sse_summary_snapshot = dict(self.sse_stats)
        self._sse_summary_latency_max = 0.0
        if not (any(delta.values()) or detached_now):
            return False
        logger.info(
            "SSE summary (last %.0fs): detached=%d reattached=%d grace_expired=%d "
            "detached_now=%d reattach_latency_max=%.1fs",
            window,
            delta["detach_total"],
            delta["reattach_total"],
            delta["grace_expired_total"],
            detached_now,
            latency_max,
        )
        return True

    async def send_to_peer(self, peer_id: str, message: dict):
        """Queue a message for a peer."""
        if peer_id in self.peers:
            await self.peers[peer_id].message_queue.put(message)

    async def broadcast_to_listeners(self, message: dict, exclude_id: str = None, owner_username: str = None):
        """Send message to connected peers with same owner, except the sender."""
        for peer_id, peer in self.peers.items():
            if peer_id != exclude_id and peer.connected:
                # If owner specified, only send to peers with same username
                if owner_username and peer.username != owner_username:
                    continue
                await peer.message_queue.put(message)

    async def handle_set_peer_status(self, peer: Peer, message: dict) -> Optional[dict]:
        """Handle peer status update (producer/listener registration).

        Three cases:

        - ``roles=["producer"]``: register / refresh as producer. If the
          payload's stable identity (``meta.install_id`` or
          ``meta.hardware_id``) collides with an existing producer
          of the same user, evict that older producer first
          (last-writer-wins, see ``_evict_stable_id_collisions``). Broadcast a
          ``peerStatusChanged`` event so listeners learn about the new
          producer.

          This path also doubles as the daemon's "I'm available again"
          signal: after its watchdog tears down an idle session
          locally, the daemon re-emits ``setPeerStatus(producer)`` with
          the same ``install_id``. ``peer.session_id`` was already
          cleared (either by the daemon sending ``endSession`` or by
          the install_id collision path below), so this simply
          refreshes ``producers`` and lets same-user listeners learn
          the robot is free again.

        - ``roles=["listener"]``: mark as listener, no broadcast.

        - ``roles=[]``: explicit withdraw. The peer wants to be removed
          from ``producers`` but keep its SSE channel open so it can
          re-register later. We end any active session it had, drop it
          from ``producers``, and tell other listeners (same user) so
          their UI clears the row immediately.
        """
        roles = message.get("roles", [])
        meta = message.get("meta", {})
        if not isinstance(meta, dict):
            # Every downstream reader (stable-id eviction, the sweep,
            # /api/* views, robot_kind_of) does ``meta.get(...)``; a
            # non-object meta would either crash the sweeper loop or
            # poison the owner's listeners. Reject it before it can
            # replace ``peer.meta``. Same HTTP-layer 400 pattern
            # ``send_message`` uses for malformed requests.
            logger.warning(
                "Rejected setPeerStatus from %s: meta must be an object, got %s",
                peer.peer_id,
                type(meta).__name__,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="meta must be a JSON object",
            )
        was_producer = self.producers.get(peer.peer_id) is peer
        prev_meta = peer.meta
        peer.meta = meta

        if "producer" in roles:
            await self._evict_stable_id_collisions(peer, meta)
            peer.role = "producer"
            self.producers[peer.peer_id] = peer
            self._track("producer_seen", peer.peer_id, peer.username, meta)  # usage
            # Daemons re-send this every heartbeat (10 s): only a new
            # registration or a meta change is worth an INFO line.
            logger.log(
                logging.INFO if not was_producer or prev_meta != meta else logging.DEBUG,
                "Producer registered: %s with meta: %s",
                peer.peer_id,
                repr(meta)[:300],
            )
            return {
                "type": "peerStatusChanged",
                "peerId": peer.peer_id,
                "roles": ["producer"],
                "meta": meta,
            }
        elif "listener" in roles:
            peer.role = "listener"
            logger.info(f"Listener registered: {peer.peer_id}")
            return None
        else:
            # Explicit withdraw: roles=[]
            return await self._withdraw_peer(peer, meta)

    async def _withdraw_peer(self, peer: Peer, meta: dict) -> Optional[dict]:
        """Remove a peer from the producer list at its own request.

        Symmetric to a producer registration: clear ``producers``,
        end the active session if any, broadcast a
        ``peerStatusChanged(roles=[])`` so other listeners (same user)
        update their UI without waiting for a TTL.

        We deliberately keep the peer in ``self.peers`` and keep its
        SSE channel open, so the daemon can ``setPeerStatus(producer)``
        again later if the underlying issue clears (USB replugged,
        backend recovered).
        """
        was_producer = peer.peer_id in self.producers
        if peer.session_id is not None:
            await self.handle_end_session(
                peer.session_id,
                reason="producer_withdrew",
                end_cause=SESSION_END_WITHDRAWN,
            )
        if was_producer:
            del self.producers[peer.peer_id]
            self._track("producer_gone", peer.peer_id)  # usage
            logger.info(
                "Producer withdrew: %s (meta=%s)", peer.peer_id, meta
            )
        peer.role = None
        if was_producer:
            return {
                "type": "peerStatusChanged",
                "peerId": peer.peer_id,
                "roles": [],
                "meta": meta,
            }
        return None

    async def _evict_stable_id_collisions(self, new_peer: Peer, new_meta: dict) -> None:
        """Last-writer-wins on stable-identity collisions.

        A re-flashed daemon, a duplicated SD card, a stale tray
        process, or a robot re-provisioned with a fresh token can
        register a producer that is the same physical robot as an
        already-registered producer of the same user. Without eviction
        we'd carry both forever and the mobile app would see the robot
        twice. Policy: keep the newcomer, drop the older.

        Identity keys, checked independently:

        - ``install_id``: reserved per-install key (not emitted by any
          shipped daemon yet, kept for forward compatibility).
        - ``hardware_id``: SHA-256 prefix of the Pollen audio device's
          USB serial, emitted by daemons >= v1.7.2 - stable per
          physical robot across reinstalls and renames. This is the
          key that actually fires in production today.

        Owner-scoped: a different HF user holding the same id
        (possible during a hardware swap between two accounts) is left
        alone here - cross-tenant collisions are out of scope for the
        signaling server, the auth layer above us guarantees isolation.
        """
        new_ids = {
            key: new_meta.get(key)
            for key in ("install_id", "hardware_id")
            if new_meta.get(key)
        }
        if not new_ids:
            return

        for old_id, old_peer in list(self.producers.items()):
            if old_id == new_peer.peer_id:
                continue
            if old_peer.username != new_peer.username:
                continue
            matched_key = next(
                (k for k, v in new_ids.items() if old_peer.meta.get(k) == v),
                None,
            )
            if matched_key is None:
                continue
            logger.info(
                "%s collision: %s already held by peer %s, evicting older",
                matched_key,
                new_ids[matched_key],
                old_id,
            )
            if old_peer.session_id is not None:
                await self.handle_end_session(
                    old_peer.session_id,
                    reason="install_id_takeover",
                    end_cause=SESSION_END_REPLACED,
                )
            await self.disconnect_peer(old_id, end_cause=SESSION_END_REPLACED)

    def producer_session_snapshot(
        self, producer: Peer
    ) -> tuple[bool, Optional[str]]:
        """Snapshot ``(busy, activeApp)`` for one producer.

        Single source of truth for "is this robot in use, and by
        whom". Used by the SSE ``list`` frame (``get_producers_list``),
        the ``/api/robot-status`` REST view, and the busy/free
        broadcasts emitted on session start/end. Keeping the
        extraction in one place means any future tweak (a different
        fallback for ``activeApp``, a new field like
        ``sessionStartedAt``, …) only needs to land here.

        ``activeApp`` is the consumer's ``meta.name`` when the
        partner is still in ``self.peers``; ``None`` when the
        producer is free, when the consumer disconnected mid-tear
        down, or when the consumer never advertised a name.
        """
        if not producer.session_id:
            return False, None
        active_app: Optional[str] = None
        if producer.partner_id and producer.partner_id in self.peers:
            active_app = self.peers[producer.partner_id].meta.get("name")
        return True, active_app

    def get_producers_list(self, username: str) -> list:
        """Get list of producers owned by the given user.

        Mirrors the shape of ``/api/robot-status`` so a listener that
        connects mid-session sees the correct ``busy``/``activeApp``
        on its initial ``list`` SSE frame, without needing a follow-up
        REST call. Subsequent transitions are pushed via
        ``sessionStateChanged`` (see ``handle_start_session`` and
        ``handle_end_session``).
        """
        out: list[dict] = []
        for p in self.producers.values():
            if not (p.connected and p.username == username):
                continue
            busy, active_app = self.producer_session_snapshot(p)
            out.append(
                {
                    "id": p.peer_id,
                    "meta": p.meta,
                    "busy": busy,
                    "activeApp": active_app,
                }
            )
        return out

    def count_connected_peers(self) -> int:
        """Number of peers (any role) whose SSE channel is currently up."""
        return sum(1 for p in self.peers.values() if p.connected)

    def count_connected_producers(self) -> int:
        """Number of registered producers whose SSE channel is currently up.

        This is the public definition of "active producer" (``/`` and
        ``/health``) and matches what ``get_producers_list`` shows owners.
        """
        return sum(1 for p in self.producers.values() if p.connected)

    def count_connected_producers_by_kind(self) -> dict[str, int]:
        """Connected producers bucketed by ``robot_kind_of(meta)``.

        Every key in ``PUBLIC_ROBOT_KINDS`` is always present, in that
        order, zero-filled, so ``/health`` consumers get a stable schema
        regardless of what is currently online.
        """
        counts = {kind: 0 for kind in PUBLIC_ROBOT_KINDS}
        for p in self.producers.values():
            if p.connected:
                counts[robot_kind_of(p.meta)] += 1
        return counts

    async def handle_start_session(self, peer: Peer, message: dict) -> dict:
        """Handle session start request."""
        producer_id = message.get("peerId")

        if producer_id not in self.producers:
            return {"type": "error", "details": f"Producer {producer_id} not found"}

        producer = self.producers[producer_id]

        # Security: verify the user owns this producer
        if producer.username != peer.username:
            logger.warning(f"User {peer.username} tried to access producer owned by {producer.username}")
            return {"type": "error", "details": "Access denied: you don't own this robot"}

        # A consumer re-starting a session on the robot it already holds
        # (it reconnected within the SSE grace and redials, or its
        # fire-and-forget endSession lost the race against the new
        # startSession) replaces its own old session instead of being
        # rejected as robot_busy by it. Active whatever the grace
        # setting. The producer gets the endSession; the consumer does
        # not, and any endSession for the old session still queued for it
        # is dropped (it abandoned that session, and an endSession
        # arriving mid-setup would abort the new one). Any OTHER consumer
        # still gets robot_busy below.
        if (
            producer.session_id is not None
            and producer.partner_id == peer.peer_id
            and peer.session_id == producer.session_id
        ):
            old_session_id = producer.session_id
            logger.info(
                "Session %s replaced by a new startSession from its consumer %s",
                old_session_id,
                peer.peer_id,
            )
            self.remember_self_ended_session(peer, old_session_id)
            self.sse_stats["consumer_session_replaced_total"] += 1
            await self.handle_end_session(
                old_session_id,
                reason="session_replaced",
                end_cause=SESSION_END_CONSUMER_REPLACED,
                skip_notify=peer.peer_id,
            )

        # Concurrency gate: reject if producer already has an active session.
        # The existing consumer's app name is read from its meta (set via setPeerStatus).
        if producer.session_id is not None:
            active_app = "another app"
            if producer.partner_id and producer.partner_id in self.peers:
                active_app = self.peers[producer.partner_id].meta.get("name") or active_app
            logger.info(
                f"Rejected session: producer {producer_id} busy with '{active_app}' "
                f"(requested by {peer.peer_id}, app={peer.meta.get('name')!r})"
            )
            return {
                "type": "sessionRejected",
                "reason": "robot_busy",
                "peerId": producer_id,
                "activeApp": active_app,
            }

        session_id = str(uuid.uuid4())

        # Store session
        self.sessions[session_id] = (producer_id, peer.peer_id)
        self._track("session_started", session_id, producer.meta)  # usage
        peer.session_id = session_id
        peer.partner_id = producer_id
        peer.role = "consumer"
        producer.session_id = session_id
        producer.partner_id = peer.peer_id

        # Notify producer
        await self.send_to_peer(producer_id, {
            "type": "startSession",
            "peerId": peer.peer_id,
            "sessionId": session_id
        })

        # Push busy transition to the user's other devices (mobile
        # + desktop on the same HF account) so they flip their
        # on-screen "free" affordance to "busy" within the round
        # trip rather than waiting on the 30 s ``/api/robot-status``
        # poll. We deliberately exclude the consumer that just
        # acquired the slot - it already knows via ``sessionStarted``
        # and echoing would make UIs react twice to their own action.
        await self.broadcast_to_listeners(
            _session_state_changed_payload(
                producer_id=producer_id,
                busy=True,
                active_app=peer.meta.get("name"),
                meta=producer.meta,
            ),
            exclude_id=peer.peer_id,
            owner_username=producer.username,
        )

        logger.info(f"Session started: {session_id}")
        return {"type": "sessionStarted", "peerId": producer_id, "sessionId": session_id}

    async def handle_peer_message(self, peer: Peer, message: dict):
        """Relay SDP/ICE messages between peers."""
        session_id = message.get("sessionId")

        if session_id not in self.sessions:
            logger.warning(f"Unknown session: {session_id}")
            return

        producer_id, consumer_id = self.sessions[session_id]
        target_id = consumer_id if peer.peer_id == producer_id else producer_id

        # Relay the message
        relay_message = {
            "type": "peer",
            "sessionId": session_id,
            **{k: v for k, v in message.items() if k not in ["type", "sessionId"]}
        }
        await self.send_to_peer(target_id, relay_message)
        logger.debug(f"Relayed peer message from {peer.peer_id} to {target_id}")

    async def handle_end_session(
        self,
        session_id: str,
        reason: Optional[str] = None,
        *,
        end_cause: str = SESSION_END_OTHER,
        skip_notify: Optional[str] = None,
    ):
        """End a session and notify both peers.

        The optional ``reason`` is propagated to both peers so clients can
        distinguish a user-initiated stop ("Session stopped") from a
        server-side eviction (e.g. ``robot_busy_local_app`` when the robot
        relay refuses because a local Python app holds the daemon lock).
        Without forwarding the reason, clients see only an unexplained
        endSession and have no way to surface a meaningful message.

        ``end_cause`` is the server-side category (``SESSION_END_*``) of
        the code path ending the session, recorded by fleet usage. It is
        independent of ``reason``, which is client-controlled.

        ``skip_notify`` names a peer whose session state is cleared but
        who is NOT sent the ``endSession``: a detaching peer (see
        ``detach_peer``) or a consumer replacing its own session (see
        ``handle_start_session``) already knows.
        """
        if session_id not in self.sessions:
            return

        producer_id, consumer_id = self.sessions[session_id]
        # Snapshot the producer BEFORE we clear the session, so the
        # post-cleanup broadcast still carries its meta even when
        # the producer has already been removed from ``producers``
        # (e.g. ``disconnect_peer`` calls us right before the del).
        producer = self.peers.get(producer_id)

        msg: dict = {"type": "endSession", "sessionId": session_id}
        if reason is not None:
            msg["reason"] = reason

        for peer_id in [producer_id, consumer_id]:
            if peer_id in self.peers:
                peer = self.peers[peer_id]
                peer.session_id = None
                peer.partner_id = None
                if peer_id != skip_notify:
                    await self.send_to_peer(peer_id, msg)

        del self.sessions[session_id]
        self._track("session_ended", session_id, end_cause)  # usage

        # Push free transition to the user's other devices, mirror
        # of the start-session broadcast. Owner is resolved from
        # whichever side of the session is still in ``peers`` so a
        # producer-side disconnect (which calls into us from
        # ``disconnect_peer``) still reaches the listeners. If
        # *both* sides are gone we skip the emit rather than fall
        # back to a global broadcast - a missing ``owner_username``
        # would leak the event to every connected listener
        # regardless of HF user.
        consumer = self.peers.get(consumer_id)
        owner_username: Optional[str] = (
            producer.username
            if producer is not None
            else consumer.username
            if consumer is not None
            else None
        )
        if owner_username is not None:
            await self.broadcast_to_listeners(
                _session_state_changed_payload(
                    producer_id=producer_id,
                    busy=False,
                    active_app=None,
                    meta=producer.meta if producer is not None else {},
                ),
                owner_username=owner_username,
            )

        logger.info(f"Session ended: {session_id} (reason={reason!r})")

    async def handle_message(self, peer: Peer, message: dict) -> Optional[dict]:
        """Process incoming message and return response if any."""
        # Inbound application-level traffic is the liveness signal the
        # producer sweep keys on: only a peer whose client half is
        # alive can POST /send (a half-open socket can't).
        peer.last_seen = time.monotonic()

        msg_type = message.get("type", "")
        logger.debug(f"Received from {peer.peer_id}: {msg_type}")

        if msg_type == "setPeerStatus":
            broadcast = await self.handle_set_peer_status(peer, message)
            if broadcast:
                # Only notify users with same username (owner)
                await self.broadcast_to_listeners(broadcast, exclude_id=peer.peer_id, owner_username=peer.username)
            return None

        elif msg_type == "list":
            return {"type": "list", "producers": self.get_producers_list(peer.username)}

        elif msg_type == "startSession":
            return await self.handle_start_session(peer, message)

        elif msg_type == "peer":
            await self.handle_peer_message(peer, message)
            return None

        elif msg_type == "endSession":
            # The sender ended this session itself: drop any stale
            # endSession for it already queued for the sender (e.g.
            # queued while it was detached), and never replay one for it
            # after a reconnect (see ``attach_sse``).
            self.remember_self_ended_session(peer, message.get("sessionId"))
            await self.handle_end_session(
                message.get("sessionId"),
                reason=message.get("reason"),
                end_cause=SESSION_END_ENDED,
            )
            return None

        else:
            logger.warning(f"Unknown message type: {msg_type}")
            return None

    async def sweep_stale_producers(self) -> list[str]:
        """Evict producers with no inbound traffic for a full lease.

        Heartbeat-capable daemons (>= v1.7.2, detected via
        ``meta.hardware_id`` - shipped by the same release as the
        heartbeat loop) refresh ``last_seen`` every few seconds
        through their ``setPeerStatus`` re-emissions on POST /send. A
        producer silent for more than ``PRODUCER_LEASE_SECONDS`` is
        therefore a half-open socket (power cut, yanked Wi-Fi), not a
        healthy robot: evict it fully so it stops showing up as
        connectable in pickers.

        Producers without ``hardware_id`` (legacy daemons that never
        heartbeat, or daemons running without a robot attached) are
        exempt - evicting them would be permanent since they have no
        health loop to re-register.

        Full ``disconnect_peer`` rather than a soft withdraw: a swept
        peer is by definition unreachable, keeping its Peer object and
        token mapping around would only leak memory and rebind a
        returning daemon onto a dead message queue. If the robot was
        in fact alive (>30 s network blackout), its producer health
        loop notices the missing registration within two 30 s polls
        and force-reconnects - the exact recovery path daemons already
        exercise on every central redeploy.

        Detached producers (SSE closed, reconnect grace running) are
        skipped: the grace expiry (``expire_detached_peers``, run just
        before this sweep) owns their eviction, so no peer is processed
        by both paths. A reattach refreshes ``last_seen``.

        Returns the list of evicted peer ids (handy for tests/logs).
        """
        now = time.monotonic()
        stale = [
            pid
            for pid, p in self.producers.items()
            if p.meta.get("hardware_id")
            and not (p.detached_at is not None and self.peers.get(pid) is p)
            and now - p.last_seen > PRODUCER_LEASE_SECONDS
        ]
        for pid in stale:
            peer = self.peers.get(pid) or self.producers.get(pid)
            logger.warning(
                "Sweeping stale producer %s (name=%r, silent for %.0fs)",
                pid,
                peer.meta.get("name") if peer else None,
                now - peer.last_seen if peer else -1,
            )
            await self.disconnect_peer(pid, end_cause=SESSION_END_SWEPT)
        return stale

    async def run_producer_sweeper(self) -> None:
        """Background task: periodic grace expiry, stale-producer sweep, pruning.

        Order per tick: expire detached peers whose SSE reconnect grace
        ran out, then sweep stale (silent) producers, which skips detached
        peers. Also emits the once-per-minute auth and SSE summary lines.

        Also rolls the fleet usage window over, so windows close on time
        even when no signalling event arrives.
        """
        while True:
            await asyncio.sleep(PRODUCER_SWEEP_INTERVAL_SECONDS)
            await self.run_sweeper_tick()

    async def run_sweeper_tick(self) -> None:
        """One sweeper iteration. Each step runs in its own try/except so a
        failure in one (logged with its traceback) never skips the others."""
        for name, step in (
            ("grace expiry", self.expire_detached_peers),
            ("stale-producer sweep", self.sweep_stale_producers),
        ):
            try:
                await step()
            except Exception:
                logger.exception("Sweeper step failed: %s", name)
        for name, fn in (
            ("rate-limit prune", _prune_rate_limit_buckets),
            ("token cache prune", lambda: hf_validator.prune()),
            ("auth summary", lambda: hf_validator.maybe_log_summary()),
            ("SSE summary", self.maybe_log_sse_summary),
        ):
            try:
                fn()
            except Exception:
                logger.exception("Sweeper step failed: %s", name)
        self._track("maybe_roll")  # usage

    async def disconnect_peer(
        self, peer_id: str, *, end_cause: str = SESSION_END_OTHER
    ):
        """Fully evict a peer from every server-side structure.

        Called from:

        - SSE close path, via ``detach_peer``: immediately when the
          reconnect grace is disabled, otherwise from
          ``expire_detached_peers`` once the grace ran out.
        - ``_evict_stable_id_collisions``, when a duplicate registers.
        - ``sweep_stale_producers``, when a heartbeat-capable producer
          went silent for a full lease (half-open socket).

        Cleanup is exhaustive on purpose: ``peers``, ``producers``,
        ``token_to_peer`` and the active session are all cleared.
        Without that, a peer left as ``connected=False`` would
        accumulate forever in ``peers`` (memory leak) and a daemon
        whose token re-binds to its old, now-zombie ``peer_id`` would
        come back wired into a dead message_queue.

        Any consumer waiting on its message_queue is signaled by the
        ``endSession`` broadcast in ``handle_end_session``; the
        message_queue itself is GC'd alongside the Peer object.

        ``end_cause`` categorises the ended session (if any) for fleet
        usage: each caller passes the one matching its path.
        """
        if peer_id not in self.peers:
            # Defensive: a producers entry must never outlive its peer
            # (e.g. re-inserted by a request that raced an eviction).
            if self.producers.pop(peer_id, None) is not None:
                self._track("producer_gone", peer_id)  # usage
            return

        peer = self.peers[peer_id]
        peer.connected = False
        peer.grace_deadline = None
        peer.detached_at = None

        # End any session this peer is part of (either as producer or consumer).
        # handle_end_session clears session_id/partner_id on both sides and
        # notifies the remaining peer so it can tear down its WebRTC state.
        if peer.session_id is not None:
            await self.handle_end_session(peer.session_id, end_cause=end_cause)

        # If we were a producer, tell same-user listeners now so their
        # UI updates without waiting for a fresh /list.
        was_producer = peer_id in self.producers
        if was_producer:
            del self.producers[peer_id]
            self._track("producer_gone", peer_id)  # usage

        # Drop any token mapping pointing at this peer. A reconnect on
        # the same token will mint a fresh peer_id.
        for tok, pid in list(self.token_to_peer.items()):
            if pid == peer_id:
                del self.token_to_peer[tok]

        # Finally evict the Peer object itself.
        del self.peers[peer_id]

        if was_producer:
            await self.broadcast_to_listeners(
                {
                    "type": "peerStatusChanged",
                    "peerId": peer_id,
                    "roles": [],
                    "meta": peer.meta,
                },
                exclude_id=peer_id,
                owner_username=peer.username,
            )
            logger.info(f"Producer disconnected: {peer_id}")

        logger.info(f"Peer disconnected: {peer_id}")


# Global instances. Fleet usage (see ``fleet_usage.py``) is always
# tracked (cheap, in memory); it is only published when ``FLEET_USAGE``
# configures a sink.
FLEET_USAGE = fleet_usage_config_from_env(os.environ)
usage = UsageTracker(window_seconds=FLEET_USAGE.window_seconds)
usage_publisher = build_usage_publisher(FLEET_USAGE, usage)
signaling = SignalingServer(usage=usage)


def _register_dev_usage_route(target: FastAPI, local_dir: str) -> None:
    """Serve ``<local_dir>/summary.json`` at ``DEV_USAGE_SUMMARY_ROUTE``.

    Dev only: registered at import time solely when
    ``FLEET_USAGE_LOCAL_DIR`` is active (which already excludes Spaces).
    """
    summary_path = os.path.join(local_dir, USAGE_SUMMARY_PATH)

    @target.get(DEV_USAGE_SUMMARY_ROUTE, include_in_schema=False)
    async def dev_fleet_usage_summary():
        try:
            with open(summary_path, "rb") as f:
                content = f.read()
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="No usage summary yet")
        return Response(
            content=content,
            media_type="application/json",
            headers={"Cache-Control": "no-store"},
        )


if FLEET_USAGE.local_dir:
    _register_dev_usage_route(app, FLEET_USAGE.local_dir)


@app.get("/events")
async def events(request: Request, token: str = Depends(_resolve_hf_token)):
    """SSE endpoint for receiving messages from server."""
    username = await validate_hf_token(token)
    if not username:
        raise HTTPException(status_code=401, detail="Invalid token")

    logger.info("SSE connection request from user: %s", username)

    if not check_rate_limit(token):
        raise HTTPException(status_code=429, detail="Rate limit exceeded. Try again later.")

    peer = signaling.get_or_create_peer(token, username)

    # One queue and one generation per SSE connection. A reconnect on
    # the same token (daemon restart while its previous socket is
    # half-open behind the proxy, or a reattach within the SSE grace)
    # supersedes the old generator: it must neither steal messages from
    # the new queue nor detach / evict the peer when its dead socket
    # finally closes. Messages still queued for the peer (e.g. queued
    # while it was detached) are carried over to the new queue and
    # delivered after the welcome + list below (see ``attach_sse``).
    generation, queue = await signaling.connect_sse(peer)

    def _is_current_connection() -> bool:
        return signaling.peers.get(peer.peer_id) is peer and peer.sse_generation == generation

    async def event_generator() -> AsyncGenerator[dict, None]:
        # The try covers the welcome + list too, so a stream that dies
        # during the handshake still detaches its peer.
        try:
            if _is_current_connection():
                signaling.sse_stream_started(peer)

            # Send welcome message with username for client info. The
            # advertised heartbeat cadence drives daemons >= v1.7.2 (they
            # negotiate from this field); older daemons ignore it and keep
            # their internal default, which is faster and therefore safe.
            yield {
                "event": "message",
                "data": json.dumps(
                    {
                        "type": "welcome",
                        "peerId": peer.peer_id,
                        "username": username,
                        "recommended_heartbeat_interval_seconds": RECOMMENDED_HEARTBEAT_INTERVAL_SECONDS,
                    }
                ),
            }

            # Send current producers list for listeners (filtered by owner)
            yield {"event": "message", "data": json.dumps({"type": "list", "producers": signaling.get_producers_list(username)})}

            while True:
                # Check if client disconnected. Best-effort:
                # ``is_disconnected`` returns True on FIN/RST visible
                # to starlette, but half-open sockets can stay False
                # forever behind HTTP/2 proxies - that case is covered
                # by the producer sweep instead.
                if await request.is_disconnected():
                    break

                # Superseded by a newer connection on the same token,
                # or evicted (sweep / stable-id collision): stop
                # serving, and let the finally below skip the eviction.
                if not _is_current_connection():
                    break

                try:
                    message = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield {"event": "message", "data": json.dumps(message)}
                except asyncio.TimeoutError:
                    # Server-pushed keepalive. Its ONLY job is to keep
                    # the HTTP/2 proxy in front of us from killing the
                    # idle connection (HF Spaces, Cloudflare, etc.).
                    yield {"event": "ping", "data": ""}

        finally:
            # Only the peer's current connection may detach it: a stale
            # generator closing late must not tear down the live peer
            # that superseded it. Detaching starts the reconnect grace
            # (or evicts at once when the grace is disabled).
            if _is_current_connection():
                await signaling.detach_peer(peer.peer_id)

    return EventSourceResponse(event_generator())


@app.post("/send")
async def send_message(request: Request, token: str = Depends(_resolve_hf_token)):
    """HTTP POST endpoint for sending messages to server."""
    username = await validate_hf_token(token)
    if not username:
        raise HTTPException(status_code=401, detail="Invalid token")

    if not check_rate_limit(token):
        raise HTTPException(status_code=429, detail="Rate limit exceeded. Try again later.")

    # Get or reconnect peer
    if token not in signaling.token_to_peer:
        raise HTTPException(status_code=400, detail="Connect to /events first")

    peer_id = signaling.token_to_peer[token]
    if peer_id not in signaling.peers:
        raise HTTPException(status_code=400, detail="Peer not found")

    peer = signaling.peers[peer_id]

    body = await request.json()
    # The peer may have been evicted (grace expiry, sweep, collision)
    # while the body was being read: handling the message anyway could
    # re-insert a ghost into ``producers``.
    if signaling.peers.get(peer_id) is not peer:
        raise HTTPException(status_code=400, detail="Peer not found")
    response = await signaling.handle_message(peer, body)

    return response or {"status": "ok"}


# Status page template. Design tokens mirror the Reachy Mini mobile app
# MUI theme (reachy_mini_mobile_app/src/theme.ts): accent #FF9500,
# radius 12, system font stack, light/dark palettes selected via
# prefers-color-scheme. ``string.Template`` ($-placeholders) is used
# instead of an f-string so CSS/JS braces need no escaping. Counters
# are server-rendered, then kept live by a small /health poll (every
# ``STATUS_POLL_SECONDS``, visible tabs only).
#
# Template hygiene: every ``$`` in the source below must be a placeholder
# filled by ``root()`` - a stray one raises ``KeyError`` at request time
# (``substitute``, not ``safe_substitute``, on purpose: a typo must fail
# loudly in the route test rather than ship a literal "$peers"). Write a
# literal dollar sign as ``$$``.

# One card per public robot kind, in ``PUBLIC_ROBOT_KINDS`` order (the
# same order ``/health`` uses), generated from the constant label table
# at import time. Both the label and the element id come from constants;
# the ``$kind_<kind>`` placeholders are the only per-request part and
# are filled with integers. Nothing here is derived from ``meta``.
_KIND_CARDS_HTML = "\n".join(
    f"""            <div class="kind">
                <div class="kind-label">{ROBOT_KIND_LABELS[kind]}</div>
                <div class="kind-value" id="kind-{kind}">$kind_{kind}</div>
            </div>"""
    for kind in PUBLIC_ROBOT_KINDS
)

_STATUS_PAGE_SOURCE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Reachy Mini Central</title>
    <style>
        :root {
            --accent: #FF9500;
            --bg: #fafafa;
            --paper: #ffffff;
            --text: #111111;
            --text-secondary: rgba(0, 0, 0, 0.65);
            --divider: rgba(0, 0, 0, 0.08);
            --radius: 12px;
        }
        @media (prefers-color-scheme: dark) {
            :root {
                --bg: #101013;
                --paper: #1a1a1a;
                --text: #f5f5f5;
                --text-secondary: rgba(255, 255, 255, 0.72);
                --divider: rgba(255, 255, 255, 0.08);
            }
        }
        * { box-sizing: border-box; }
        body {
            margin: 0;
            padding: 48px 20px;
            background: var(--bg);
            color: var(--text);
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", sans-serif;
            line-height: 1.5;
        }
        main { max-width: 720px; margin: 0 auto; }
        header { display: flex; align-items: center; gap: 10px; margin-bottom: 24px; }
        .dot { width: 10px; height: 10px; border-radius: 50%; background: var(--accent); }
        h1 { font-size: 24px; font-weight: 700; letter-spacing: -0.2px; margin: 0; }
        .pill {
            margin-left: auto;
            font-size: 12px; font-weight: 600;
            color: var(--accent);
            border: 1px solid rgba(255, 149, 0, 0.35);
            padding: 3px 12px; border-radius: 999px;
        }
        .card {
            background: var(--paper);
            border: 1px solid var(--divider);
            border-radius: var(--radius);
            padding: 20px;
            margin-bottom: 12px;
        }
        .stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-bottom: 12px; }
        .stats .card { margin-bottom: 0; }
        .value { font-size: 32px; font-weight: 700; letter-spacing: -0.4px; margin-top: 4px; }
        .kinds { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-top: 12px; }
        .kind {
            display: flex; align-items: center; justify-content: space-between; gap: 12px;
            border: 1px solid var(--divider);
            border-left: 3px solid var(--accent);
            border-radius: var(--radius);
            padding: 12px 16px;
        }
        .kind-label { font-size: 14px; font-weight: 600; }
        .kind-value { font-size: 24px; font-weight: 700; letter-spacing: -0.3px; }
        .overline {
            font-size: 11px; font-weight: 600; letter-spacing: 0.5px;
            text-transform: uppercase; color: var(--text-secondary);
        }
        ul { margin: 10px 0 0; padding-left: 20px; color: var(--text-secondary); font-size: 14px; }
        li { margin-bottom: 4px; }
        li strong { color: var(--text); font-weight: 600; }
        code {
            background: var(--divider);
            padding: 2px 7px; border-radius: 6px;
            font-size: 13px;
            font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
        }
        footer { color: var(--text-secondary); font-size: 13px; margin-top: 20px; }
        footer p { margin: 0 0 4px; }
        @media (max-width: 520px) { .stats, .kinds { grid-template-columns: 1fr; } }
    </style>
</head>
<body>
    <main>
        <header>
            <span class="dot"></span>
            <h1>Reachy Mini Central</h1>
            <span class="pill">running</span>
        </header>
        <section class="stats">
            <div class="card">
                <div class="overline">Connected peers</div>
                <div class="value" id="peers">$peers</div>
            </div>
            <div class="card">
                <div class="overline">Active producers</div>
                <div class="value" id="producers">$producers</div>
            </div>
            <div class="card">
                <div class="overline">Active sessions</div>
                <div class="value" id="sessions">$sessions</div>
            </div>
        </section>
        <section class="card">
            <div class="overline">Robots online by kind</div>
            <div class="kinds">
${kind_cards}
            </div>
        </section>
$usage_section
        <section class="card">
            <div class="overline">Endpoints</div>
            <ul>
                <li><code>GET /events</code> - SSE stream for receiving messages</li>
                <li><code>POST /send</code> - send messages to server</li>
                <li><code>GET /api/robot-status</code> - busy/free status of the caller's robots</li>
                <li><code>GET /health</code> - public counters, incl. <code>producers_by_kind</code> and uptime</li>
                <li>Authentication: <code>Authorization: Bearer &lt;HF token&gt;</code></li>
            </ul>
        </section>
        <section class="card">
            <div class="overline">Security</div>
            <ul>
                <li>HuggingFace token authentication required</li>
                <li>Owner-based filtering: users only see their own robots</li>
                <li>Rate limiting: $rate_requests requests per ${rate_window}s per peer (sliding window)</li>
                <li>Stale-producer sweep: silent producers evicted after ${lease}s</li>
            </ul>
        </section>
        <footer>
            <p>Up since <span id="started-at">$started_at</span>, running for <span id="uptime">$uptime</span> (counters reset on every redeploy).</p>
            <p>Live counters refresh every ${poll_seconds}s while this page is visible.</p>
            <p>Implements the GStreamer WebRTC signaling protocol over HTTP/SSE.</p>
        </footer>
    </main>
    <script>
        // Mirrors _format_uptime() server-side so the value does not
        // visibly change shape on the first poll.
        function formatUptime(totalSeconds) {
            const s = Math.max(0, Math.floor(totalSeconds));
            const d = Math.floor(s / 86400);
            const h = Math.floor((s % 86400) / 3600);
            const m = Math.floor((s % 3600) / 60);
            if (d > 0) return d + "d " + h + "h " + m + "m";
            if (h > 0) return h + "h " + m + "m";
            return m + "m";
        }
        async function refresh() {
            try {
                const response = await fetch("/health", { cache: "no-store" });
                if (!response.ok) return;
                const data = await response.json();
                // textContent only: /health values are integers, and the
                // per-kind keys are a fixed server-side set, but the page
                // must never interpret any of it as markup.
                for (const key of ["peers", "producers", "sessions"]) {
                    document.getElementById(key).textContent = data[key];
                }
                const byKind = data.producers_by_kind || {};
                for (const kind of Object.keys(byKind)) {
                    const el = document.getElementById("kind-" + kind);
                    if (el) el.textContent = byKind[kind];
                }
                if (typeof data.uptime_seconds === "number") {
                    document.getElementById("uptime").textContent = formatUptime(data.uptime_seconds);
                }
            } catch (err) {
                // Transient network error: keep last values, retry next tick.
            }
        }
        // Poll only while the tab is visible: a hidden tab costs this
        // server nothing. Coming back refreshes at once, then resumes.
        const POLL_MS = $poll_ms;
        let pollTimer = null;
        function startPolling() {
            if (pollTimer === null) pollTimer = setInterval(refresh, POLL_MS);
        }
        function stopPolling() {
            if (pollTimer !== null) {
                clearInterval(pollTimer);
                pollTimer = null;
            }
        }
        document.addEventListener("visibilitychange", function () {
            if (document.visibilityState === "visible") {
                refresh();
                startPolling();
            } else {
                stopPolling();
            }
        });
        if (document.visibilityState === "visible") startPolling();
    </script>
</body>
</html>"""

# Expand the constant per-kind cards once at import. ``${kind_cards}`` is
# spelled as a Template placeholder on purpose: if this expansion ever
# stops matching, ``substitute()`` raises KeyError and the route test
# fails instead of the page silently shipping the raw sentinel.
_STATUS_PAGE = Template(_STATUS_PAGE_SOURCE.replace("${kind_cards}", _KIND_CARDS_HTML))

# The "Fleet usage" section depends only on operator config, so it is
# rendered once at import ("" when no usage source is configured).
_USAGE_SECTION_HTML = render_usage_section(FLEET_USAGE)


def _format_uptime(total_seconds: float) -> str:
    """Human-readable uptime, e.g. ``3d 4h 12m`` / ``4h 12m`` / ``12m``.

    Keep in sync with ``formatUptime`` in the status page JS.
    """
    s = max(0, int(total_seconds))
    d, rem = divmod(s, 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    if d > 0:
        return f"{d}d {h}h {m}m"
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"


def _public_counters() -> dict:
    """Counters shared by ``/`` and ``/health`` - one definition, two views.

    ``peers``, ``producers`` and ``producers_by_kind`` count connected
    peers only; ``producers_by_kind`` always carries every key in
    ``PUBLIC_ROBOT_KINDS`` (zero-filled). ``started_at`` is the wall-clock
    start in ISO 8601 UTC; ``uptime_seconds`` is monotonic.
    """
    return {
        "peers": signaling.count_connected_peers(),
        "producers": signaling.count_connected_producers(),
        "sessions": len(signaling.sessions),
        "producers_by_kind": signaling.count_connected_producers_by_kind(),
        "started_at": STARTED_AT_ISO,
        "uptime_seconds": int(time.monotonic() - _STARTED_MONOTONIC),
    }


# Public-page load bound. Every open status-page tab polls ``/health``;
# the body (counters walk every producer) is computed at most once per
# ``HEALTH_CACHE_SECONDS`` and the same dict is served to every caller in
# that window, so N viewers cost one computation per window, not N.
# ``Cache-Control: no-store`` stays on the responses: this server-side
# cache is what bounds the work, and a proxy/browser cache would only
# add staleness on top. The status page itself polls every
# ``STATUS_POLL_SECONDS`` and only while its tab is visible.
HEALTH_CACHE_SECONDS = 2.0
STATUS_POLL_SECONDS = 15

# (computed_at monotonic, body) or None. Single event loop and no await
# between check and store, so no lock is needed.
_health_cache: Optional[tuple[float, dict]] = None


def _health_cache_reset() -> None:
    """Drop the cached ``/health`` body (tests; next call recomputes)."""
    global _health_cache
    _health_cache = None


def _cached_health_body(now: Optional[float] = None) -> dict:
    """The ``/health`` body, recomputed at most once per ``HEALTH_CACHE_SECONDS``.

    Shared by ``/health`` and ``/`` (which reads the counter keys). The
    returned dict is shared between callers and must not be mutated.
    ``now`` (monotonic seconds) is injectable for tests.
    """
    global _health_cache
    if now is None:
        now = time.monotonic()
    cached = _health_cache
    if cached is not None and 0 <= now - cached[0] < HEALTH_CACHE_SECONDS:
        return cached[1]
    body = {
        "status": "healthy",
        **_public_counters(),
        "usage_publisher": publisher_health(usage, usage_publisher),
        "auth": hf_validator.health(),
        "sse": signaling.sse_health(),
    }
    _health_cache = (now, body)
    return body


@app.get("/")
async def root():
    """Status page: server-rendered counters kept live by a /health poll.

    Everything interpolated here is either an integer counter, a
    server-side constant, a server-generated timestamp, or the fleet
    usage section built from validated operator config. Nothing from
    ``meta`` may ever be passed to ``substitute`` (no escaping).
    """
    counters = _cached_health_body()
    return HTMLResponse(
        content=_STATUS_PAGE.substitute(
            peers=counters["peers"],
            producers=counters["producers"],
            sessions=counters["sessions"],
            started_at=counters["started_at"],
            uptime=_format_uptime(counters["uptime_seconds"]),
            rate_requests=RATE_LIMIT_REQUESTS,
            rate_window=int(RATE_LIMIT_WINDOW),
            lease=int(PRODUCER_LEASE_SECONDS),
            usage_section=_USAGE_SECTION_HTML,
            poll_ms=int(STATUS_POLL_SECONDS * 1000),
            poll_seconds=int(STATUS_POLL_SECONDS),
            **{f"kind_{k}": v for k, v in counters["producers_by_kind"].items()},
        ),
        headers={"Cache-Control": "no-store"},
    )


@app.get("/health")
async def health():
    """Public health check and counters.

    Shape (existing keys are public and must stay stable; new keys are
    additive)::

        {
            "status": "healthy",
            "peers": 3, "producers": 2, "sessions": 1,
            "producers_by_kind": {"reachy_mini": 1, "microduck": 1, "other": 0},
            "started_at": "2026-09-16T08:00:00Z",
            "uptime_seconds": 12345,
            "usage_publisher": {
                "enabled": true,
                "last_published_at": "2026-09-16T08:30:00Z",
                "pending_rows": 0,
                "dropped_rows": 0
            },
            "auth": {
                "cache_size": 280, "negative_cache_size": 3,
                "whoami_calls_total": 1200, "whoami_rejected_total": 4,
                "whoami_errors_total": 0, "unknown_token_shed_total": 0
            },
            "sse": {
                "grace_seconds": 15.0, "detached_now": 0,
                "detach_total": 120, "reattach_total": 118,
                "grace_expired_total": 2, "reattach_latency_s_max": 8.8,
                "sessions_ended_at_detach_total": 7,
                "consumer_session_replaced_total": 1
            }
        }

    ``usage_publisher`` is the fleet usage publisher's aggregate state
    (``enabled`` false when no sink is configured; ``last_published_at``
    null until the first successful publish) - the only outside view of a
    stalled publisher.

    ``auth`` is the token-validation layer's aggregate state: positive /
    negative cache sizes and since-boot counters of whoami calls, explicit
    HF rejections, inconclusive calls (network / 429 / 5xx) and
    unknown-token requests shed by the whoami cap (503). No identifiers.

    ``sse`` is the SSE reconnect grace's aggregate state: the configured
    grace, peers currently detached (SSE closed, still registered and
    counted above), and since-boot counts of detaches, reattaches within
    the grace, grace expiries (evictions) and the longest reattach
    latency seen, plus producer sessions ended because the producer's
    stream closed or was superseded and consumer self-replacements. No
    identifiers.

    The body is served from a micro-cache (``HEALTH_CACHE_SECONDS``), so
    values may lag live state by up to that long.
    """
    return JSONResponse(_cached_health_body(), headers={"Cache-Control": "no-store"})


@app.get("/api/robot-status")
async def robot_status(token: str = Depends(_resolve_hf_token)):
    """Return busy/free status and currently-connected app for each robot owned by the caller.

    Used by clients (e.g. the desktop app) to render a passive status indicator
    without consuming a session slot. Filtered by HuggingFace username so users
    only see their own robots.

    Response shape:
        {
            "robots": [
                {
                    "peerId": "...",
                    "robotName": "reachy_mini",
                    "busy": true,
                    "activeApp": "Hand Tracker Live App Demo",
                    "meta": {"name": "reachy_mini", "install_id": "abc123..."},
                    "last_seen_age_seconds": 2.4
                },
                ...
            ]
        }

    ``meta`` mirrors the producer metadata as registered via ``setPeerStatus``
    (same shape as the SSE ``list`` message). It carries ``install_id`` -
    the stable per-install key that mobile/desktop clients use to dedupe a
    central listing against the same physical robot's BLE / loopback row.
    Forwarded verbatim so future daemon-side fields (capabilities, version,
    ...) appear without another central change.

    ``last_seen_age_seconds`` is the wall-time gap since the producer's
    last inbound message (POST /send, which includes the daemon's
    periodic heartbeat). For heartbeat-capable daemons this is a real
    liveness signal: the sweep evicts producers whose age exceeds
    ``PRODUCER_LEASE_SECONDS``. For legacy daemons (no ``hardware_id``
    in ``meta``) it only reflects their last signaling activity.
    """
    username = await validate_hf_token(token)
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )

    now = time.monotonic()
    robots = []
    for pid, p in signaling.producers.items():
        if p.username != username:
            continue
        busy, active_app = signaling.producer_session_snapshot(p)
        robots.append(
            {
                "peerId": pid,
                "robotName": p.meta.get("name"),
                "busy": busy,
                "activeApp": active_app,
                "meta": p.meta,
                "last_seen_age_seconds": round(now - p.last_seen, 2),
            }
        )

    return {"robots": robots}


@app.get("/api/debug/peers")
async def debug_peers(token: str = Depends(_resolve_hf_token)):
    """Owner-filtered dump of all known peers for debugging.

    Returns every peer (producers AND consumers) belonging to the caller,
    not just registered producers. Use this when a robot does not show up
    where expected: see whether the daemon's SSE channel is still open
    (``connected=True``), how stale ``last_seen`` is, what role/meta is
    registered, and whether a session is in progress. ``detached`` is
    true while the peer's SSE stream is closed but its reconnect grace is
    still running (it is then still listed and counted);
    ``detached_age_seconds`` is how long it has been detached (null when
    attached).

    This is more verbose than ``/api/robot-status`` (which only returns
    registered producers and elides session/peer details). Same auth
    rules and same owner-only filter, so it's safe to expose.

    Response shape:
        {
            "now": 1234.56,
            "peers": [
                {
                    "peerId": "...",
                    "role": "producer",
                    "connected": true,
                    "in_producers": true,
                    "session_id": null,
                    "partner_id": null,
                    "meta": {...},
                    "last_seen": 1230.12,
                    "last_seen_age_seconds": 4.44,
                    "detached": false,
                    "detached_age_seconds": null
                },
                ...
            ]
        }
    """
    username = await validate_hf_token(token)
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )

    now = time.monotonic()
    grace_now = signaling.clock()
    peers = []
    for pid, p in signaling.peers.items():
        if p.username != username:
            continue
        peers.append(
            {
                "peerId": pid,
                "role": p.role,
                "connected": p.connected,
                "in_producers": pid in signaling.producers,
                "session_id": p.session_id,
                "partner_id": p.partner_id,
                "meta": p.meta,
                "last_seen": round(p.last_seen, 2),
                "last_seen_age_seconds": round(now - p.last_seen, 2),
                "detached": p.detached_at is not None,
                "detached_age_seconds": (
                    round(max(0.0, grace_now - p.detached_at), 2)
                    if p.detached_at is not None
                    else None
                ),
            }
        )

    return {
        "now": round(now, 2),
        "peers": peers,
    }
