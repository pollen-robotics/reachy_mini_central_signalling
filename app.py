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

Lifecycle responsibilities (see ``reachy_mini/docs/SIGNALING.md`` for the
canonical contract):

- Forward producer ``meta`` verbatim to listeners (no re-interpretation).
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
  channel close.
"""

import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from string import Template
from typing import Optional, AsyncGenerator

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from sse_starlette.sse import EventSourceResponse

logging.basicConfig(level=logging.INFO)
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
    """Run the stale-producer sweeper for the app's lifetime."""
    sweeper_task = asyncio.create_task(signaling.run_producer_sweeper())
    try:
        yield
    finally:
        sweeper_task.cancel()
        try:
            await sweeper_task
        except asyncio.CancelledError:
            pass


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

# Cache for validated tokens: token -> (username, expires_at monotonic).
#
# Entries expire after ``TOKEN_CACHE_TTL_SECONDS`` so a token revoked on
# HuggingFace stops working here within the TTL instead of surviving
# until the next Space restart. Expired entries are kept around (and
# lazily re-validated) so that a transient whoami outage degrades to
# serving the stale identity rather than 401-ing the whole fleet; only
# an explicit rejection from HF drops the entry. Entries stale for more
# than ``TOKEN_CACHE_STALE_GRACE_SECONDS`` are pruned by the sweeper.
TOKEN_CACHE_TTL_SECONDS = 3600.0
TOKEN_CACHE_STALE_GRACE_SECONDS = 86400.0
token_cache: dict[str, tuple[str, float]] = {}

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
            token_cache[_tok.strip()] = (_user.strip(), float("inf"))
elif os.environ.get("DEV_TOKEN_SEED"):
    logger.warning(
        "DEV_TOKEN_SEED is set but ignored: refusing to seed the token "
        "cache on a deployed Space."
    )


def _prune_token_cache() -> None:
    """Drop cache entries whose stale-serving grace has fully elapsed."""
    now = time.monotonic()
    for tok, (_, expires_at) in list(token_cache.items()):
        if now > expires_at + TOKEN_CACHE_STALE_GRACE_SECONDS:
            del token_cache[tok]


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


async def validate_hf_token(token: str) -> Optional[str]:
    """Validate HuggingFace token and return username if valid.

    Fresh cache hits are served directly. Expired entries trigger a
    re-validation against whoami; on transient failure (network,
    HF outage) the stale identity is served rather than failing the
    caller, and only an explicit non-200 from HF (revoked / invalid
    token) drops the entry.
    """
    if not token:
        return None

    cached = token_cache.get(token)
    if cached is not None and time.monotonic() < cached[1]:
        return cached[0]

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://huggingface.co/api/whoami-v2",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10.0
            )

            if response.status_code == 200:
                data = response.json()
                username = data.get("name", "unknown")
                token_cache[token] = (
                    username,
                    time.monotonic() + TOKEN_CACHE_TTL_SECONDS,
                )
                logger.info(f"Token validated for user: {username}")
                return username

            if response.status_code in (401, 403):
                # Explicit rejection: the token is revoked or invalid.
                # This is the ONLY case that may drop a cache entry.
                logger.warning(
                    f"Token rejected by HF: {response.status_code}"
                )
                token_cache.pop(token, None)
                return None

            # 429 / 5xx: HF is unhappy, not the token. Treat like a
            # network failure and serve the stale identity if we have
            # one. Confusing a whoami 429 with a revocation would let
            # an attacker who exhausts our whoami quota (by spraying
            # invalid tokens, which are validated pre-rate-limit) turn
            # HF rate-limiting into fleet-wide 401s at the hourly
            # re-validation.
            logger.warning(
                f"Token validation inconclusive (HF {response.status_code})"
            )
            if cached is not None:
                return cached[0]
            return None
    except Exception as e:
        logger.error(f"Error validating token: {e}")
        if cached is not None:
            logger.warning(
                "Serving stale token cache entry for user %s during "
                "validation outage",
                cached[0],
            )
            return cached[0]
        return None


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


class SignalingServer:
    """HTTP-based WebRTC signaling server."""

    def __init__(self):
        self.peers: dict[str, Peer] = {}
        self.sessions: dict[str, tuple[str, str]] = {}  # session_id -> (producer_id, consumer_id)
        self.producers: dict[str, Peer] = {}  # producer_id -> Peer
        # Token -> peer_id mapping. Lets a daemon recover its peer_id
        # across an SSE reconnect without losing its producer slot, but
        # is purged on disconnect so a hard-evicted peer doesn't come
        # back with the same id.
        self.token_to_peer: dict[str, str] = {}

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
                logger.info(f"Peer reconnected: {peer_id}")
                return peer

        # Create new peer
        peer_id = str(uuid.uuid4())
        peer = Peer(peer_id=peer_id, username=username)
        self.peers[peer_id] = peer
        self.token_to_peer[token] = peer_id
        logger.info(f"New peer created: {peer_id} for user {username}")
        return peer

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
          (last-writer-wins, see ``docs/SIGNALING.md``). Broadcast a
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
        peer.meta = meta

        if "producer" in roles:
            await self._evict_stable_id_collisions(peer, meta)
            peer.role = "producer"
            self.producers[peer.peer_id] = peer
            logger.info(f"Producer registered: {peer.peer_id} with meta: {meta}")
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
            await self.handle_end_session(peer.session_id, reason="producer_withdrew")
        if was_producer:
            del self.producers[peer.peer_id]
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
                    old_peer.session_id, reason="install_id_takeover"
                )
            await self.disconnect_peer(old_id)

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

    async def handle_end_session(self, session_id: str, reason: Optional[str] = None):
        """End a session and notify both peers.

        The optional ``reason`` is propagated to both peers so clients can
        distinguish a user-initiated stop ("Session stopped") from a
        server-side eviction (e.g. ``robot_busy_local_app`` when the robot
        relay refuses because a local Python app holds the daemon lock).
        Without forwarding the reason, clients see only an unexplained
        endSession and have no way to surface a meaningful message.
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
                await self.send_to_peer(peer_id, msg)

        del self.sessions[session_id]

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
            await self.handle_end_session(
                message.get("sessionId"),
                reason=message.get("reason"),
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

        Returns the list of evicted peer ids (handy for tests/logs).
        """
        now = time.monotonic()
        stale = [
            pid
            for pid, p in self.producers.items()
            if p.meta.get("hardware_id")
            and now - p.last_seen > PRODUCER_LEASE_SECONDS
        ]
        for pid in stale:
            peer = self.peers.get(pid)
            logger.warning(
                "Sweeping stale producer %s (name=%r, silent for %.0fs)",
                pid,
                peer.meta.get("name") if peer else None,
                now - peer.last_seen if peer else -1,
            )
            await self.disconnect_peer(pid)
        return stale

    async def run_producer_sweeper(self) -> None:
        """Background task: periodic stale-producer sweep + cache pruning."""
        while True:
            await asyncio.sleep(PRODUCER_SWEEP_INTERVAL_SECONDS)
            try:
                await self.sweep_stale_producers()
                _prune_rate_limit_buckets()
                _prune_token_cache()
            except Exception:
                logger.exception("Producer sweeper iteration failed")

    async def disconnect_peer(self, peer_id: str):
        """Fully evict a peer from every server-side structure.

        Called from three places:

        - SSE close path (``request.is_disconnected()`` becoming true:
          the peer's HTTP channel closed cleanly).
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
        """
        if peer_id not in self.peers:
            return

        peer = self.peers[peer_id]
        peer.connected = False

        # End any session this peer is part of (either as producer or consumer).
        # handle_end_session clears session_id/partner_id on both sides and
        # notifies the remaining peer so it can tear down its WebRTC state.
        if peer.session_id is not None:
            await self.handle_end_session(peer.session_id)

        # If we were a producer, tell same-user listeners now so their
        # UI updates without waiting for a fresh /list.
        was_producer = peer_id in self.producers
        if was_producer:
            del self.producers[peer_id]

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


# Global signaling server instance
signaling = SignalingServer()


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
    # half-open behind the proxy) supersedes the old generator: it
    # must neither steal messages from the shared queue nor evict the
    # peer when its dead socket finally closes. Replacing the queue is
    # safe because an SSE (re)connect restarts the conversation anyway
    # (welcome + list are re-sent below); anything queued before the
    # reconnect addressed the previous connection.
    peer.sse_generation += 1
    generation = peer.sse_generation
    peer.message_queue = asyncio.Queue()
    queue = peer.message_queue

    def _is_current_connection() -> bool:
        return signaling.peers.get(peer.peer_id) is peer and peer.sse_generation == generation

    async def event_generator() -> AsyncGenerator[dict, None]:
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

        try:
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
            # Only the peer's current connection may evict it: a stale
            # generator closing late must not tear down the live peer
            # that superseded it.
            if _is_current_connection():
                await signaling.disconnect_peer(peer.peer_id)

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
    response = await signaling.handle_message(peer, body)

    return response or {"status": "ok"}


# Status page template. Design tokens mirror the Reachy Mini mobile app
# MUI theme (reachy_mini_mobile_app/src/theme.ts): accent #FF9500,
# radius 12, system font stack, light/dark palettes selected via
# prefers-color-scheme. ``string.Template`` ($-placeholders) is used
# instead of an f-string so CSS/JS braces need no escaping. Counters
# are server-rendered, then kept live by a small /health poll.
_STATUS_PAGE = Template("""<!DOCTYPE html>
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
        @media (max-width: 520px) { .stats { grid-template-columns: 1fr; } }
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
            <div class="overline">Endpoints</div>
            <ul>
                <li><code>GET /events</code> - SSE stream for receiving messages</li>
                <li><code>POST /send</code> - send messages to server</li>
                <li><code>GET /api/robot-status</code> - busy/free status of the caller's robots</li>
                <li><code>GET /health</code> - public counters</li>
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
            Implements the GStreamer WebRTC signaling protocol over HTTP/SSE.
        </footer>
    </main>
    <script>
        async function refresh() {
            try {
                const response = await fetch("/health");
                if (!response.ok) return;
                const data = await response.json();
                for (const key of ["peers", "producers", "sessions"]) {
                    document.getElementById(key).textContent = data[key];
                }
            } catch (err) {
                // Transient network error: keep last values, retry next tick.
            }
        }
        setInterval(refresh, 3000);
    </script>
</body>
</html>""")


@app.get("/")
async def root():
    """Status page: server-rendered counters kept live by a /health poll."""
    return HTMLResponse(
        content=_STATUS_PAGE.substitute(
            peers=len([p for p in signaling.peers.values() if p.connected]),
            producers=len(signaling.producers),
            sessions=len(signaling.sessions),
            rate_requests=RATE_LIMIT_REQUESTS,
            rate_window=int(RATE_LIMIT_WINDOW),
            lease=int(PRODUCER_LEASE_SECONDS),
        )
    )


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "peers": len([p for p in signaling.peers.values() if p.connected]),
        "producers": len(signaling.producers),
        "sessions": len(signaling.sessions),
    }


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
    registered, and whether a session is in progress.

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
                    "last_seen_age_seconds": 4.44
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
            }
        )

    return {
        "now": round(now, 2),
        "peers": peers,
    }
