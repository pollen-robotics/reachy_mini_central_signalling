"""
Reachy Mini Central - WebRTC Signaling Server

This server implements the GStreamer WebRTC signaling protocol using:
- SSE (Server-Sent Events) for server-to-client messages
- HTTP POST for client-to-server messages

This works reliably through HTTP/2 proxies like HuggingFace Spaces.

Lifecycle responsibilities (see ``reachy_mini/docs/SIGNALING.md`` for the
canonical contract):

- Forward producer ``meta`` verbatim to listeners (no re-interpretation).
- Track ``last_seen`` per peer; evict peers whose lease has expired.
- Honour ``setPeerStatus(roles=[])`` by removing the peer from
  ``producers`` immediately (the SSE channel stays open so the daemon
  can re-register without reconnecting).
- Detect ``install_id`` collisions inside a single user's fleet and
  evict the older producer (last-writer-wins) so a re-flashed daemon
  never coexists with its own ghost.
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
from typing import Optional, AsyncGenerator

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from sse_starlette.sse import EventSourceResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- Liveness (TTL / lease) settings ---------------------------------
#
# A peer is considered alive while it keeps its SSE channel open AND
# its last application-level activity (POST /send or SSE message
# delivery) is younger than ``LEASE_SECONDS``. The sweeper task scans
# every ``SWEEPER_INTERVAL_SECONDS`` and evicts any expired peer.
#
# Sizing: ``LEASE_SECONDS`` MUST be at least ``2.5 *
# RECOMMENDED_HEARTBEAT`` so a healthy daemon tolerates 2 consecutive
# missed heartbeats (network blip, transient relay restart) without
# false eviction. The daemon negotiates its actual heartbeat interval
# from this lease via the ``welcome`` SSE message (see
# ``recommended_heartbeat_interval_seconds`` below), so the only
# parameter we need to keep stable is the lease itself.
#
# This Space (tfrere/reachy_mini_central) has ``sleep_time=None`` and
# ``gcTimeout=48h``, so cold-start latency is not a concern; we can
# size the lease tightly to minimise the staleness window observed by
# remote clients (mobile/desktop too far for BLE).
#
# 30 s default: heartbeat = lease / 3 = 10 s, which sits comfortably
# below the 60 s idle timeout typical of HTTP/2 proxies (HF Spaces,
# Cloudflare). This applies the WebSocket "75 % rule" (heartbeat at
# 75 % of the shortest proxy timeout) while halving baseline relay
# traffic vs the previous 5 s cadence. Tunable via env var for local
# testing of failure paths without a code change.
LEASE_SECONDS = float(os.getenv("REACHY_CENTRAL_LEASE_SECONDS", "30"))
SWEEPER_INTERVAL_SECONDS = float(os.getenv("REACHY_CENTRAL_SWEEPER_INTERVAL", "3"))


def _recommended_heartbeat_interval() -> float:
    """Return the heartbeat interval daemons should use, derived from
    ``LEASE_SECONDS``.

    Lease ÷ 3 gives daemons ~2 consecutive misses of headroom before
    eviction (heartbeat at t=0,3,6 → last_seen=0; if next 3 fail, lease
    expires at t=15 with one tick of margin). Daemons receive this in
    the ``welcome`` frame and adjust their loop accordingly, so server
    operators only ever need to tune ``LEASE_SECONDS``.
    """
    return max(1.0, LEASE_SECONDS / 3.0)


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
    """Spawn / cancel the TTL sweeper task alongside the FastAPI app."""
    sweeper_task = asyncio.create_task(signaling.run_ttl_sweeper())
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

# Cache for validated tokens (token -> username)
token_cache: dict[str, str] = {}


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
# per connection). With heartbeat at 10 s (6 req/min), a typical mobile
# session (offer + answer + ~10 ICE candidates ~ 15 req over a few
# seconds), and aggressive reconnects, observed peak under load is
# ~50-100 req/min/peer. We keep a 12-24x headroom so adding features
# (status polls, presence, etc.) does not require retuning the limit.
RATE_LIMIT_REQUESTS = 1200
RATE_LIMIT_WINDOW = 60.0
_rate_limit_buckets: dict[str, deque[float]] = {}


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
    """Validate HuggingFace token and return username if valid."""
    if not token:
        return None

    # Check cache first
    if token in token_cache:
        return token_cache[token]

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
                token_cache[token] = username
                logger.info(f"Token validated for user: {username}")
                return username
            else:
                logger.warning(f"Token validation failed: {response.status_code}")
                return None
    except Exception as e:
        logger.error(f"Error validating token: {e}")
        return None


@dataclass
class Peer:
    """Represents a connected peer (robot or client).

    ``last_seen`` is the sole input to the TTL sweeper. We refresh it
    on every signal that the peer is still wired to us:

    - POST /send received from this peer (strongest signal).
    - SSE keepalive yielded after a successful
      ``await request.is_disconnected() == False`` check (the yield
      proves the TCP stream is still flowing to the peer's socket).
    - SSE message delivered.

    We deliberately do NOT touch on internal server bookkeeping that
    has no causal link to peer reachability.
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
    # Liveness bookkeeping
    # ------------------------------------------------------------------

    def touch(self, peer_id: str) -> None:
        """Refresh ``last_seen`` on inbound application traffic.

        Called by the only code paths that prove the peer is actually
        reachable: a ``POST /send`` arriving from it, or a session
        message successfully delivered through its message queue
        (which the consumer side will ack via its own POST round-trip).

        Server-side SSE keepalive pings do NOT call this. A keepalive
        is just us writing into the local TCP send buffer and observing
        no immediate error. On a half-open connection (peer's network
        cut without a clean FIN/RST) the buffer happily absorbs writes
        for minutes, so refreshing ``last_seen`` on every keepalive
        would hold zombie peers alive forever - exactly the bug we
        had before the heartbeat protocol was introduced. Heartbeats
        are now driven by the daemon (re-emitting ``setPeerStatus``
        every ``HEARTBEAT_INTERVAL_SECONDS`` even when its meta is
        unchanged), which arrives here as a real ``POST /send`` and
        does refresh the lease.
        """
        peer = self.peers.get(peer_id)
        if peer is not None:
            peer.last_seen = time.monotonic()

    async def run_ttl_sweeper(self) -> None:
        """Background task: evict peers whose lease has expired.

        See ``LEASE_SECONDS`` and ``SWEEPER_INTERVAL_SECONDS`` at the
        top of the module. We snapshot the peer ids before iterating
        because ``disconnect_peer`` mutates ``self.peers``.
        """
        logger.info(
            "TTL sweeper running every %.1fs, lease=%.1fs",
            SWEEPER_INTERVAL_SECONDS,
            LEASE_SECONDS,
        )
        while True:
            try:
                await asyncio.sleep(SWEEPER_INTERVAL_SECONDS)
                cutoff = time.monotonic() - LEASE_SECONDS
                stale = [
                    peer_id
                    for peer_id, peer in self.peers.items()
                    if peer.last_seen < cutoff
                ]
                for peer_id in stale:
                    logger.info(
                        "Lease expired (%.1fs idle): evicting peer %s",
                        time.monotonic() - self.peers[peer_id].last_seen,
                        peer_id,
                    )
                    await self.disconnect_peer(peer_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # The sweeper is best-effort; never let one bad iteration
                # kill the loop and stop us from cleaning up future zombies.
                logger.exception("TTL sweeper iteration failed: %s", e)

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
          payload's ``meta.install_id`` collides with an existing producer
          of the same user, evict that older producer first
          (last-writer-wins, see ``docs/SIGNALING.md``). Broadcast a
          ``peerStatusChanged`` event so listeners learn about the new
          producer.

        - ``roles=["listener"]``: mark as listener, no broadcast.

        - ``roles=[]``: explicit withdraw. The peer wants to be removed
          from ``producers`` but keep its SSE channel open so it can
          re-register later. We end any active session it had, drop it
          from ``producers``, and tell other listeners (same user) so
          their UI clears the row immediately - no waiting for a TTL
          or SSE close.
        """
        roles = message.get("roles", [])
        meta = message.get("meta", {})
        peer.meta = meta

        if "producer" in roles:
            await self._evict_install_id_collisions(peer, meta)
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

    async def _evict_install_id_collisions(self, new_peer: Peer, new_meta: dict) -> None:
        """Last-writer-wins on ``meta.install_id`` collisions.

        A re-flashed daemon, a duplicated SD card, or a stale tray
        process can register a producer whose ``install_id`` matches
        an already-registered producer of the same user. Without
        eviction we'd carry both forever and the mobile app would see
        the robot twice. Policy: keep the newcomer, drop the older.

        Owner-scoped: a different HF user holding the same
        ``install_id`` (collisions are unlikely with UUID4 but
        possible during a hardware swap between two accounts) is left
        alone here - cross-tenant collisions are out of scope for the
        signaling server, the auth layer above us guarantees isolation.
        """
        new_install_id = new_meta.get("install_id")
        if not new_install_id:
            return

        for old_id, old_peer in list(self.producers.items()):
            if old_id == new_peer.peer_id:
                continue
            if old_peer.username != new_peer.username:
                continue
            if old_peer.meta.get("install_id") != new_install_id:
                continue
            logger.info(
                "install_id collision: %s already held by peer %s, evicting older",
                new_install_id,
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

    async def disconnect_peer(self, peer_id: str):
        """Fully evict a peer from every server-side structure.

        Called from three places:

        - SSE close path (``request.is_disconnected()`` becoming true).
        - The TTL sweeper, when a peer goes silent for ``LEASE_SECONDS``.
        - ``_evict_install_id_collisions``, when a duplicate registers.

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

    async def event_generator() -> AsyncGenerator[dict, None]:
        # Send welcome message with username for client info. We do
        # NOT touch ``last_seen`` here: a fresh peer was just created
        # by ``get_or_create_peer`` (which seeded ``last_seen``), and
        # touching on outbound writes is unsound liveness anyway.
        # Inbound traffic (POST /send) is the only authoritative
        # signal, and the daemon's heartbeat will produce one within
        # ``recommended_heartbeat_interval_seconds`` (see below).
        #
        # ``lease_seconds`` and ``recommended_heartbeat_interval_seconds``
        # let the daemon auto-tune its heartbeat loop so the only knob
        # operators need to turn lives here on the server. Older daemon
        # versions ignore unknown welcome fields, so this is fully
        # backwards-compatible: they keep using their hard-coded default.
        yield {
            "event": "message",
            "data": json.dumps(
                {
                    "type": "welcome",
                    "peerId": peer.peer_id,
                    "username": username,
                    "lease_seconds": LEASE_SECONDS,
                    "recommended_heartbeat_interval_seconds": _recommended_heartbeat_interval(),
                }
            ),
        }

        # Send current producers list for listeners (filtered by owner)
        yield {"event": "message", "data": json.dumps({"type": "list", "producers": signaling.get_producers_list(username)})}

        try:
            while True:
                # Check if client disconnected. This is best-effort:
                # ``is_disconnected`` returns True on FIN/RST visible
                # to starlette, but half-open sockets can stay False
                # for minutes behind HTTP/2 proxies. The TTL sweeper
                # is the authoritative cleanup path for those.
                if await request.is_disconnected():
                    break

                try:
                    # Wait for message with timeout. A queued message
                    # exists because some other peer (typically a
                    # consumer) sent us application traffic that we
                    # are forwarding here. That counts as evidence
                    # that the addressee was relevant; we refresh
                    # ``last_seen`` so an in-flight session never
                    # races the sweeper.
                    message = await asyncio.wait_for(peer.message_queue.get(), timeout=30.0)
                    signaling.touch(peer.peer_id)
                    yield {"event": "message", "data": json.dumps(message)}
                except asyncio.TimeoutError:
                    # Server-pushed keepalive. Its ONLY job is to keep
                    # the HTTP/2 proxy in front of us from killing the
                    # idle connection (HF Spaces, Cloudflare, etc.).
                    # We deliberately do NOT ``touch()`` here: a yield
                    # that doesn't raise proves nothing about the
                    # peer's reachability (the local TCP send buffer
                    # absorbs writes silently on half-open sockets).
                    # The daemon's heartbeat (every
                    # ``HEARTBEAT_INTERVAL_SECONDS``, arriving as a
                    # POST /send) is the authoritative liveness
                    # signal. See ``docs/SIGNALING.md``.
                    yield {"event": "ping", "data": ""}

        finally:
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
    # POST /send is the strongest "the peer is alive" signal we have:
    # the daemon explicitly chose to talk to us right now. Refresh the
    # lease before processing so the sweeper never preempts in-flight
    # work.
    signaling.touch(peer_id)

    body = await request.json()
    response = await signaling.handle_message(peer, body)

    return response or {"status": "ok"}


@app.get("/")
async def root():
    """Simple status page."""
    return HTMLResponse(content=f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Reachy Mini Central</title>
        <style>
            body {{ font-family: sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }}
            .status {{ padding: 10px; background: #e8f5e9; border-radius: 5px; }}
            .security {{ padding: 10px; background: #e3f2fd; border-radius: 5px; margin-top: 10px; }}
            code {{ background: #f5f5f5; padding: 2px 6px; border-radius: 3px; }}
        </style>
    </head>
    <body>
        <h1>Reachy Mini Central</h1>
        <div class="status">
            <p><strong>Status:</strong> Running</p>
            <p><strong>Connected Peers:</strong> {len([p for p in signaling.peers.values() if p.connected])}</p>
            <p><strong>Active Producers:</strong> {len(signaling.producers)}</p>
            <p><strong>Active Sessions:</strong> {len(signaling.sessions)}</p>
        </div>
        <div class="security">
            <p><strong>Security:</strong></p>
            <ul>
                <li>HuggingFace token authentication required</li>
                <li>Owner-based filtering: users only see their own robots</li>
                <li>Rate limiting: {RATE_LIMIT_REQUESTS} requests per {int(RATE_LIMIT_WINDOW)}s per peer (sliding window)</li>
            </ul>
        </div>
        <h2>Endpoints</h2>
        <ul>
            <li><code>GET /events?token=...</code> - SSE stream for receiving messages</li>
            <li><code>POST /send?token=...</code> - Send messages to server</li>
        </ul>
        <h2>Protocol</h2>
        <p>This server implements the GStreamer WebRTC signaling protocol over HTTP/SSE.</p>
    </body>
    </html>
    """)


@app.get("/health")
async def health():
    """Health check endpoint.

    Exposes the liveness parameters so external clients (and the daemon
    welcome handshake) can introspect the active configuration without
    redeploying.
    """
    return {
        "status": "healthy",
        "peers": len([p for p in signaling.peers.values() if p.connected]),
        "producers": len(signaling.producers),
        "sessions": len(signaling.sessions),
        "lease_seconds": LEASE_SECONDS,
        "sweeper_interval_seconds": SWEEPER_INTERVAL_SECONDS,
        "recommended_heartbeat_interval_seconds": _recommended_heartbeat_interval(),
    }


@app.get("/api/robot-status")
async def robot_status(token: str = Depends(_resolve_hf_token)):
    """Return busy/free status and currently-connected app for each robot owned by the caller.

    Used by clients (e.g. the desktop app) to render a passive status indicator
    without consuming a session slot. Filtered by HuggingFace username so users
    only see their own robots.

    Response shape:
        {
            "lease_seconds": 15,
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

    ``last_seen_age_seconds`` is the wall-time gap since the producer last
    sent inbound traffic (POST /send heartbeat). Clients can gate their UI
    "reachable" state on ``age < lease_seconds * 0.6`` for a tighter
    freshness check than the server-side sweeper window. ``lease_seconds``
    is mirrored at the top level so clients don't need to query
    ``/health`` separately.
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

    return {"lease_seconds": LEASE_SECONDS, "robots": robots}


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
            "lease_seconds": 15,
            "sweeper_interval_seconds": 3,
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
        "lease_seconds": LEASE_SECONDS,
        "sweeper_interval_seconds": SWEEPER_INTERVAL_SECONDS,
        "now": round(now, 2),
        "peers": peers,
    }
