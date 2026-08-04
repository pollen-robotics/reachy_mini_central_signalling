"""Tests for the signaling-server lifecycle behaviour.

Covers:

- ``setPeerStatus(roles=[])`` removes the peer from ``producers`` but
  keeps its SSE channel open and broadcasts ``peerStatusChanged``.
- ``setPeerStatus(roles=["producer"])`` after a withdraw flips the
  daemon back into ``producers`` (the "I'm available again" path the
  daemon-driven liveness design relies on).
- ``install_id`` collisions between two producers of the same user
  evict the older.
- ``disconnect_peer`` clears every server-side structure (no leak).
- Per-peer sliding-window rate limiting.

Central no longer runs a TTL sweeper; session liveness is owned by the
daemon (see module docstring of ``app.py``). The corresponding tests
have been removed.

Run with::

    pip install pytest pytest-asyncio httpx fastapi sse-starlette
    python -m pytest test_signaling.py -v
"""

from __future__ import annotations

import time
from collections import deque

import pytest

from app import (
    PRODUCER_LEASE_SECONDS,
    RATE_LIMIT_REQUESTS,
    RATE_LIMIT_WINDOW,
    TOKEN_CACHE_STALE_GRACE_SECONDS,
    TOKEN_CACHE_TTL_SECONDS,
    Peer,
    SignalingServer,
    _prune_rate_limit_buckets,
    _prune_token_cache,
    _rate_limit_buckets,
    _rate_limit_key,
    check_rate_limit,
    token_cache,
    validate_hf_token,
)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _make_server() -> SignalingServer:
    return SignalingServer()


_token_counter = 0


def _make_peer(server: SignalingServer, username: str = "alice") -> Peer:
    """Register a fresh peer + token mapping (mimics the SSE handshake).

    Each call mints a unique token; ``get_or_create_peer`` therefore
    creates a brand new ``Peer`` rather than rebinding to the previous
    one - the latter would silently make ``old`` and ``new`` the same
    object in the install_id collision tests.
    """
    global _token_counter
    _token_counter += 1
    return server.get_or_create_peer(
        token=f"tok-{username}-{_token_counter}", username=username
    )


# ----------------------------------------------------------------------
# setPeerStatus(roles=[]) - explicit withdraw
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_withdraw_removes_from_producers_but_keeps_peer_alive():
    server = _make_server()
    p = _make_peer(server)
    await server.handle_set_peer_status(
        p, {"roles": ["producer"], "meta": {"name": "r"}}
    )
    assert p.peer_id in server.producers
    assert p.peer_id in server.peers

    await server.handle_set_peer_status(p, {"roles": [], "meta": {"name": "r"}})
    assert p.peer_id not in server.producers
    assert p.peer_id in server.peers, "withdraw must keep SSE peer alive"
    assert p.role is None


@pytest.mark.asyncio
async def test_withdraw_broadcasts_to_same_user_listeners():
    server = _make_server()
    producer = _make_peer(server, username="alice")
    listener = _make_peer(server, username="alice")
    other_user = _make_peer(server, username="bob")

    await server.handle_set_peer_status(
        producer, {"roles": ["producer"], "meta": {"name": "r"}}
    )
    # Drain initial registration broadcast.
    while not listener.message_queue.empty():
        listener.message_queue.get_nowait()

    broadcast = await server.handle_set_peer_status(
        producer, {"roles": [], "meta": {"name": "r"}}
    )
    assert broadcast is not None
    assert broadcast["roles"] == []
    # Caller forwards via broadcast_to_listeners; simulate that step.
    await server.broadcast_to_listeners(
        broadcast, exclude_id=producer.peer_id, owner_username=producer.username
    )

    msg = listener.message_queue.get_nowait()
    assert msg["type"] == "peerStatusChanged"
    assert msg["roles"] == []
    assert other_user.message_queue.empty(), "must not leak across users"


@pytest.mark.asyncio
async def test_withdraw_when_not_a_producer_is_a_noop():
    server = _make_server()
    p = _make_peer(server)
    out = await server.handle_set_peer_status(p, {"roles": [], "meta": {}})
    assert out is None
    assert p.peer_id in server.peers


# ----------------------------------------------------------------------
# Daemon "I'm available again" path
# ----------------------------------------------------------------------
#
# In the new design, the daemon owns session liveness: when its
# watchdog detects an idle peer it tears down the PC, sends
# ``endSession``, and re-registers via ``setPeerStatus(producer)``.
# Central must accept that re-registration cleanly and flip the peer
# back into ``producers``.


@pytest.mark.asyncio
async def test_reregister_after_withdraw_restores_producer_slot():
    server = _make_server()
    p = _make_peer(server)
    await server.handle_set_peer_status(
        p, {"roles": ["producer"], "meta": {"name": "r", "install_id": "iid-1"}}
    )
    assert p.peer_id in server.producers

    # Daemon withdraws (e.g. transient backend hiccup).
    await server.handle_set_peer_status(
        p, {"roles": [], "meta": {"name": "r", "install_id": "iid-1"}}
    )
    assert p.peer_id not in server.producers

    # Daemon recovers and re-announces availability.
    broadcast = await server.handle_set_peer_status(
        p, {"roles": ["producer"], "meta": {"name": "r", "install_id": "iid-1"}}
    )
    assert p.peer_id in server.producers
    assert p.role == "producer"
    assert broadcast is not None
    assert broadcast["type"] == "peerStatusChanged"
    assert broadcast["roles"] == ["producer"]


@pytest.mark.asyncio
async def test_reregister_after_endsession_clears_session_state():
    """After the daemon sends ``endSession`` and re-registers, the
    producer slot must be present AND session_id/partner_id must be
    clear so the next consumer can start a fresh session.
    """
    server = _make_server()
    producer = _make_peer(server, username="alice")
    consumer = _make_peer(server, username="alice")
    await server.handle_set_peer_status(
        producer, {"roles": ["producer"], "meta": {"name": "r", "install_id": "iid-2"}}
    )
    await server.handle_start_session(consumer, {"peerId": producer.peer_id})
    session_id = producer.session_id
    assert session_id is not None

    # Daemon tears down the session locally and tells central.
    await server.handle_end_session(session_id, reason="idle_timeout")
    assert producer.session_id is None
    assert producer.partner_id is None
    # Producer is still in the registry (endSession does not unregister).
    assert producer.peer_id in server.producers

    # Daemon re-announces availability; same install_id, same peer_id.
    await server.handle_set_peer_status(
        producer, {"roles": ["producer"], "meta": {"name": "r", "install_id": "iid-2"}}
    )
    assert producer.peer_id in server.producers
    assert producer.session_id is None


# ----------------------------------------------------------------------
# install_id collision -> last writer wins
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_install_id_collision_evicts_old_producer():
    server = _make_server()
    old = _make_peer(server, username="alice")
    new = _make_peer(server, username="alice")
    install_id = "x" * 32

    await server.handle_set_peer_status(
        old, {"roles": ["producer"], "meta": {"install_id": install_id, "name": "r"}}
    )
    await server.handle_set_peer_status(
        new, {"roles": ["producer"], "meta": {"install_id": install_id, "name": "r"}}
    )
    assert new.peer_id in server.producers
    assert old.peer_id not in server.producers
    assert old.peer_id not in server.peers, "old peer must be fully evicted, not just demoted"


@pytest.mark.asyncio
async def test_install_id_collision_does_not_cross_users():
    """Different HF users hitting the same install_id (rare hardware swap)
    must NOT evict each other - that is handled at the auth layer."""
    server = _make_server()
    alice = _make_peer(server, username="alice")
    bob = _make_peer(server, username="bob")
    install_id = "y" * 32

    await server.handle_set_peer_status(
        alice, {"roles": ["producer"], "meta": {"install_id": install_id}}
    )
    await server.handle_set_peer_status(
        bob, {"roles": ["producer"], "meta": {"install_id": install_id}}
    )
    assert alice.peer_id in server.producers
    assert bob.peer_id in server.producers


@pytest.mark.asyncio
async def test_install_id_collision_ends_old_session():
    server = _make_server()
    old = _make_peer(server, username="alice")
    new = _make_peer(server, username="alice")
    consumer = _make_peer(server, username="alice")

    await server.handle_set_peer_status(
        old, {"roles": ["producer"], "meta": {"install_id": "z" * 32}}
    )
    # Hand-craft an active session on the old producer so we can
    # observe the cleanup path running.
    await server.handle_start_session(consumer, {"peerId": old.peer_id})
    assert old.session_id is not None

    await server.handle_set_peer_status(
        new, {"roles": ["producer"], "meta": {"install_id": "z" * 32}}
    )
    assert old.session_id is None
    # Consumer received an endSession with the takeover reason.
    seen_takeover = False
    while not consumer.message_queue.empty():
        msg = consumer.message_queue.get_nowait()
        if msg.get("type") == "endSession" and msg.get("reason") == "install_id_takeover":
            seen_takeover = True
    assert seen_takeover


# ----------------------------------------------------------------------
# sessionStateChanged broadcast (busy/free transitions)
# ----------------------------------------------------------------------
#
# A second device of the same HF user (typically the mobile app while
# the desktop already holds a session) must learn about a busy/free
# transition without waiting on its 30 s ``/api/robot-status`` poll.
# Central pushes ``sessionStateChanged`` to every same-owner listener
# whenever a session starts or ends. Cross-tenant leakage is the
# regression that would matter most here, so each test explicitly
# checks that an unrelated user's queue stays empty.


def _drain(peer: Peer) -> list[dict]:
    """Pop everything currently queued on the peer, return as a list.

    Tests that just registered a producer have already filled their
    listener's queue with the registration broadcast; pulling them
    here keeps the assertions on the busy/free events from depending
    on registration frame indices.
    """
    out = []
    while not peer.message_queue.empty():
        out.append(peer.message_queue.get_nowait())
    return out


@pytest.mark.asyncio
async def test_start_session_broadcasts_busy_to_same_user_listeners():
    server = _make_server()
    producer = _make_peer(server, username="alice")
    consumer = _make_peer(server, username="alice")
    listener = _make_peer(server, username="alice")
    other_user = _make_peer(server, username="bob")

    await server.handle_set_peer_status(
        producer, {"roles": ["producer"], "meta": {"name": "reachy_mini"}}
    )
    consumer.meta = {"name": "Conversation App"}
    _drain(listener)

    await server.handle_start_session(consumer, {"peerId": producer.peer_id})

    msgs = _drain(listener)
    busy_msgs = [m for m in msgs if m.get("type") == "sessionStateChanged"]
    assert len(busy_msgs) == 1
    assert busy_msgs[0]["busy"] is True
    assert busy_msgs[0]["activeApp"] == "Conversation App"
    assert busy_msgs[0]["peerId"] == producer.peer_id
    assert other_user.message_queue.empty(), "must not leak across users"


@pytest.mark.asyncio
async def test_start_session_does_not_echo_to_acquirer():
    """The consumer that just started the session already knows about
    it via the ``sessionStarted`` reply. Echoing a
    ``sessionStateChanged`` to the same socket would make every UI
    react twice to its own action.
    """
    server = _make_server()
    producer = _make_peer(server, username="alice")
    consumer = _make_peer(server, username="alice")

    await server.handle_set_peer_status(
        producer, {"roles": ["producer"], "meta": {"name": "r"}}
    )
    _drain(consumer)

    await server.handle_start_session(consumer, {"peerId": producer.peer_id})

    msgs = _drain(consumer)
    assert all(
        m.get("type") != "sessionStateChanged" for m in msgs
    ), "consumer should not receive the same-owner broadcast for its own action"


@pytest.mark.asyncio
async def test_end_session_broadcasts_free_to_same_user_listeners():
    server = _make_server()
    producer = _make_peer(server, username="alice")
    consumer = _make_peer(server, username="alice")
    listener = _make_peer(server, username="alice")
    other_user = _make_peer(server, username="bob")

    await server.handle_set_peer_status(
        producer, {"roles": ["producer"], "meta": {"name": "r"}}
    )
    await server.handle_start_session(consumer, {"peerId": producer.peer_id})
    _drain(listener)

    assert producer.session_id is not None
    await server.handle_end_session(producer.session_id, reason="user_stopped")

    msgs = _drain(listener)
    free_msgs = [m for m in msgs if m.get("type") == "sessionStateChanged"]
    assert len(free_msgs) == 1
    assert free_msgs[0]["busy"] is False
    assert free_msgs[0]["activeApp"] is None
    assert free_msgs[0]["peerId"] == producer.peer_id
    assert other_user.message_queue.empty(), "must not leak across users"


@pytest.mark.asyncio
async def test_end_session_when_session_unknown_is_a_noop():
    """A spurious ``endSession`` for a vanished session id (race after
    a TTL eviction, double-tap from a flaky client) must not raise
    and must not emit a broadcast.
    """
    server = _make_server()
    listener = _make_peer(server, username="alice")
    await server.handle_end_session("ghost-session", reason="ghost")
    assert listener.message_queue.empty()


@pytest.mark.asyncio
async def test_session_state_broadcast_skips_when_owner_unresolvable():
    """Anti-leak: if both peers are gone before teardown, central
    cannot resolve the owner, so it MUST skip the broadcast instead
    of falling back to a global emit (cross-tenant leak).
    """
    server = _make_server()
    producer = _make_peer(server, username="alice")
    consumer = _make_peer(server, username="alice")
    bob_listener = _make_peer(server, username="bob")

    await server.handle_set_peer_status(
        producer, {"roles": ["producer"], "meta": {"name": "r"}}
    )
    await server.handle_start_session(consumer, {"peerId": producer.peer_id})
    session_id = producer.session_id
    _drain(bob_listener)

    # Bypass disconnect_peer (which itself calls handle_end_session)
    # to exercise the "session still in self.sessions but both
    # peers gone" race directly.
    del server.peers[producer.peer_id]
    del server.peers[consumer.peer_id]
    await server.handle_end_session(session_id, reason="vanished")

    assert bob_listener.message_queue.empty()


# ----------------------------------------------------------------------
# get_producers_list shape (initial SSE list frame for new listeners)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_producers_list_includes_busy_state_for_active_session():
    """A listener connecting mid-session must see ``busy=True`` on
    its initial ``list`` frame, otherwise the UI starts in a stale
    "free" state and only flips on the next start/end transition.
    """
    server = _make_server()
    producer = _make_peer(server, username="alice")
    consumer = _make_peer(server, username="alice")
    consumer.meta = {"name": "Hand Tracker Live"}

    await server.handle_set_peer_status(
        producer, {"roles": ["producer"], "meta": {"name": "r"}}
    )
    await server.handle_start_session(consumer, {"peerId": producer.peer_id})

    rows = server.get_producers_list("alice")
    assert len(rows) == 1
    assert rows[0]["id"] == producer.peer_id
    assert rows[0]["busy"] is True
    assert rows[0]["activeApp"] == "Hand Tracker Live"


@pytest.mark.asyncio
async def test_producers_list_reports_free_when_no_session():
    server = _make_server()
    producer = _make_peer(server, username="alice")
    await server.handle_set_peer_status(
        producer, {"roles": ["producer"], "meta": {"name": "r"}}
    )

    rows = server.get_producers_list("alice")
    assert len(rows) == 1
    assert rows[0]["busy"] is False
    assert rows[0]["activeApp"] is None


@pytest.mark.asyncio
async def test_producers_list_filters_by_owner():
    """Anti-leak guard at the ``get_producers_list`` level: a future
    refactor that bypasses the broadcast path can't quietly leak
    cross-tenant rows on the initial SSE list frame.
    """
    server = _make_server()
    alice_robot = _make_peer(server, username="alice")
    bob_robot = _make_peer(server, username="bob")
    for p in (alice_robot, bob_robot):
        await server.handle_set_peer_status(
            p, {"roles": ["producer"], "meta": {"name": f"{p.username}-r"}}
        )

    assert {row["id"] for row in server.get_producers_list("alice")} == {
        alice_robot.peer_id
    }
    assert {row["id"] for row in server.get_producers_list("bob")} == {
        bob_robot.peer_id
    }


# ----------------------------------------------------------------------
# producer_session_snapshot helper (single source of truth)
# ----------------------------------------------------------------------
#
# The helper backs three call sites at once: get_producers_list, the
# /api/robot-status REST view, and the busy/free SSE broadcasts. A
# direct test pins its contract so a future tweak (different
# fallback for activeApp, new field in the tuple, etc.) is caught at
# the helper level instead of cascading into integration-style
# regressions across all three.


@pytest.mark.asyncio
async def test_producer_session_snapshot_free_when_no_session():
    server = _make_server()
    p = _make_peer(server)
    await server.handle_set_peer_status(
        p, {"roles": ["producer"], "meta": {"name": "r"}}
    )
    assert server.producer_session_snapshot(p) == (False, None)


@pytest.mark.asyncio
async def test_producer_session_snapshot_busy_carries_consumer_app_name():
    server = _make_server()
    producer = _make_peer(server, username="alice")
    consumer = _make_peer(server, username="alice")
    consumer.meta = {"name": "Hand Tracker Live"}
    await server.handle_set_peer_status(
        producer, {"roles": ["producer"], "meta": {"name": "r"}}
    )
    await server.handle_start_session(consumer, {"peerId": producer.peer_id})

    assert server.producer_session_snapshot(producer) == (True, "Hand Tracker Live")


@pytest.mark.asyncio
async def test_producer_session_snapshot_busy_with_no_partner_yields_no_app():
    """``activeApp`` falls back to None when the partner has been
    evicted mid-session (race) or never advertised a meta.name.
    The producer is still ``busy`` because its session is still
    wired - clients should render "in use" without the app label.
    """
    server = _make_server()
    producer = _make_peer(server, username="alice")
    consumer = _make_peer(server, username="alice")
    await server.handle_set_peer_status(
        producer, {"roles": ["producer"], "meta": {"name": "r"}}
    )
    await server.handle_start_session(consumer, {"peerId": producer.peer_id})
    del server.peers[consumer.peer_id]

    assert server.producer_session_snapshot(producer) == (True, None)


# ----------------------------------------------------------------------
# disconnect_peer cleanup invariants
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disconnect_peer_clears_all_structures():
    server = _make_server()
    token = "tok-disconnect-test"
    p = server.get_or_create_peer(token=token, username="alice")
    await server.handle_set_peer_status(
        p, {"roles": ["producer"], "meta": {"name": "r"}}
    )

    await server.disconnect_peer(p.peer_id)

    assert p.peer_id not in server.peers
    assert p.peer_id not in server.producers
    assert token not in server.token_to_peer


@pytest.mark.asyncio
async def test_disconnect_peer_idempotent():
    server = _make_server()
    p = _make_peer(server)
    await server.disconnect_peer(p.peer_id)
    # Second call must not raise on a missing peer.
    await server.disconnect_peer(p.peer_id)


# ----------------------------------------------------------------------
# Rate limiting (per-peer, sliding window)
# ----------------------------------------------------------------------


@pytest.fixture(autouse=False)
def _clean_rate_limit_buckets():
    """Snapshot and restore the global bucket store around a test.

    Tests in this section exercise process-wide state, so we isolate
    them from each other and from anything that ran earlier in the
    session.
    """
    snapshot = {k: v.copy() for k, v in _rate_limit_buckets.items()}
    _rate_limit_buckets.clear()
    try:
        yield
    finally:
        _rate_limit_buckets.clear()
        _rate_limit_buckets.update(snapshot)


def test_rate_limit_key_is_stable_and_non_reversible(_clean_rate_limit_buckets):
    """Same token -> same bucket key (deterministic), different tokens
    -> different keys (no collisions at this scale).
    """
    a = _rate_limit_key("token-alice-1")
    b = _rate_limit_key("token-alice-1")
    c = _rate_limit_key("token-alice-2")
    assert a == b
    assert a != c
    assert "token" not in a, "raw token must not be embedded in the key"
    assert len(a) == 16


def test_rate_limit_allows_requests_under_threshold(_clean_rate_limit_buckets):
    token = "tok-under"
    for _ in range(10):
        assert check_rate_limit(token) is True


def test_rate_limit_blocks_requests_over_threshold(_clean_rate_limit_buckets):
    """Once the window is full, the next call returns False - the caller
    is expected to translate that into a 429.
    """
    token = "tok-over"
    for _ in range(RATE_LIMIT_REQUESTS):
        assert check_rate_limit(token) is True
    assert check_rate_limit(token) is False


def test_rate_limit_isolates_peers_under_same_user(_clean_rate_limit_buckets):
    """Two daemons under the same HF account (different tokens) get
    independent buckets. This is the regression test for the original
    incident: heartbeat traffic from one robot must not starve the
    other.
    """
    saturated_token = "tok-busy"
    quiet_token = "tok-quiet"
    for _ in range(RATE_LIMIT_REQUESTS):
        assert check_rate_limit(saturated_token) is True
    assert check_rate_limit(saturated_token) is False
    assert check_rate_limit(quiet_token) is True


def test_rate_limit_sliding_window_drops_aged_timestamps(
    _clean_rate_limit_buckets,
):
    """A timestamp older than the window must roll out without waiting
    for a counter reset. The deque-based implementation enforces this
    naturally; this test guards against a regression to a fixed-window
    counter.
    """
    token = "tok-slide"
    bucket = _rate_limit_buckets.setdefault(_rate_limit_key(token), deque())
    bucket.extend([time.monotonic() - RATE_LIMIT_WINDOW - 1.0] * RATE_LIMIT_REQUESTS)
    assert len(bucket) == RATE_LIMIT_REQUESTS

    assert check_rate_limit(token) is True
    assert len(bucket) == 1, "stale timestamps must be evicted before the new one is appended"


def test_rate_limit_records_each_allowed_request(_clean_rate_limit_buckets):
    """The bucket length must equal the number of allowed calls so far,
    so capacity planning is straightforward.
    """
    token = "tok-count"
    for i in range(1, 6):
        assert check_rate_limit(token) is True
        assert len(_rate_limit_buckets[_rate_limit_key(token)]) == i


def test_rate_limit_default_capacity_matches_industry_baseline():
    """Sanity bound on the configured capacity: at least 600 req/min
    (~10 req/s) per peer so heartbeats + signaling + reconnects fit
    comfortably without retuning.
    """
    assert RATE_LIMIT_REQUESTS >= 600
    assert RATE_LIMIT_WINDOW <= 60.0


def test_prune_rate_limit_buckets_drops_aged_and_empty(
    _clean_rate_limit_buckets,
):
    """The sweeper-side prune must reclaim buckets of departed peers
    (all timestamps aged out) and empty leftovers, but never an active
    bucket.
    """
    now = time.monotonic()
    _rate_limit_buckets[_rate_limit_key("tok-dead")] = deque(
        [now - RATE_LIMIT_WINDOW - 5.0]
    )
    _rate_limit_buckets[_rate_limit_key("tok-empty")] = deque()
    _rate_limit_buckets[_rate_limit_key("tok-live")] = deque([now])

    _prune_rate_limit_buckets()

    assert _rate_limit_key("tok-dead") not in _rate_limit_buckets
    assert _rate_limit_key("tok-empty") not in _rate_limit_buckets
    assert _rate_limit_key("tok-live") in _rate_limit_buckets


# ----------------------------------------------------------------------
# Liveness: last_seen refresh + stale-producer sweep
# ----------------------------------------------------------------------


HEARTBEATING_META = {"name": "r", "transport": "wifi", "hardware_id": "cafe1234"}
LEGACY_META = {"name": "old-r", "transport": "wifi"}  # pre-v1.7.2: no hardware_id


@pytest.mark.asyncio
async def test_inbound_message_refreshes_last_seen():
    """Any POST /send traffic (here: a heartbeat setPeerStatus routed
    through handle_message) must refresh last_seen - this is the signal
    the sweep keys on, and what makes last_seen_age_seconds an honest
    liveness metric for clients.
    """
    server = _make_server()
    p = _make_peer(server)
    p.last_seen = time.monotonic() - 1000.0

    await server.handle_message(
        p, {"type": "setPeerStatus", "roles": ["producer"], "meta": HEARTBEATING_META}
    )

    assert time.monotonic() - p.last_seen < 1.0


@pytest.mark.asyncio
async def test_sweep_evicts_stale_heartbeating_producer():
    """A heartbeat-capable producer silent for a full lease is a
    half-open socket: full eviction, and same-user listeners are told.
    """
    server = _make_server()
    ghost = server.get_or_create_peer(token="tok-ghost", username="alice")
    listener = _make_peer(server, username="alice")
    await server.handle_message(
        ghost,
        {"type": "setPeerStatus", "roles": ["producer"], "meta": HEARTBEATING_META},
    )
    while not listener.message_queue.empty():
        listener.message_queue.get_nowait()

    ghost.last_seen = time.monotonic() - PRODUCER_LEASE_SECONDS - 1.0
    evicted = await server.sweep_stale_producers()

    assert evicted == [ghost.peer_id]
    assert ghost.peer_id not in server.producers
    assert ghost.peer_id not in server.peers
    assert "tok-ghost" not in server.token_to_peer

    msg = listener.message_queue.get_nowait()
    assert msg["type"] == "peerStatusChanged"
    assert msg["roles"] == []


@pytest.mark.asyncio
async def test_sweep_spares_legacy_producer_without_hardware_id():
    """Daemons < v1.7.2 never heartbeat and cannot self-heal after an
    eviction (no producer health loop). They must keep today's
    behaviour verbatim: never swept, however stale.
    """
    server = _make_server()
    legacy = _make_peer(server)
    await server.handle_message(
        legacy, {"type": "setPeerStatus", "roles": ["producer"], "meta": LEGACY_META}
    )
    legacy.last_seen = time.monotonic() - 10 * PRODUCER_LEASE_SECONDS

    evicted = await server.sweep_stale_producers()

    assert evicted == []
    assert legacy.peer_id in server.producers


@pytest.mark.asyncio
async def test_sweep_spares_fresh_producer():
    server = _make_server()
    fresh = _make_peer(server)
    await server.handle_message(
        fresh,
        {"type": "setPeerStatus", "roles": ["producer"], "meta": HEARTBEATING_META},
    )

    evicted = await server.sweep_stale_producers()

    assert evicted == []
    assert fresh.peer_id in server.producers


@pytest.mark.asyncio
async def test_sweep_ends_ghost_session_and_notifies_consumer():
    """Sweeping a producer that died mid-session must free the session
    slot and push endSession to the surviving consumer.
    """
    server = _make_server()
    producer = _make_peer(server, username="alice")
    consumer = _make_peer(server, username="alice")
    await server.handle_message(
        producer,
        {"type": "setPeerStatus", "roles": ["producer"], "meta": HEARTBEATING_META},
    )
    response = await server.handle_start_session(
        consumer, {"peerId": producer.peer_id}
    )
    assert response["type"] == "sessionStarted"
    while not consumer.message_queue.empty():
        consumer.message_queue.get_nowait()

    producer.last_seen = time.monotonic() - PRODUCER_LEASE_SECONDS - 1.0
    await server.sweep_stale_producers()

    assert not server.sessions
    assert consumer.session_id is None
    types = []
    while not consumer.message_queue.empty():
        types.append(consumer.message_queue.get_nowait()["type"])
    assert "endSession" in types


# ----------------------------------------------------------------------
# Stable-id collisions: hardware_id joins install_id
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hardware_id_collision_evicts_older_producer():
    """A robot re-provisioned with a fresh token registers under a new
    peer while its old registration lingers (half-open ghost).
    hardware_id - shipped by every daemon >= v1.7.2 - must trigger the
    last-writer-wins eviction that install_id (not emitted by any
    shipped daemon) was designed for.
    """
    server = _make_server()
    old = _make_peer(server, username="alice")
    await server.handle_set_peer_status(
        old, {"roles": ["producer"], "meta": {"name": "r", "hardware_id": "cafe1234"}}
    )

    new = _make_peer(server, username="alice")
    await server.handle_set_peer_status(
        new, {"roles": ["producer"], "meta": {"name": "r", "hardware_id": "cafe1234"}}
    )

    assert old.peer_id not in server.producers
    assert old.peer_id not in server.peers
    assert new.peer_id in server.producers


@pytest.mark.asyncio
async def test_hardware_id_collision_scoped_to_owner():
    server = _make_server()
    alices = _make_peer(server, username="alice")
    await server.handle_set_peer_status(
        alices, {"roles": ["producer"], "meta": {"name": "r", "hardware_id": "cafe1234"}}
    )

    bobs = _make_peer(server, username="bob")
    await server.handle_set_peer_status(
        bobs, {"roles": ["producer"], "meta": {"name": "r", "hardware_id": "cafe1234"}}
    )

    assert alices.peer_id in server.producers, "cross-tenant eviction is forbidden"
    assert bobs.peer_id in server.producers


# ----------------------------------------------------------------------
# Token cache: TTL, serve-stale-on-error, explicit-rejection drop
# ----------------------------------------------------------------------


@pytest.fixture
def _clean_token_cache():
    snapshot = dict(token_cache)
    token_cache.clear()
    try:
        yield
    finally:
        token_cache.clear()
        token_cache.update(snapshot)


class _FakeResponse:
    def __init__(self, status_code: int, name: str = ""):
        self.status_code = status_code
        self._name = name

    def json(self):
        return {"name": self._name}


class _FakeAsyncClient:
    """Stands in for httpx.AsyncClient; behaviour set per test."""

    response: _FakeResponse | None = None
    error: Exception | None = None
    calls: int = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, *args, **kwargs):
        type(self).calls += 1
        if type(self).error is not None:
            raise type(self).error
        return type(self).response


@pytest.fixture
def _fake_whoami(monkeypatch):
    import httpx

    _FakeAsyncClient.response = None
    _FakeAsyncClient.error = None
    _FakeAsyncClient.calls = 0
    monkeypatch.setattr(httpx, "AsyncClient", _FakeAsyncClient)
    return _FakeAsyncClient


@pytest.mark.asyncio
async def test_fresh_cache_hit_skips_network(_clean_token_cache, _fake_whoami):
    token_cache["tok"] = ("alice", time.monotonic() + TOKEN_CACHE_TTL_SECONDS)
    assert await validate_hf_token("tok") == "alice"
    assert _fake_whoami.calls == 0


@pytest.mark.asyncio
async def test_expired_entry_revalidates_and_refreshes_ttl(
    _clean_token_cache, _fake_whoami
):
    token_cache["tok"] = ("alice", time.monotonic() - 1.0)
    _fake_whoami.response = _FakeResponse(200, "alice")

    assert await validate_hf_token("tok") == "alice"
    assert _fake_whoami.calls == 1
    assert token_cache["tok"][1] > time.monotonic(), "TTL must be refreshed"


@pytest.mark.asyncio
async def test_revoked_token_is_dropped_on_explicit_rejection(
    _clean_token_cache, _fake_whoami
):
    """The point of the TTL: a token revoked on HF must stop working
    within one TTL, not survive until the next Space restart.
    """
    token_cache["tok"] = ("alice", time.monotonic() - 1.0)
    _fake_whoami.response = _FakeResponse(401)

    assert await validate_hf_token("tok") is None
    assert "tok" not in token_cache


@pytest.mark.asyncio
async def test_whoami_429_is_not_treated_as_revocation(
    _clean_token_cache, _fake_whoami
):
    """HF rate-limiting our whoami calls (attacker-induced or organic)
    must NOT log users out: only 401/403 are revocations, anything
    else degrades to the stale identity.
    """
    token_cache["tok"] = ("alice", time.monotonic() - 1.0)
    _fake_whoami.response = _FakeResponse(429)

    assert await validate_hf_token("tok") == "alice"
    assert "tok" in token_cache, "entry must survive an HF 429"


@pytest.mark.asyncio
async def test_whoami_5xx_is_not_treated_as_revocation(
    _clean_token_cache, _fake_whoami
):
    token_cache["tok"] = ("alice", time.monotonic() - 1.0)
    _fake_whoami.response = _FakeResponse(503)

    assert await validate_hf_token("tok") == "alice"
    assert "tok" in token_cache


@pytest.mark.asyncio
async def test_stale_entry_served_during_validation_outage(
    _clean_token_cache, _fake_whoami
):
    """A whoami outage must degrade to stale identities, not 401 the
    whole fleet.
    """
    token_cache["tok"] = ("alice", time.monotonic() - 1.0)
    _fake_whoami.error = ConnectionError("whoami down")

    assert await validate_hf_token("tok") == "alice"
    assert "tok" in token_cache, "stale entry must survive for the next retry"


@pytest.mark.asyncio
async def test_unknown_token_fails_closed_during_outage(
    _clean_token_cache, _fake_whoami
):
    _fake_whoami.error = ConnectionError("whoami down")
    assert await validate_hf_token("never-seen") is None


def test_prune_token_cache_drops_only_beyond_grace(_clean_token_cache):
    now = time.monotonic()
    token_cache["tok-ancient"] = ("a", now - TOKEN_CACHE_STALE_GRACE_SECONDS - 1.0)
    token_cache["tok-stale"] = ("b", now - 10.0)  # expired but within grace
    token_cache["tok-fresh"] = ("c", now + TOKEN_CACHE_TTL_SECONDS)

    _prune_token_cache()

    assert "tok-ancient" not in token_cache
    assert "tok-stale" in token_cache
    assert "tok-fresh" in token_cache
