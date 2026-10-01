"""SSE reconnect grace: detach / reattach / expiry.

When a peer's current SSE stream closes it is *detached*, not evicted:
it stays registered (listed, counted) for ``sse_grace_seconds``; a
reconnect on the same token resumes the same Peer (same peerId) and
receives what was queued meanwhile; otherwise the sweeper evicts it
through ``disconnect_peer`` (end cause ``peer_disconnected``).

Session rule under test: a detaching producer's session ends at detach
(the daemon relay drops its local sessions whenever its SSE drops), a
detaching consumer's session survives the grace.

Unit tests drive ``SignalingServer`` with an injected clock; route tests
go through the real ``/events`` generator and the HTTP endpoints.

Run with::

    python -m pytest test_sse_grace.py -v
"""

from __future__ import annotations

import logging
import time

import pytest
from fastapi.testclient import TestClient

import app as app_module
import test_signaling
from app import (
    PRODUCER_LEASE_SECONDS,
    TOKEN_CACHE_TTL_SECONDS,
    Peer,
    SignalingServer,
    _rate_limit_buckets,
    token_cache,
)
from fleet_usage import (
    SESSION_END_ENDED,
    SESSION_END_PEER_DISCONNECTED,
    SESSION_END_CONSUMER_REPLACED,
    UsageTracker,
)

GRACE = 15.0
MINI = {"name": "mini1", "transport": "wifi", "hardware_id": "hw-1"}
MINI2 = {"name": "mini2", "transport": "wifi", "hardware_id": "hw-2"}


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


class Clock:
    """Monotonic clock under test control."""

    def __init__(self, now: float = 1000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class UsageRecorder:
    """Records every fleet usage hook call as ``(event, args)``."""

    def __init__(self):
        self.calls: list[tuple] = []

    def __getattr__(self, event):
        def record(*args):
            self.calls.append((event, args))

        return record

    def events(self, *names: str) -> list[tuple]:
        return [c for c in self.calls if c[0] in names]


def _server(clock: Clock, usage=None, **kwargs) -> SignalingServer:
    kwargs.setdefault("sse_grace_seconds", GRACE)
    return SignalingServer(usage=usage, clock=clock, **kwargs)


def _connect(server: SignalingServer, token: str, username: str = "alice") -> Peer:
    """What ``GET /events`` does before streaming: bind the token, attach."""
    peer = server.get_or_create_peer(token, username)
    server.attach_sse(peer)
    server.sse_stream_started(peer)
    return peer


async def _producer(server, token, username="alice", meta=MINI) -> Peer:
    peer = _connect(server, token, username)
    await server.handle_message(
        peer, {"type": "setPeerStatus", "roles": ["producer"], "meta": dict(meta)}
    )
    return peer


async def _listener(server, token, username="alice", name="app") -> Peer:
    peer = _connect(server, token, username)
    await server.handle_message(
        peer, {"type": "setPeerStatus", "roles": ["listener"], "meta": {"name": name}}
    )
    return peer


async def _start(server, consumer, producer) -> str:
    resp = await server.handle_message(
        consumer, {"type": "startSession", "peerId": producer.peer_id}
    )
    assert resp["type"] == "sessionStarted", resp
    return resp["sessionId"]


def _drain(peer: Peer) -> list[dict]:
    out = []
    while not peer.message_queue.empty():
        out.append(peer.message_queue.get_nowait())
    return out


def _removals(msgs: list[dict]) -> list[dict]:
    return [m for m in msgs if m["type"] == "peerStatusChanged" and m["roles"] == []]


# ----------------------------------------------------------------------
# Detach: still registered, listed, counted; nothing broadcast
# ----------------------------------------------------------------------


async def test_detach_keeps_producer_registered_listed_and_counted():
    clock = Clock()
    usage = UsageRecorder()
    server = _server(clock, usage)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    _drain(phone)
    usage.calls.clear()

    await server.detach_peer(robot.peer_id)

    assert robot.detached_at == clock.now
    assert server.peers[robot.peer_id] is robot
    assert server.producers[robot.peer_id] is robot
    assert server.token_to_peer["tok-robot"] == robot.peer_id
    assert [p["id"] for p in server.get_producers_list("alice")] == [robot.peer_id]
    assert server.count_connected_producers() == 1
    assert server.count_connected_peers() == 2
    assert server.count_connected_producers_by_kind()["reachy_mini"] == 1
    assert _drain(phone) == []  # no removal broadcast
    assert usage.calls == []  # no producer_gone / session_ended
    assert server.sse_health() == {
        "grace_seconds": GRACE,
        "detached_now": 1,
        "detach_total": 1,
        "reattach_total": 0,
        "grace_expired_total": 0,
        "reattach_latency_s_max": 0.0,
        "sessions_ended_at_detach_total": 0,
        "consumer_session_replaced_total": 0,
    }


async def test_detach_is_idempotent_and_ignores_unknown_peers():
    clock = Clock()
    server = _server(clock)
    robot = await _producer(server, "tok-robot")
    await server.detach_peer(robot.peer_id)
    first = robot.detached_at
    clock.advance(3)
    await server.detach_peer(robot.peer_id)
    await server.detach_peer("no-such-peer")
    assert robot.detached_at == first
    assert server.sse_stats["detach_total"] == 1


# ----------------------------------------------------------------------
# Reattach within the grace
# ----------------------------------------------------------------------


async def test_reattach_within_grace_resumes_same_peer_and_flushes_queue():
    clock = Clock()
    server = _server(clock)
    phone = await _listener(server, "tok-phone")
    await server.detach_peer(phone.peer_id)

    # Broadcasts addressed to the detached listener are queued.
    robot = await _producer(server, "tok-robot")
    clock.advance(6.2)

    again = server.get_or_create_peer("tok-phone", "alice")
    assert again is phone
    generation, queue = server.attach_sse(again)
    assert phone.detached_at is not None  # until the stream starts
    server.sse_stream_started(again)

    assert generation == 2
    assert phone.message_queue is queue
    assert phone.detached_at is None
    flushed = _drain(phone)
    assert [m["type"] for m in flushed] == ["peerStatusChanged"]
    assert flushed[0]["peerId"] == robot.peer_id
    health = server.sse_health()
    assert health["detached_now"] == 0
    assert health["reattach_total"] == 1
    assert health["reattach_latency_s_max"] == pytest.approx(6.2)


async def test_reattach_preserves_queue_order():
    clock = Clock()
    server = _server(clock)
    phone = await _listener(server, "tok-phone")
    await server.detach_peer(phone.peer_id)
    for i in range(5):
        await server.send_to_peer(phone.peer_id, {"type": "x", "i": i})
    _connect(server, "tok-phone")
    assert [m["i"] for m in _drain(phone)] == [0, 1, 2, 3, 4]


async def test_robot_reattach_and_reregistration_cause_no_removal_nor_usage_events():
    """The relay's reconnect: SSE cut, same token back ~6 s later, then
    the same setPeerStatus it sends after every welcome."""
    clock = Clock()
    usage = UsageRecorder()
    server = _server(clock, usage)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    _drain(phone)
    usage.calls.clear()

    await server.detach_peer(robot.peer_id)
    clock.advance(6)
    again = await _producer(server, "tok-robot")

    assert again is robot and again.peer_id == robot.peer_id
    msgs = _drain(phone)
    assert _removals(msgs) == []
    # The re-registration is the same idempotent peerStatusChanged every
    # 10 s heartbeat already produces - never a removal.
    assert all(m["roles"] == ["producer"] for m in msgs if m["type"] == "peerStatusChanged")
    assert usage.events("producer_gone", "session_ended", "session_started") == []


async def test_reattach_does_not_double_count_distinct_robots():
    clock = Clock()
    tracker = UsageTracker(600)
    server = _server(clock, tracker)
    robot = await _producer(server, "tok-robot")
    await server.detach_peer(robot.peer_id)
    clock.advance(6)
    await _producer(server, "tok-robot")
    assert tracker._window.distinct.counts()["reachy_mini"] == 1
    assert tracker._current_robot_counts()["reachy_mini"] == 1
    assert tracker._window.robots_peak["reachy_mini"] == 1


async def test_reattach_refreshes_last_seen():
    clock = Clock()
    server = _server(clock)
    robot = await _producer(server, "tok-robot")
    robot.last_seen = time.monotonic() - 100
    await server.detach_peer(robot.peer_id)
    _connect(server, "tok-robot")
    assert time.monotonic() - robot.last_seen < 1.0


async def test_grace_zero_drops_old_queue_on_reconnect():
    """Grace disabled: a superseding connection starts on an empty queue (previous behaviour)."""
    server = _server(Clock(), sse_grace_seconds=0)
    phone = await _listener(server, "tok-phone")
    await server.send_to_peer(phone.peer_id, {"type": "x"})
    _connect(server, "tok-phone")
    assert _drain(phone) == []


# ----------------------------------------------------------------------
# Expiry
# ----------------------------------------------------------------------


async def test_expiry_evicts_like_an_sse_close_did():
    clock = Clock()
    usage = UsageRecorder()
    server = _server(clock, usage)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    _drain(phone)
    usage.calls.clear()
    await server.detach_peer(robot.peer_id)

    clock.advance(GRACE - 0.1)
    assert await server.expire_detached_peers() == []
    assert robot.peer_id in server.producers

    clock.advance(0.2)
    assert await server.expire_detached_peers() == [robot.peer_id]

    assert robot.peer_id not in server.peers
    assert robot.peer_id not in server.producers
    assert "tok-robot" not in server.token_to_peer
    removals = _removals(_drain(phone))
    assert [m["peerId"] for m in removals] == [robot.peer_id]
    assert usage.events("producer_gone") == [("producer_gone", (robot.peer_id,))]
    assert server.sse_health()["grace_expired_total"] == 1
    assert server.sse_health()["detached_now"] == 0
    # A reconnect after expiry mints a new peer id.
    assert _connect(server, "tok-robot").peer_id != robot.peer_id


async def test_expiry_ends_kept_consumer_session_with_peer_disconnected():
    clock = Clock()
    usage = UsageRecorder()
    server = _server(clock, usage)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    other = await _listener(server, "tok-desktop", name="desktop")
    sid = await _start(server, phone, robot)
    _drain(robot)
    _drain(other)

    await server.detach_peer(phone.peer_id)
    assert sid in server.sessions  # hybrid: consumer session survives
    clock.advance(GRACE)
    await server.expire_detached_peers()

    assert server.sessions == {}
    assert robot.session_id is None
    assert {"type": "endSession", "sessionId": sid} in _drain(robot)
    busy = [m for m in _drain(other) if m["type"] == "sessionStateChanged"]
    assert busy and busy[-1]["busy"] is False
    assert usage.events("session_ended") == [
        ("session_ended", (sid, SESSION_END_PEER_DISCONNECTED))
    ]


async def test_no_leaks_after_expiry():
    clock = Clock()
    tracker = UsageTracker(600)
    server = _server(clock, tracker)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    await _start(server, phone, robot)
    await server.detach_peer(phone.peer_id)  # consumer first: session kept
    await server.send_to_peer(phone.peer_id, {"type": "x"})
    clock.advance(1)
    await server.detach_peer(robot.peer_id)
    clock.advance(GRACE)

    assert sorted(await server.expire_detached_peers()) == sorted([robot.peer_id, phone.peer_id])
    assert server.peers == {}
    assert server.producers == {}
    assert server.token_to_peer == {}
    assert server.sessions == {}
    assert tracker._producers == {}
    assert tracker._sessions == {}
    assert server.count_detached_peers() == 0
    assert await server.expire_detached_peers() == []


# ----------------------------------------------------------------------
# Session policy
# ----------------------------------------------------------------------


async def test_hybrid_producer_detach_ends_session_immediately_like_before():
    clock = Clock()
    usage = UsageRecorder()
    server = _server(clock, usage)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    desktop = await _listener(server, "tok-desktop", name="desktop")
    sid = await _start(server, phone, robot)
    for p in (robot, phone, desktop):
        _drain(p)

    await server.detach_peer(robot.peer_id)

    assert server.sessions == {}
    assert robot.session_id is None and phone.session_id is None
    assert _drain(phone)[0] == {"type": "endSession", "sessionId": sid}
    # No endSession queued for the detached robot (it already dropped the
    # session and would get it as a stale message on reattach). It does
    # get the owner-wide busy=false broadcast, like any same-user peer.
    assert [m["type"] for m in _drain(robot)] == ["sessionStateChanged"]
    state = [m for m in _drain(desktop) if m["type"] == "sessionStateChanged"]
    assert [m["busy"] for m in state] == [False]
    assert usage.events("session_ended") == [
        ("session_ended", (sid, SESSION_END_PEER_DISCONNECTED))
    ]
    # Still listed, now free.
    assert server.get_producers_list("alice")[0]["busy"] is False
    # A new consumer can take it while the robot is still detached.
    other = await _listener(server, "tok-other")
    assert (await server.handle_message(other, {"type": "startSession", "peerId": robot.peer_id}))["type"] == "sessionStarted"


async def test_hybrid_consumer_detach_keeps_session_and_relays_into_queue():
    clock = Clock()
    usage = UsageRecorder()
    server = _server(clock, usage)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    sid = await _start(server, phone, robot)
    _drain(phone)
    _drain(robot)
    usage.calls.clear()

    await server.detach_peer(phone.peer_id)
    assert server.sessions[sid] == (robot.peer_id, phone.peer_id)
    assert server.get_producers_list("alice")[0]["busy"] is True
    assert _drain(robot) == []  # no endSession sent to the robot

    # The robot keeps signalling (ICE); it is queued for the phone.
    await server.handle_message(robot, {"type": "peer", "sessionId": sid, "ice": {"candidate": "c1"}})
    clock.advance(5)
    _connect(server, "tok-phone")
    flushed = _drain(phone)
    assert flushed == [{"type": "peer", "sessionId": sid, "ice": {"candidate": "c1"}}]
    assert usage.events("session_ended") == []
    assert sid in server.sessions


async def test_detached_consumer_session_still_blocks_other_consumers():
    clock = Clock()
    server = _server(clock)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    await _start(server, phone, robot)
    await server.detach_peer(phone.peer_id)
    desktop = await _listener(server, "tok-desktop", name="desktop")
    resp = await server.handle_message(desktop, {"type": "startSession", "peerId": robot.peer_id})
    assert resp["type"] == "sessionRejected" and resp["reason"] == "robot_busy"


async def test_consumer_reconnect_and_restart_replaces_its_own_old_session():
    """Consumer SSE cut, reconnect on the same token, immediate
    startSession on the same robot: not rejected by its own old session."""
    clock = Clock()
    usage = UsageRecorder()
    server = _server(clock, usage)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    old_sid = await _start(server, phone, robot)
    await server.detach_peer(phone.peer_id)
    clock.advance(6)
    again = await _listener(server, "tok-phone")
    assert again is phone
    _drain(robot)
    _drain(phone)
    usage.calls.clear()

    resp = await server.handle_message(phone, {"type": "startSession", "peerId": robot.peer_id})

    assert resp["type"] == "sessionStarted"
    new_sid = resp["sessionId"]
    assert new_sid != old_sid
    assert list(server.sessions) == [new_sid]
    robot_msgs = [m for m in _drain(robot) if m["type"] in ("endSession", "startSession")]
    assert [m["type"] for m in robot_msgs] == ["endSession", "startSession"]
    assert robot_msgs[0]["sessionId"] == old_sid
    assert robot_msgs[1]["sessionId"] == new_sid
    # The consumer is not told about the session it just replaced.
    assert all(m.get("sessionId") != old_sid for m in _drain(phone))
    assert usage.events("session_ended") == [("session_ended", (old_sid, SESSION_END_CONSUMER_REPLACED))]
    assert server.sse_health()["consumer_session_replaced_total"] == 1
    assert [c[0] for c in usage.events("session_started")] == ["session_started"]


async def test_consumer_endsession_post_while_detached_ends_session_cleanly():
    """The Python consumer's teardown POSTs endSession right after its SSE
    dropped: with the peer still registered it now lands (cause ``ended``)."""
    clock = Clock()
    usage = UsageRecorder()
    server = _server(clock, usage)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    sid = await _start(server, phone, robot)
    await server.detach_peer(phone.peer_id)
    await server.handle_message(phone, {"type": "endSession", "sessionId": sid})
    assert server.sessions == {}
    assert usage.events("session_ended") == [("session_ended", (sid, SESSION_END_ENDED))]


# ----------------------------------------------------------------------
# Interaction with the other eviction paths
# ----------------------------------------------------------------------


async def test_token_rotation_during_grace_evicts_detached_peer_by_hardware_id():
    clock = Clock()
    usage = UsageRecorder()
    server = _server(clock, usage)
    old = await _producer(server, "tok-robot-v1")
    phone = await _listener(server, "tok-phone")
    _drain(phone)
    await server.detach_peer(old.peer_id)
    usage.calls.clear()

    new = await _producer(server, "tok-robot-v2")  # same hardware_id, new token

    assert new.peer_id != old.peer_id
    assert old.peer_id not in server.peers
    assert old.peer_id not in server.producers
    assert "tok-robot-v1" not in server.token_to_peer
    assert list(server.producers) == [new.peer_id]
    msgs = [m for m in _drain(phone) if m["type"] == "peerStatusChanged"]
    assert (old.peer_id, []) in [(m["peerId"], m["roles"]) for m in msgs]
    assert (new.peer_id, ["producer"]) in [(m["peerId"], m["roles"]) for m in msgs]
    assert server.count_detached_peers() == 0
    clock.advance(GRACE)
    assert await server.expire_detached_peers() == []
    assert server.sse_stats["grace_expired_total"] == 0
    assert usage.events("producer_gone") == [("producer_gone", (old.peer_id,))]


async def test_withdraw_while_detached_then_expiry():
    clock = Clock()
    usage = UsageRecorder()
    server = _server(clock, usage)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    _drain(phone)
    await server.detach_peer(robot.peer_id)
    usage.calls.clear()

    # An in-flight POST from the daemon's shutdown path lands after the cut.
    await server.handle_message(robot, {"type": "setPeerStatus", "roles": [], "meta": dict(MINI)})

    assert robot.peer_id not in server.producers
    assert robot.peer_id in server.peers and robot.detached_at is not None
    assert [m["peerId"] for m in _removals(_drain(phone))] == [robot.peer_id]
    clock.advance(GRACE)
    assert await server.expire_detached_peers() == [robot.peer_id]
    assert robot.peer_id not in server.peers
    assert _drain(phone) == []  # no second removal: it was no longer a producer
    assert usage.events("producer_gone") == [("producer_gone", (robot.peer_id,))]


async def test_withdraw_after_reattach_behaves_as_before():
    clock = Clock()
    server = _server(clock)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    await server.detach_peer(robot.peer_id)
    _connect(server, "tok-robot")
    _drain(phone)
    await server.handle_message(robot, {"type": "setPeerStatus", "roles": [], "meta": dict(MINI)})
    assert robot.peer_id not in server.producers
    assert robot.peer_id in server.peers
    assert [m["peerId"] for m in _removals(_drain(phone))] == [robot.peer_id]


async def test_stale_sweep_skips_detached_and_grace_expiry_evicts_once():
    clock = Clock()
    usage = UsageRecorder()
    server = _server(clock, usage)
    robot = await _producer(server, "tok-robot")
    robot.last_seen = time.monotonic() - PRODUCER_LEASE_SECONDS - 5
    await server.detach_peer(robot.peer_id)
    usage.calls.clear()

    assert await server.sweep_stale_producers() == []
    assert robot.peer_id in server.producers

    clock.advance(GRACE)
    # Sweeper order: expiry first, then the stale sweep.
    assert await server.expire_detached_peers() == [robot.peer_id]
    assert await server.sweep_stale_producers() == []
    assert usage.events("producer_gone") == [("producer_gone", (robot.peer_id,))]


async def test_stale_sweep_still_evicts_attached_silent_producer():
    clock = Clock()
    server = _server(clock)
    robot = await _producer(server, "tok-robot")
    robot.last_seen = time.monotonic() - PRODUCER_LEASE_SECONDS - 5
    assert await server.sweep_stale_producers() == [robot.peer_id]


# ----------------------------------------------------------------------
# Grace 0 = previous behaviour
# ----------------------------------------------------------------------


async def test_grace_zero_detach_evicts_immediately():
    usage = UsageRecorder()
    server = _server(Clock(), usage, sse_grace_seconds=0)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    sid = await _start(server, phone, robot)
    _drain(phone)
    usage.calls.clear()

    await server.detach_peer(robot.peer_id)

    assert robot.peer_id not in server.peers
    assert "tok-robot" not in server.token_to_peer
    msgs = _drain(phone)
    assert {"type": "endSession", "sessionId": sid} in msgs
    assert [m["peerId"] for m in _removals(msgs)] == [robot.peer_id]
    assert usage.events("session_ended") == [("session_ended", (sid, SESSION_END_PEER_DISCONNECTED))]
    assert server.sse_health()["detach_total"] == 0
    assert await server.expire_detached_peers() == []


@pytest.mark.parametrize(
    "existing_test",
    [
        test_signaling.test_disconnect_peer_clears_all_structures,
        test_signaling.test_sweep_evicts_stale_heartbeating_producer,
        test_signaling.test_sweep_ends_ghost_session_and_notifies_consumer,
        test_signaling.test_hardware_id_collision_evicts_older_producer,
        test_signaling.test_install_id_collision_ends_old_session,
    ],
    ids=lambda f: f.__name__,
)
async def test_existing_eviction_tests_pass_with_grace_disabled(monkeypatch, existing_test):
    monkeypatch.setattr(app_module, "SSE_RECONNECT_GRACE_SECONDS", 0.0)
    assert SignalingServer().sse_grace_seconds == 0
    await existing_test()


# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------


async def test_detach_and_reattach_log_nothing_at_info(caplog):
    clock = Clock()
    server = _server(clock)
    robot = await _producer(server, "tok-robot", meta=MINI)
    with caplog.at_level(logging.INFO, logger="app"):
        await server.detach_peer(robot.peer_id)
        clock.advance(6)
        _connect(server, "tok-robot")
    assert [r for r in caplog.records if r.levelno >= logging.INFO] == []


async def test_sse_summary_is_once_per_interval_and_aggregate_only(caplog):
    clock = Clock()
    server = _server(clock)
    robot = await _producer(server, "tok-robot")
    robot2 = await _producer(server, "tok-robot2", meta=MINI2)
    await server.detach_peer(robot.peer_id)
    await server.detach_peer(robot2.peer_id)
    clock.advance(7)
    _connect(server, "tok-robot")
    clock.advance(GRACE)
    await server.expire_detached_peers()

    with caplog.at_level(logging.INFO, logger="app"):
        assert server.maybe_log_sse_summary(now=clock.now - 40) is False  # < 60 s
        assert server.maybe_log_sse_summary(now=clock.now + 60) is True
        assert server.maybe_log_sse_summary(now=clock.now + 70) is False
        # Idle interval: silent.
        assert server.maybe_log_sse_summary(now=clock.now + 200) is False
    lines = [r.getMessage() for r in caplog.records if "SSE summary" in r.getMessage()]
    assert len(lines) == 1
    line = lines[0]
    assert "detached=2 reattached=1 grace_expired=1 detached_now=0" in line
    assert "reattach_latency_max=7.0s" in line
    for secret in ("tok-robot", "alice", "hw-1", robot.peer_id, robot2.peer_id):
        assert secret not in line


# ----------------------------------------------------------------------
# Route level: the real /events generator and the HTTP views
# ----------------------------------------------------------------------


class _Request:
    """Request stand-in: ``closed`` drives ``is_disconnected``."""

    client = None

    def __init__(self, closed: bool):
        self.closed = closed

    async def is_disconnected(self) -> bool:
        return self.closed


@pytest.fixture
def route_server(monkeypatch):
    """Fresh module-global SignalingServer with an injected clock."""
    clock = Clock()
    server = SignalingServer(sse_grace_seconds=GRACE, clock=clock)
    monkeypatch.setattr(app_module, "signaling", server)
    for tok, user in (("tok-robot", "alice"), ("tok-phone", "alice"), ("tok-bob", "bob")):
        monkeypatch.setitem(token_cache, tok, (user, float("inf")))
    buckets = {k: v.copy() for k, v in _rate_limit_buckets.items()}
    app_module._health_cache_reset()
    yield server, clock
    app_module._health_cache_reset()
    _rate_limit_buckets.clear()
    _rate_limit_buckets.update(buckets)


async def _open(token: str, closed: bool = False):
    response = await app_module.events(_Request(closed), token=token)
    return response.body_iterator


async def _run_to_close(token: str) -> list[dict]:
    import json

    return [json.loads(e["data"]) async for e in await _open(token, closed=True)]


async def test_route_sse_close_detaches_and_reconnect_resumes_with_queue(route_server):
    import json

    server, clock = route_server
    frames = await _run_to_close("tok-phone")
    assert [f["type"] for f in frames] == ["welcome", "list"]
    phone = server.peers[frames[0]["peerId"]]
    assert phone.detached_at is not None

    await server.send_to_peer(phone.peer_id, {"type": "queued", "n": 1})
    clock.advance(6)
    gen = await _open("tok-phone")
    first = [json.loads((await gen.__anext__())["data"]) for _ in range(3)]
    assert first[0]["type"] == "welcome" and first[0]["peerId"] == phone.peer_id
    assert first[1]["type"] == "list"
    assert first[2] == {"type": "queued", "n": 1}
    assert phone.detached_at is None
    await gen.aclose()  # the stream closes again: detached again
    assert phone.detached_at is not None
    assert server.sse_stats["detach_total"] == 2
    assert server.sse_stats["reattach_total"] == 1


async def test_route_superseded_generation_close_does_not_detach(route_server):
    server, _clock = route_server
    old = await _open("tok-robot", closed=True)  # created, not yet run
    new = await _open("tok-robot")  # supersedes it
    robot = server.peers[server.token_to_peer["tok-robot"]]
    assert robot.sse_generation == 2

    async for _ in old:  # old socket finally closes
        pass
    assert robot.detached_at is None
    assert server.sse_stats["detach_total"] == 0

    await server.send_to_peer(robot.peer_id, {"type": "x"})
    for _ in range(3):  # welcome, list, x: now inside the streaming loop
        await new.__anext__()
    await new.aclose()  # the current one closing does detach
    assert robot.detached_at is not None


async def test_route_reconnect_whose_stream_never_starts_leaves_peer_detached(route_server):
    server, clock = route_server
    frames = await _run_to_close("tok-phone")
    phone = server.peers[frames[0]["peerId"]]
    await _open("tok-phone")  # response built, body never iterated
    assert phone.detached_at is not None
    clock.advance(GRACE)
    assert await server.expire_detached_peers() == [phone.peer_id]
    assert server.peers == {} and server.token_to_peer == {}


async def test_route_stream_closed_during_handshake_still_detaches(route_server):
    server, _clock = route_server
    gen = await _open("tok-phone")
    await gen.__anext__()  # welcome only
    await gen.aclose()
    phone = server.peers[server.token_to_peer["tok-phone"]]
    assert phone.detached_at is not None


async def test_route_sse_close_then_expiry_records_peer_disconnected(route_server, monkeypatch):
    server, clock = route_server
    tracker = UsageTracker(600)
    server.usage = tracker
    robot = server.get_or_create_peer("tok-robot", "alice")
    await server.handle_message(robot, {"type": "setPeerStatus", "roles": ["producer"], "meta": dict(MINI)})
    sid = await _start(server, await _listener(server, "tok-phone"), robot)

    await _run_to_close("tok-robot")
    assert robot.peer_id in server.producers
    assert sid not in server.sessions  # hybrid: producer session ended at detach
    clock.advance(GRACE)
    await server.expire_detached_peers()
    assert robot.peer_id not in server.peers
    assert tracker._window.end_reasons[SESSION_END_PEER_DISCONNECTED] == 1


def test_health_and_debug_views_while_detached(route_server):
    server, clock = route_server
    client = TestClient(app_module.app)
    robot = server.get_or_create_peer("tok-robot", "alice")
    server.attach_sse(robot)
    server.sse_stream_started(robot)
    r = client.post(
        "/send",
        json={"type": "setPeerStatus", "roles": ["producer"], "meta": MINI},
        headers={"Authorization": "Bearer tok-robot"},
    )
    assert r.status_code == 200

    import asyncio

    asyncio.run(server.detach_peer(robot.peer_id))
    clock.advance(4.5)
    app_module._health_cache_reset()

    health = client.get("/health").json()
    assert health["producers"] == 1 and health["peers"] == 1
    assert health["producers_by_kind"]["reachy_mini"] == 1
    assert health["sse"] == {
        "grace_seconds": GRACE,
        "detached_now": 1,
        "detach_total": 1,
        "reattach_total": 0,
        "grace_expired_total": 0,
        "reattach_latency_s_max": 0.0,
        "sessions_ended_at_detach_total": 0,
        "consumer_session_replaced_total": 0,
    }
    status = client.get("/api/robot-status", headers={"Authorization": "Bearer tok-phone"}).json()
    assert [row["peerId"] for row in status["robots"]] == [robot.peer_id]
    rows = client.get("/api/debug/peers", headers={"Authorization": "Bearer tok-phone"}).json()["peers"]
    assert rows[0]["detached"] is True
    assert rows[0]["detached_age_seconds"] == pytest.approx(4.5)
    # Owner-scoped as before.
    assert client.get("/api/debug/peers", headers={"Authorization": "Bearer tok-bob"}).json()["peers"] == []

    # A heartbeat POST from the detached robot still works (peer registered).
    r = client.post(
        "/send",
        json={"type": "setPeerStatus", "roles": ["producer"], "meta": MINI},
        headers={"Authorization": "Bearer tok-robot"},
    )
    assert r.status_code == 200

    clock.advance(GRACE)
    asyncio.run(server.expire_detached_peers())
    app_module._health_cache_reset()
    health = client.get("/health").json()
    assert health["producers"] == 0
    assert health["sse"]["grace_expired_total"] == 1
    r = client.post(
        "/send",
        json={"type": "list"},
        headers={"Authorization": "Bearer tok-robot"},
    )
    assert r.status_code == 400 and r.json() == {"detail": "Connect to /events first"}


def test_health_sse_block_present_in_default_app():
    body = app_module._cached_health_body(now=time.monotonic() + 10_000)
    assert set(body["sse"]) == {
        "grace_seconds", "detached_now", "detach_total", "reattach_total",
        "grace_expired_total", "reattach_latency_s_max",
        "sessions_ended_at_detach_total", "consumer_session_replaced_total",
    }
    app_module._health_cache_reset()


def test_default_grace_is_15_seconds():
    assert app_module.SSE_RECONNECT_GRACE_SECONDS == 15.0
    assert not hasattr(app_module, "SSE_GRACE_SESSION_POLICY")
    assert SignalingServer().sse_grace_seconds == 15.0


# ----------------------------------------------------------------------
# Stale endSession frames are never replayed to the peer that ended them
# ----------------------------------------------------------------------


async def test_stale_endsession_purged_when_consumer_ends_it_while_detached():
    """Reviewer P4: phone and robot both cut; the robot's detach ends the
    session and queues endSession for the phone; the phone's redial POSTs
    endSession itself before reconnecting. The reattach must not replay it
    (the JS SDK would abort its new dial on any endSession)."""
    clock = Clock()
    server = _server(clock)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    sid = await _start(server, phone, robot)
    _drain(phone)
    await server.detach_peer(phone.peer_id)
    await server.detach_peer(robot.peer_id)  # producer side: session ends
    await server.handle_message(phone, {"type": "endSession", "sessionId": sid})
    clock.advance(6)
    _connect(server, "tok-phone")
    assert all(m["type"] != "endSession" for m in _drain(phone))


async def test_echo_of_own_endsession_while_detached_is_not_replayed():
    """Consumer detached with its session kept; its redial POSTs endSession
    (the session really ends now); the echo queued for it is dropped on
    reattach, while the robot gets its endSession as usual."""
    clock = Clock()
    server = _server(clock)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    sid = await _start(server, phone, robot)
    _drain(robot)
    _drain(phone)
    await server.detach_peer(phone.peer_id)
    await server.handle_message(phone, {"type": "endSession", "sessionId": sid})
    assert server.sessions == {}
    assert {"type": "endSession", "sessionId": sid} in _drain(robot)
    clock.advance(4)
    _connect(server, "tok-phone")
    assert all(m["type"] != "endSession" for m in _drain(phone))


async def test_endsession_the_peer_did_not_end_is_still_delivered_on_reattach():
    clock = Clock()
    server = _server(clock)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    sid = await _start(server, phone, robot)
    _drain(phone)
    await server.detach_peer(phone.peer_id)
    await server.handle_message(robot, {"type": "endSession", "sessionId": sid})  # robot ends it
    _connect(server, "tok-phone")
    assert {"type": "endSession", "sessionId": sid} in _drain(phone)


async def test_replacement_purges_queued_endsession_for_old_session():
    clock = Clock()
    server = _server(clock)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    old_sid = await _start(server, phone, robot)
    # A stale endSession for the old session is somehow still queued.
    await server.send_to_peer(phone.peer_id, {"type": "endSession", "sessionId": old_sid})
    resp = await server.handle_message(phone, {"type": "startSession", "peerId": robot.peer_id})
    assert resp["type"] == "sessionStarted"
    assert all(m.get("sessionId") != old_sid for m in _drain(phone))


# ----------------------------------------------------------------------
# Consumer self-replacement: also at grace 0; other consumers still busy
# ----------------------------------------------------------------------


@pytest.mark.parametrize("grace", [0, GRACE])
async def test_consumer_self_replacement_works_at_any_grace(grace):
    usage = UsageRecorder()
    server = _server(Clock(), usage, sse_grace_seconds=grace)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    r1 = await server.handle_message(phone, {"type": "startSession", "peerId": robot.peer_id})
    r2 = await server.handle_message(phone, {"type": "startSession", "peerId": robot.peer_id})
    assert r1["type"] == r2["type"] == "sessionStarted"
    assert list(server.sessions) == [r2["sessionId"]]
    assert usage.events("session_ended") == [
        ("session_ended", (r1["sessionId"], SESSION_END_CONSUMER_REPLACED))
    ]


@pytest.mark.parametrize("grace", [0, GRACE])
async def test_attached_other_consumer_of_same_user_still_gets_robot_busy(grace):
    server = _server(Clock(), sse_grace_seconds=grace)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    desktop = await _listener(server, "tok-desktop", name="desktop")
    sid = await _start(server, phone, robot)
    resp = await server.handle_message(desktop, {"type": "startSession", "peerId": robot.peer_id})
    assert resp == {
        "type": "sessionRejected",
        "reason": "robot_busy",
        "peerId": robot.peer_id,
        "activeApp": "app",
    }
    assert list(server.sessions) == [sid]


# ----------------------------------------------------------------------
# Grace deadline: re-armed on attach, pending until the stream starts
# ----------------------------------------------------------------------


async def test_reconnect_at_end_of_grace_is_not_expired_before_it_starts():
    """Reviewer P1: attach at 14.9 s, sweep at 15.1 s, stream starts after."""
    clock = Clock()
    server = _server(clock)
    phone = await _listener(server, "tok-phone")
    await server.detach_peer(phone.peer_id)
    clock.advance(14.9)
    again = server.get_or_create_peer("tok-phone", "alice")
    server.attach_sse(again)  # bound, not streaming yet
    clock.advance(0.2)
    assert await server.expire_detached_peers() == []
    server.sse_stream_started(again)
    assert phone.detached_at is None and phone.grace_deadline is None
    assert server.sse_stats["reattach_total"] == 1
    clock.advance(100)
    assert await server.expire_detached_peers() == []


async def test_reattach_whose_stream_never_starts_expires_after_rearmed_grace():
    clock = Clock()
    server = _server(clock)
    phone = await _listener(server, "tok-phone")
    await server.detach_peer(phone.peer_id)
    clock.advance(10)
    server.attach_sse(server.get_or_create_peer("tok-phone", "alice"))
    clock.advance(GRACE - 0.1)
    assert await server.expire_detached_peers() == []
    clock.advance(0.2)
    assert await server.expire_detached_peers() == [phone.peer_id]


async def test_superseded_attached_peer_whose_new_stream_never_starts_expires():
    """Reviewer P3: no detach counted, no session rule for a consumer, but
    the peer can no longer be stranded."""
    clock = Clock()
    server = _server(clock)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    sid = await _start(server, phone, robot)
    await server.connect_sse(server.get_or_create_peer("tok-phone", "alice"))  # never starts
    assert phone.detached_at is None
    assert sid in server.sessions  # consumer: no session rule on takeover
    assert server.sse_stats["detach_total"] == 0
    clock.advance(GRACE)
    assert await server.expire_detached_peers() == [phone.peer_id]
    assert phone.peer_id not in server.peers
    assert server.sessions == {}


async def test_two_rapid_reconnects_middle_started_then_superseded():
    clock = Clock()
    server = _server(clock)
    phone = await _listener(server, "tok-phone")  # stream #1 live
    mid = server.get_or_create_peer("tok-phone", "alice")
    await server.connect_sse(mid)  # stream #2
    server.sse_stream_started(mid)  # #2 starts
    await server.connect_sse(mid)  # stream #3 supersedes #2, never starts
    clock.advance(GRACE - 1)
    assert await server.expire_detached_peers() == []
    clock.advance(1)
    assert await server.expire_detached_peers() == [phone.peer_id]

    # Same sequence where #3 does start: nothing expires.
    server2 = _server(Clock())
    p = await _listener(server2, "tok-phone")
    await server2.connect_sse(p)
    server2.sse_stream_started(p)
    await server2.connect_sse(p)
    server2.sse_stream_started(p)
    server2.clock.advance(1000)
    assert await server2.expire_detached_peers() == []


async def test_producer_takeover_ends_its_session_like_a_detach():
    """A relay reconnecting while central still holds its old (half-open)
    stream already dropped its sessions: the session ends at takeover."""
    clock = Clock()
    usage = UsageRecorder()
    server = _server(clock, usage)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    sid = await _start(server, phone, robot)
    # startSession(sid) is still sitting in the robot's (old) queue.
    assert any(m["type"] == "startSession" for m in list(robot.message_queue._queue))
    _drain(phone)
    usage.calls.clear()

    await server.connect_sse(server.get_or_create_peer("tok-robot", "alice"))

    assert server.sessions == {}
    assert {"type": "endSession", "sessionId": sid} in _drain(phone)
    assert all(m.get("sessionId") != sid for m in _drain(robot))  # nothing stale replayed
    assert usage.events("session_ended") == [("session_ended", (sid, SESSION_END_PEER_DISCONNECTED))]
    assert server.sse_health()["sessions_ended_at_detach_total"] == 1
    assert server.sse_stats["detach_total"] == 0
    assert robot.peer_id in server.producers


async def test_producer_takeover_keeps_session_at_grace_zero():
    server = _server(Clock(), sse_grace_seconds=0)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    sid = await _start(server, phone, robot)
    await server.connect_sse(server.get_or_create_peer("tok-robot", "alice"))
    assert sid in server.sessions  # previous behaviour


async def test_detach_purges_stale_session_frames_from_producer_queue():
    clock = Clock()
    server = _server(clock)
    robot = await _producer(server, "tok-robot")
    phone = await _listener(server, "tok-phone")
    sid = await _start(server, phone, robot)  # startSession(sid) queued for robot
    await server.detach_peer(robot.peer_id)
    _connect(server, "tok-robot")
    assert all(m.get("sessionId") != sid for m in _drain(robot))


# ----------------------------------------------------------------------
# Ghost producer re-insert (POST racing an eviction)
# ----------------------------------------------------------------------


class _RacingRequest:
    """POST /send request whose body read lets an eviction run first."""

    client = None

    def __init__(self, body, during_read):
        self._body, self._during_read = body, during_read

    async def json(self):
        await self._during_read()
        return self._body


async def test_send_racing_an_eviction_does_not_reinsert_ghost(route_server):
    server, clock = route_server
    robot = server.get_or_create_peer("tok-robot", "alice")
    server.attach_sse(robot)
    server.sse_stream_started(robot)
    await server.handle_message(robot, {"type": "setPeerStatus", "roles": ["producer"], "meta": dict(MINI)})
    await server.detach_peer(robot.peer_id)
    clock.advance(GRACE)

    req = _RacingRequest(
        {"type": "setPeerStatus", "roles": ["producer"], "meta": dict(MINI)},
        server.expire_detached_peers,
    )
    with pytest.raises(app_module.HTTPException) as exc:
        await app_module.send_message(req, token="tok-robot")
    assert exc.value.status_code == 400 and exc.value.detail == "Peer not found"
    assert server.producers == {} and server.peers == {}


async def test_disconnect_peer_drops_orphan_producers_entry():
    usage = UsageRecorder()
    server = _server(Clock(), usage)
    robot = await _producer(server, "tok-robot")
    del server.peers[robot.peer_id]  # orphaned producers entry
    usage.calls.clear()
    await server.disconnect_peer(robot.peer_id)
    assert server.producers == {}
    assert usage.events("producer_gone") == [("producer_gone", (robot.peer_id,))]


async def test_ghost_reinserted_after_expiry_is_swept():
    """Reviewer probe2: a message handled for an already-expired peer (bypassing
    the route guard) re-inserts it into producers; the stale sweep must still
    be able to remove that orphan."""
    clock = Clock()
    server = _server(clock)
    robot = await _producer(server, "tok-robot")
    await server.detach_peer(robot.peer_id)
    clock.advance(GRACE)
    await server.expire_detached_peers()
    await server.handle_message(robot, {"type": "setPeerStatus", "roles": ["producer"], "meta": dict(MINI)})
    assert robot.peer_id in server.producers  # the ghost
    robot.last_seen = time.monotonic() - PRODUCER_LEASE_SECONDS - 1
    assert await server.sweep_stale_producers() == [robot.peer_id]
    assert server.producers == {}


async def test_stale_sweep_removes_orphan_producer():
    server = _server(Clock())
    robot = await _producer(server, "tok-robot")
    del server.peers[robot.peer_id]
    robot.last_seen = time.monotonic() - PRODUCER_LEASE_SECONDS - 1
    assert await server.sweep_stale_producers() == [robot.peer_id]
    assert server.producers == {}


# ----------------------------------------------------------------------
# Sweeper robustness
# ----------------------------------------------------------------------


async def test_failing_peer_disconnect_does_not_block_other_expiries(monkeypatch, caplog):
    clock = Clock()
    server = _server(clock)
    a = await _listener(server, "tok-a")
    b = await _listener(server, "tok-b")
    await server.detach_peer(a.peer_id)
    await server.detach_peer(b.peer_id)
    clock.advance(GRACE)
    real = server.disconnect_peer

    async def flaky(pid, **kw):
        if pid == a.peer_id:
            raise RuntimeError("boom")
        await real(pid, **kw)

    monkeypatch.setattr(server, "disconnect_peer", flaky)
    with caplog.at_level(logging.ERROR, logger="app"):
        await server.expire_detached_peers()
        await server.expire_detached_peers()  # a is not retried/counted again
    assert b.peer_id not in server.peers
    assert server.sse_stats["grace_expired_total"] == 2
    assert any("SSE grace expiry failed" in r.getMessage() for r in caplog.records)


async def test_sweeper_tick_runs_every_step_even_if_one_fails(monkeypatch, caplog):
    clock = Clock()
    server = _server(clock)
    ran = []

    async def boom():
        raise RuntimeError("expiry bug")

    async def sweep():
        ran.append("sweep")
        return []

    monkeypatch.setattr(server, "expire_detached_peers", boom)
    monkeypatch.setattr(server, "sweep_stale_producers", sweep)
    monkeypatch.setattr(server, "maybe_log_sse_summary", lambda: ran.append("sse_summary"))
    monkeypatch.setattr(app_module, "_prune_rate_limit_buckets", lambda: (_ for _ in ()).throw(RuntimeError("prune bug")))
    with caplog.at_level(logging.ERROR, logger="app"):
        await server.run_sweeper_tick()
    assert ran == ["sweep", "sse_summary"]
    failed = [r.getMessage() for r in caplog.records if "Sweeper step failed" in r.getMessage()]
    assert failed == ["Sweeper step failed: grace expiry", "Sweeper step failed: rate-limit prune"]


# ----------------------------------------------------------------------
# Production close path: the generator cancelled at the queue wait
# ----------------------------------------------------------------------


async def test_route_generator_cancelled_at_queue_wait_detaches(route_server):
    """sse-starlette cancels the streaming task on client disconnect while
    the generator is parked in ``queue.get()`` - reviewer P2."""
    import asyncio

    server, _clock = route_server
    gen = await _open("tok-phone")
    await gen.__anext__()
    await gen.__anext__()
    pending = asyncio.ensure_future(gen.__anext__())
    await asyncio.sleep(0.01)  # now awaiting queue.get()
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    phone = server.peers[server.token_to_peer["tok-phone"]]
    assert phone.detached_at is not None
    assert phone.grace_deadline == _clock.now + GRACE
