"""HTTP route tests for ``app.py`` (status page, health, owner-scoped APIs).

The other suites drive ``SignalingServer`` directly; this one goes
through FastAPI so template rendering, response headers, auth
dependencies and the JSON shapes clients depend on are covered.

Harness notes:

- ``TestClient(app)`` is used WITHOUT the context manager, so the
  lifespan (and its background sweeper task) never runs: no timing in
  these tests.
- Authentication is short-circuited by pre-seeding ``token_cache`` with
  fresh entries, mirroring what a successful whoami would leave behind.
  Nothing here talks to huggingface.co.
- ``GET /events`` is a never-ending SSE stream and is not exercised via
  the client; the token->peer binding it would create is reproduced
  with ``signaling.get_or_create_peer`` so ``POST /send`` can be used
  as the real registration path.

Run with::

    python -m pytest test_routes.py -v
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

import app as app_module
from app import (
    PUBLIC_ROBOT_KINDS,
    ROBOT_KIND_LABELS,
    TOKEN_CACHE_TTL_SECONDS,
    _rate_limit_buckets,
    signaling,
    token_cache,
)

ALICE_TOKEN = "tok-alice"
BOB_TOKEN = "tok-bob"
ALICE, BOB = "alice", "bob"

MINI_META = {"name": "mini1", "transport": "usb", "hardware_id": "aaaa"}
DUCK_META = {"name": "duck1", "kind": "microduck", "hardware_id": "bbbb", "release": "0.9.1"}


# ----------------------------------------------------------------------
# Fixtures / helpers
# ----------------------------------------------------------------------


def _reset_signaling_state() -> None:
    signaling.peers.clear()
    signaling.producers.clear()
    signaling.sessions.clear()
    signaling.token_to_peer.clear()


@pytest.fixture
def client():
    """Fresh module-global state + a TestClient without lifespan."""
    cache_snapshot = dict(token_cache)
    bucket_snapshot = {k: v.copy() for k, v in _rate_limit_buckets.items()}
    _reset_signaling_state()
    token_cache.clear()
    _rate_limit_buckets.clear()
    fresh = time.monotonic() + TOKEN_CACHE_TTL_SECONDS
    token_cache[ALICE_TOKEN] = (ALICE, fresh)
    token_cache[BOB_TOKEN] = (BOB, fresh)
    try:
        yield TestClient(app_module.app)
    finally:
        _reset_signaling_state()
        token_cache.clear()
        token_cache.update(cache_snapshot)
        _rate_limit_buckets.clear()
        _rate_limit_buckets.update(bucket_snapshot)


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _bind(token: str, username: str):
    """Reproduce the token->peer binding ``GET /events`` performs."""
    return signaling.get_or_create_peer(token, username)


def _register_producer(client: TestClient, token: str, username: str, meta: dict):
    """Bind the token and register a producer through the real ``/send``."""
    peer = _bind(token, username)
    r = client.post(
        "/send",
        json={"type": "setPeerStatus", "roles": ["producer"], "meta": meta},
        headers=_bearer(token),
    )
    assert r.status_code == 200, r.text
    return peer


# ----------------------------------------------------------------------
# GET /  (public status page)
# ----------------------------------------------------------------------


def test_root_renders_status_page(client):
    r = client.get("/")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert r.headers["cache-control"] == "no-store"
    body = r.text
    for element_id in ("peers", "producers", "sessions", "uptime", "started-at"):
        assert f'id="{element_id}"' in body
    assert "Up since" in body
    assert "counters reset on every redeploy" in body


def test_root_has_no_unexpanded_template_placeholder(client):
    """``Template.substitute`` raises on a *missing* key, but a literal
    ``$`` that survived (e.g. from ``safe_substitute`` or a ``$$`` typo)
    would render verbatim. The page has no legitimate dollar sign.
    """
    assert "$" not in client.get("/").text


def test_root_renders_one_card_per_kind_from_constant_labels(client):
    body = client.get("/").text
    for kind in PUBLIC_ROBOT_KINDS:
        assert f'id="kind-{kind}"' in body
        assert ROBOT_KIND_LABELS[kind] in body
    assert "Robots online by kind" in body


def test_root_counters_start_at_zero(client):
    body = client.get("/").text
    assert 'id="peers">0<' in body
    assert 'id="producers">0<' in body
    assert 'id="sessions">0<' in body
    for kind in PUBLIC_ROBOT_KINDS:
        assert f'id="kind-{kind}">0<' in body


def test_root_reflects_registered_fleet(client):
    _register_producer(client, ALICE_TOKEN, ALICE, MINI_META)
    _register_producer(client, BOB_TOKEN, BOB, DUCK_META)

    body = client.get("/").text
    assert 'id="peers">2<' in body
    assert 'id="producers">2<' in body
    assert 'id="kind-reachy_mini">1<' in body
    assert 'id="kind-microduck">1<' in body
    assert 'id="kind-other">0<' in body


def test_root_never_echoes_meta(client):
    """Meta is attacker-controlled and the template does no escaping:
    an unknown kind (or any other meta field) must only ever move a
    counter, never appear in the page.
    """
    hostile = {
        "name": "<img src=x onerror=alert(1)>",
        "kind": "<script>alert('kind')</script>",
        "hardware_id": "h0st1le",
    }
    _register_producer(client, ALICE_TOKEN, ALICE, hostile)

    body = client.get("/").text
    assert "alert(" not in body
    assert "<img" not in body
    assert "h0st1le" not in body
    assert 'id="kind-other">1<' in body


# ----------------------------------------------------------------------
# GET /health
# ----------------------------------------------------------------------


def test_health_shape(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.headers["cache-control"] == "no-store"
    data = r.json()
    assert set(data) == {
        "status", "peers", "producers", "sessions",
        "producers_by_kind", "started_at", "uptime_seconds",
    }
    assert data["status"] == "healthy"
    assert data["peers"] == data["producers"] == data["sessions"] == 0
    assert data["producers_by_kind"] == {kind: 0 for kind in PUBLIC_ROBOT_KINDS}


def test_health_started_at_is_utc_iso8601_and_uptime_is_int(client):
    data = client.get("/health").json()
    started = datetime.fromisoformat(data["started_at"].replace("Z", "+00:00"))
    assert started.tzinfo is not None
    assert started.utcoffset().total_seconds() == 0
    assert started <= datetime.now(timezone.utc)
    assert isinstance(data["uptime_seconds"], int)
    assert data["uptime_seconds"] >= 0
    assert data["started_at"] == app_module.STARTED_AT_ISO
    # The page shows the very same string.
    assert f'id="started-at">{app_module.STARTED_AT_ISO}<' in client.get("/").text


def test_health_counts_producers_by_kind(client):
    _register_producer(client, ALICE_TOKEN, ALICE, MINI_META)
    _register_producer(client, BOB_TOKEN, BOB, DUCK_META)

    data = client.get("/health").json()
    assert data["peers"] == 2
    assert data["producers"] == 2
    assert data["sessions"] == 0
    assert data["producers_by_kind"] == {"reachy_mini": 1, "microduck": 1, "other": 0}


def test_health_buckets_unknown_kind_as_other(client):
    _register_producer(client, ALICE_TOKEN, ALICE, {**MINI_META, "kind": "unicorn"})
    data = client.get("/health").json()
    assert data["producers_by_kind"] == {"reachy_mini": 0, "microduck": 0, "other": 1}
    # The key set is fixed: an unknown kind never becomes a new key.
    assert set(data["producers_by_kind"]) == set(PUBLIC_ROBOT_KINDS)


def test_health_and_root_agree_and_exclude_disconnected_producers(client):
    """``producers`` used to be ``len(producers)`` unfiltered while
    ``peers`` filtered on ``connected``; both surfaces now share one
    connected-only definition.
    """
    mini = _register_producer(client, ALICE_TOKEN, ALICE, MINI_META)
    _register_producer(client, BOB_TOKEN, BOB, DUCK_META)
    mini.connected = False  # SSE gone, eviction not yet run

    data = client.get("/health").json()
    assert data["peers"] == 1
    assert data["producers"] == 1
    assert data["producers_by_kind"] == {"reachy_mini": 0, "microduck": 1, "other": 0}

    body = client.get("/").text
    assert 'id="peers">1<' in body
    assert 'id="producers">1<' in body
    assert 'id="kind-reachy_mini">0<' in body
    assert 'id="kind-microduck">1<' in body


# ----------------------------------------------------------------------
# POST /send
# ----------------------------------------------------------------------


def test_send_requires_token(client):
    r = client.post("/send", json={"type": "list"})
    assert r.status_code == 401
    assert r.json() == {"detail": "Missing token"}


def test_send_requires_prior_events_binding(client):
    r = client.post("/send", json={"type": "list"}, headers=_bearer(ALICE_TOKEN))
    assert r.status_code == 400
    assert r.json() == {"detail": "Connect to /events first"}


def test_send_rejects_non_object_meta_and_keeps_previous_meta(client):
    peer = _register_producer(client, ALICE_TOKEN, ALICE, MINI_META)

    r = client.post(
        "/send",
        json={"type": "setPeerStatus", "roles": ["producer"], "meta": "x"},
        headers=_bearer(ALICE_TOKEN),
    )
    assert r.status_code == 400
    assert r.json() == {"detail": "meta must be a JSON object"}
    assert peer.meta == MINI_META
    assert peer.peer_id in signaling.producers
    assert client.get("/health").json()["producers_by_kind"]["reachy_mini"] == 1


def test_send_list_is_owner_scoped_and_forwards_meta_verbatim(client):
    _register_producer(client, ALICE_TOKEN, ALICE, MINI_META)
    _register_producer(client, BOB_TOKEN, BOB, {**DUCK_META, "kind": "MicroDuck "})

    r = client.post("/send", json={"type": "list"}, headers=_bearer(BOB_TOKEN))
    assert r.status_code == 200
    producers = r.json()["producers"]
    assert len(producers) == 1
    # Raw, un-normalised kind reaches the owner: classification is only
    # for the public counters.
    assert producers[0]["meta"] == {**DUCK_META, "kind": "MicroDuck "}


# ----------------------------------------------------------------------
# GET /api/robot-status
# ----------------------------------------------------------------------


def test_robot_status_requires_token(client):
    r = client.get("/api/robot-status")
    assert r.status_code == 401
    assert r.json() == {"detail": "Missing token"}


def test_robot_status_rejects_non_bearer_scheme(client):
    r = client.get("/api/robot-status", headers={"Authorization": "Basic dXNlcjpwdw=="})
    assert r.status_code == 401
    assert "Bearer" in r.json()["detail"]


def test_robot_status_rejects_unknown_token_without_network(client, monkeypatch):
    """A token that is not cached goes to whoami; stub it to a 401 so the
    ``Invalid token`` branch is covered deterministically.
    """
    import httpx

    class _Resp:
        status_code = 401

        def json(self):
            return {}

    class _FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, *a, **kw):
            return _Resp()

    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
    r = client.get("/api/robot-status", headers=_bearer("never-seen"))
    assert r.status_code == 401
    assert r.json() == {"detail": "Invalid token"}


def test_robot_status_filters_by_owner(client):
    alice_peer = _register_producer(client, ALICE_TOKEN, ALICE, MINI_META)
    _register_producer(client, BOB_TOKEN, BOB, DUCK_META)

    r = client.get("/api/robot-status", headers=_bearer(ALICE_TOKEN))
    assert r.status_code == 200
    robots = r.json()["robots"]
    assert len(robots) == 1
    row = robots[0]
    assert row["peerId"] == alice_peer.peer_id
    assert row["robotName"] == "mini1"
    assert row["busy"] is False
    assert row["activeApp"] is None
    assert row["meta"] == MINI_META
    assert row["last_seen_age_seconds"] >= 0

    r = client.get("/api/robot-status", headers=_bearer(BOB_TOKEN))
    assert [x["robotName"] for x in r.json()["robots"]] == ["duck1"]


def test_robot_status_accepts_deprecated_query_token(client):
    _register_producer(client, ALICE_TOKEN, ALICE, MINI_META)
    r = client.get("/api/robot-status", params={"token": ALICE_TOKEN})
    assert r.status_code == 200
    assert len(r.json()["robots"]) == 1


# ----------------------------------------------------------------------
# GET /api/debug/peers
# ----------------------------------------------------------------------


def test_debug_peers_requires_token(client):
    r = client.get("/api/debug/peers")
    assert r.status_code == 401
    assert r.json() == {"detail": "Missing token"}


def test_debug_peers_filters_by_owner_and_includes_consumers(client):
    _register_producer(client, ALICE_TOKEN, ALICE, MINI_META)
    _register_producer(client, BOB_TOKEN, BOB, DUCK_META)
    # A bare SSE peer (consumer-to-be) of alice's: not in producers, but
    # must still show up in her debug dump.
    consumer = _bind("tok-alice-phone", ALICE)
    token_cache["tok-alice-phone"] = (ALICE, time.monotonic() + TOKEN_CACHE_TTL_SECONDS)

    r = client.get("/api/debug/peers", headers=_bearer(ALICE_TOKEN))
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data["now"], float)
    rows = {row["peerId"]: row for row in data["peers"]}
    assert len(rows) == 2
    assert consumer.peer_id in rows
    assert rows[consumer.peer_id]["in_producers"] is False
    assert rows[consumer.peer_id]["role"] is None
    producer_rows = [row for row in rows.values() if row["in_producers"]]
    assert len(producer_rows) == 1
    assert producer_rows[0]["meta"] == MINI_META
    assert producer_rows[0]["connected"] is True
    assert set(producer_rows[0]) == {
        "peerId", "role", "connected", "in_producers", "session_id",
        "partner_id", "meta", "last_seen", "last_seen_age_seconds",
    }
