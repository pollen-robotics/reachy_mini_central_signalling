"""Token-validation layer (``hf_auth.TokenValidator``) and log hygiene.

Each test builds a fresh ``TokenValidator`` wired to a fake whoami client
(controllable status, latency and a gate to hold calls in flight) and a
fake monotonic clock. Route-level tests swap ``app.hf_validator`` for such
an instance. Only the pool-queuing test opens real sockets, to a local
listener; nothing here talks to huggingface.co.

Run with::

    python -m pytest test_auth_cache.py -v
"""

from __future__ import annotations

import asyncio
import logging
import re
import time

import httpx
import pytest
from fastapi.testclient import TestClient

import app as app_module
import hf_auth
from hf_auth import (
    TOKEN_CACHE_STALE_GRACE_SECONDS,
    TOKEN_CACHE_TTL_JITTER_SECONDS,
    TOKEN_CACHE_TTL_SECONDS,
    TOKEN_NEGATIVE_CACHE_SECONDS,
    TOKEN_REFRESH_RETRY_SECONDS,
    TokenValidationUnavailable,
    TokenValidator,
    token_hash,
)

T0 = 10_000.0


# ----------------------------------------------------------------------
# Fakes / fixtures
# ----------------------------------------------------------------------


class _Resp:
    def __init__(self, status_code: int, body=None):
        self.status_code = status_code
        self._body = body

    def json(self):
        if isinstance(self._body, BaseException):
            raise self._body
        return self._body


class FakeWhoami:
    """Controllable whoami backend.

    ``responder(token)`` returns ``(status, username)``, ``(status, body,
    "raw")`` for an arbitrary JSON body, or an exception instance to raise.
    ``gate`` (when set) holds every call in flight until released. Every
    client the factory builds is recorded with the kwargs it got.
    """

    def __init__(self):
        self.calls = 0
        self.latency = 0.0
        self.gate: asyncio.Event | None = None
        self.responder = lambda token: (200, "alice")
        self.clients: list[_FakeClient] = []
        self.factory_kwargs: list[dict] = []

    def factory(self, **kwargs):
        self.factory_kwargs.append(kwargs)
        client = _FakeClient(self)
        self.clients.append(client)
        return client


class _FakeClient:
    def __init__(self, backend: FakeWhoami):
        self.backend = backend
        self.is_closed = False

    async def get(self, url, headers=None, **kwargs):
        assert url == hf_auth.WHOAMI_URL
        backend = self.backend
        token = headers["Authorization"].removeprefix("Bearer ")
        backend.calls += 1
        if backend.gate is not None:
            await backend.gate.wait()
        if backend.latency:
            await asyncio.sleep(backend.latency)
        outcome = backend.responder(token)
        if isinstance(outcome, BaseException):
            raise outcome
        if len(outcome) == 3:
            return _Resp(outcome[0], outcome[1])
        status, name = outcome
        return _Resp(status, {"name": name})

    async def aclose(self):
        self.is_closed = True


class FakeClock:
    def __init__(self, now: float = T0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def backend():
    return FakeWhoami()


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def make(backend, clock):
    def _make(**overrides) -> TokenValidator:
        kwargs = {"client_factory": backend.factory, "clock": clock}
        kwargs.update(overrides)
        return TokenValidator(**kwargs)

    return _make


@pytest.fixture
def v(make) -> TokenValidator:
    return make()


async def _drain(validator: TokenValidator) -> None:
    while validator._inflight:
        await asyncio.gather(*list(validator._inflight.values()), return_exceptions=True)


def _rejecting(valid: dict[str, str]):
    """Responder: tokens in ``valid`` map to usernames, everything else 401."""
    return lambda token: (200, valid[token]) if token in valid else (401, "")


INCONCLUSIVE_OUTCOMES = [
    (429, ""),
    (500, ""),
    (503, ""),
    ConnectionError("down"),
    asyncio.TimeoutError(),
    httpx.PoolTimeout("pool full"),
    (200, ValueError("not json"), "raw"),
]
INCONCLUSIVE_IDS = ["429", "500", "503", "network", "timeout", "pool-timeout", "bad-json"]


# ----------------------------------------------------------------------
# Positive cache, stale-while-revalidate, single-flight
# ----------------------------------------------------------------------


async def test_fresh_hit_makes_no_call(v, backend, clock):
    v.token_cache["tok"] = ("alice", clock() + 10.0)
    for _ in range(10):
        assert await v.validate("tok") == "alice"
    await _drain(v)
    assert backend.calls == 0


async def test_expired_entry_served_stale_with_one_background_refresh(v, backend, clock):
    v.token_cache["tok"] = ("alice", clock() - 1.0)
    backend.gate = asyncio.Event()

    # 50 concurrent callers all answered from cache while HF is held.
    results = await asyncio.wait_for(
        asyncio.gather(*(v.validate("tok") for _ in range(50))), timeout=1.0
    )
    assert results == ["alice"] * 50
    await asyncio.sleep(0.01)
    assert backend.calls == 1
    assert v.inflight_count == 1
    assert v.background_count == 1

    backend.gate.set()
    await _drain(v)
    assert backend.calls == 1
    _, expires_at = v.token_cache["tok"]
    assert clock() + TOKEN_CACHE_TTL_SECONDS <= expires_at
    assert expires_at <= clock() + TOKEN_CACHE_TTL_SECONDS + TOKEN_CACHE_TTL_JITTER_SECONDS
    assert v.inflight_count == 0
    assert v.background_count == 0


async def test_first_time_validation_is_single_flight(v, backend):
    backend.gate = asyncio.Event()
    waiters = [asyncio.create_task(v.validate("new-tok")) for _ in range(50)]
    await asyncio.sleep(0.01)
    assert backend.calls == 1
    backend.gate.set()
    assert await asyncio.gather(*waiters) == ["alice"] * 50
    assert backend.calls == 1
    assert v.token_cache["new-tok"][0] == "alice"
    assert v.inflight_count == 0
    # Joining an in-flight call does not draw from the unknown-token bucket.
    assert v.bucket.tokens == hf_auth.WHOAMI_UNKNOWN_BURST - 1


async def test_cancelled_waiter_does_not_break_the_shared_call(v, backend):
    backend.gate = asyncio.Event()
    first = asyncio.create_task(v.validate("new-tok"))
    second = asyncio.create_task(v.validate("new-tok"))
    await asyncio.sleep(0.01)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    backend.gate.set()
    assert await second == "alice"
    assert backend.calls == 1
    assert v.inflight_count == 0


async def test_refresh_rejection_drops_entry_and_negative_caches(v, backend, clock):
    v.token_cache["tok-revoked"] = ("alice", clock() - 1.0)
    backend.responder = lambda token: (401, "")

    assert await v.validate("tok-revoked") == "alice"  # stale, refresh kicked
    await _drain(v)
    assert "tok-revoked" not in v.token_cache
    assert token_hash("tok-revoked") in v.negative_cache
    assert await v.validate("tok-revoked") is None
    assert backend.calls == 1
    assert v.stats["whoami_rejected_total"] == 1
    assert v.stats["negative_cache_hits_total"] == 1


@pytest.mark.parametrize("outcome", INCONCLUSIVE_OUTCOMES, ids=INCONCLUSIVE_IDS)
async def test_inconclusive_refresh_keeps_stale_identity(v, backend, clock, outcome):
    v.token_cache["tok"] = ("alice", clock() - 1.0)
    backend.responder = lambda token: outcome

    assert await v.validate("tok") == "alice"
    await _drain(v)
    assert v.token_cache["tok"][0] == "alice"
    assert backend.calls == 1
    assert v.stats["whoami_errors_total"] == 1

    # Backed off: still served stale, no new call until the retry delay.
    assert await v.validate("tok") == "alice"
    await _drain(v)
    assert backend.calls == 1
    clock.advance(TOKEN_REFRESH_RETRY_SECONDS)
    backend.responder = lambda token: (200, "alice")
    assert await v.validate("tok") == "alice"
    await _drain(v)
    assert backend.calls == 2
    assert v.token_cache["tok"][1] > clock()


@pytest.mark.parametrize("outcome", INCONCLUSIVE_OUTCOMES, ids=INCONCLUSIVE_IDS)
async def test_inconclusive_unknown_token_is_503_never_none(v, backend, outcome):
    """No verdict for a never-seen token: retryable 503, not a 401-shaped None."""
    backend.gate = asyncio.Event()
    backend.responder = lambda token: outcome
    waiters = [asyncio.create_task(v.validate("never-seen")) for _ in range(20)]
    await asyncio.sleep(0.01)
    backend.gate.set()
    results = await asyncio.gather(*waiters, return_exceptions=True)
    assert all(isinstance(r, TokenValidationUnavailable) for r in results), results
    assert results[0].status_code == 503
    assert results[0].headers == {"Retry-After": str(hf_auth.WHOAMI_SHED_RETRY_AFTER_SECONDS)}
    assert backend.calls == 1
    assert "never-seen" not in v.token_cache
    assert token_hash("never-seen") not in v.negative_cache


@pytest.mark.parametrize(
    "body",
    [{}, {"name": None}, {"name": ""}, {"name": 123}, {"name": ["x"]}, ["alice"], "alice", None],
    ids=["missing", "none", "empty", "int", "list", "array-body", "string-body", "null-body"],
)
async def test_200_without_a_usable_name_is_inconclusive(v, backend, clock, body):
    backend.responder = lambda token: (200, body, "raw")
    with pytest.raises(TokenValidationUnavailable):
        await v.validate("unknown")
    assert "unknown" not in v.token_cache

    v.token_cache["known"] = ("alice", clock() - 1.0)
    assert await v.validate("known") == "alice"
    await _drain(v)
    assert v.token_cache["known"][0] == "alice"
    assert v.token_cache["known"][1] < clock(), "a non-identity must not refresh the TTL"
    assert v.stats["whoami_errors_total"] == 2


async def test_real_timeout_is_inconclusive(make, backend, clock):
    v = make(total_timeout=0.05)
    backend.latency = 1.0
    v.token_cache["tok"] = ("alice", clock() - 1.0)
    assert await v.validate("tok") == "alice"
    await asyncio.wait_for(_drain(v), timeout=1.0)
    assert v.token_cache["tok"][0] == "alice"
    assert v.stats["whoami_errors_total"] == 1
    # Unknown token during the same slowness: bounded wait, then 503.
    with pytest.raises(TokenValidationUnavailable):
        await asyncio.wait_for(v.validate("other"), timeout=1.0)


async def _slow_whoami_server(latency: float):
    writers = []

    async def handle(reader, writer):
        writers.append(writer)
        try:
            while True:
                if not await reader.readline():
                    return
                while (await reader.readline()) not in (b"\r\n", b""):
                    pass
                await asyncio.sleep(latency)
                body = b'{"name":"u"}'
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                    b"Content-Length: %d\r\n\r\n" % len(body) + body
                )
                await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    server.open_writers = writers
    return server


async def _stop_server(server) -> None:
    server.close()
    for writer in server.open_writers:
        writer.close()
    await asyncio.sleep(0.05)  # let handlers observe the close
    await server.wait_closed()


async def test_calls_queued_on_a_full_pool_get_503_not_401():
    """The reviewer's regression: pool-queued calls that hit the total
    timeout must be 503, never 401. Real httpx client, tiny pool."""
    server = await _slow_whoami_server(0.3)
    port = server.sockets[0].getsockname()[1]
    v = TokenValidator(
        whoami_url=f"http://127.0.0.1:{port}/api/whoami-v2",
        max_connections=2,
        total_timeout=0.5,
    )
    try:
        results = await asyncio.gather(
            *(v.validate(f"tok-{i}") for i in range(8)), return_exceptions=True
        )
    finally:
        await v.aclose()
        await _stop_server(server)
    assert None not in results
    ok = [r for r in results if r == "u"]
    shed = [r for r in results if isinstance(r, TokenValidationUnavailable)]
    assert len(ok) + len(shed) == 8
    assert ok and shed


async def test_entry_beyond_stale_grace_is_treated_as_unknown(v, backend, clock):
    v.token_cache["tok"] = ("alice", clock() - TOKEN_CACHE_STALE_GRACE_SECONDS - 1.0)
    backend.responder = lambda token: (401, "")
    assert await v.validate("tok") is None
    assert "tok" not in v.token_cache
    assert backend.calls == 1


async def test_dev_seeded_entries_never_refresh(v, backend, clock):
    v.seed("dev-tok", "dev")
    clock.advance(10 * TOKEN_CACHE_STALE_GRACE_SECONDS)
    assert await v.validate("dev-tok") == "dev"
    v.prune()
    assert "dev-tok" in v.token_cache
    assert backend.calls == 0


async def test_background_refreshes_are_bounded(make, backend, clock):
    v = make(max_background_refreshes=3)
    backend.gate = asyncio.Event()
    for i in range(10):
        v.token_cache[f"tok-{i}"] = (f"user-{i}", clock() - 1.0)
    for i in range(10):
        assert await v.validate(f"tok-{i}") == f"user-{i}"
    await asyncio.sleep(0.01)
    assert v.background_count == 3
    assert backend.calls == 3
    backend.gate.set()
    await _drain(v)
    # Skipped tokens are picked up by their next requests, 3 at a time.
    for _ in range(3):
        for i in range(10):
            await v.validate(f"tok-{i}")
        await _drain(v)
    assert backend.calls == 10
    assert all(v.token_cache[f"tok-{i}"][1] > clock() for i in range(10))


# ----------------------------------------------------------------------
# Negative cache
# ----------------------------------------------------------------------


async def test_negative_cache_expires_after_its_ttl(v, backend, clock):
    backend.responder = lambda token: (401, "")
    assert await v.validate("bad") is None
    assert backend.calls == 1

    clock.advance(TOKEN_NEGATIVE_CACHE_SECONDS - 1.0)
    assert await v.validate("bad") is None
    assert backend.calls == 1

    clock.advance(1.0)
    assert await v.validate("bad") is None
    assert backend.calls == 2


async def test_negative_cache_is_pruned_and_bounded(make, backend, clock):
    v = make(negative_cache_max_entries=5)
    backend.responder = lambda token: (401, "")
    for i in range(8):
        assert await v.validate(f"bad-{i}") is None
        clock.advance(1.0)
    assert len(v.negative_cache) == 5
    assert token_hash("bad-0") not in v.negative_cache  # oldest evicted
    assert token_hash("bad-7") in v.negative_cache

    clock.advance(TOKEN_NEGATIVE_CACHE_SECONDS)
    v.prune()
    assert len(v.negative_cache) == 0


async def test_a_successful_validation_clears_a_negative_entry(v, backend, clock):
    backend.responder = lambda token: (401, "")
    assert await v.validate("flaky") is None
    clock.advance(TOKEN_NEGATIVE_CACHE_SECONDS)
    backend.responder = lambda token: (200, "alice")
    assert await v.validate("flaky") == "alice"
    assert token_hash("flaky") not in v.negative_cache


async def test_raw_tokens_never_stored_or_logged(v, backend, clock, caplog):
    secret_unknown = "hf_SECRETunknownTOKEN0001"
    secret_known = "hf_SECRETknownTOKEN0002"
    v.token_cache[secret_known] = ("alice", clock() - 1.0)
    backend.responder = lambda token: (401, "")
    with caplog.at_level(logging.DEBUG):
        for _ in range(3):
            assert await v.validate(secret_unknown) is None
        assert await v.validate(secret_known) == "alice"
        await _drain(v)
        assert await v.validate(secret_known) is None
        backend.responder = lambda token: ConnectionError("down")
        with pytest.raises(TokenValidationUnavailable):
            await v.validate("hf_SECRETthirdTOKEN0003")
        v.maybe_log_summary(clock() + 3600)

    for key in v.negative_cache:
        assert re.fullmatch(r"[0-9a-f]{32}", key)
    assert "SECRET" not in caplog.text
    assert caplog.records


# ----------------------------------------------------------------------
# Unknown-token cap
# ----------------------------------------------------------------------


async def test_unknown_tokens_beyond_the_bucket_are_shed_without_calls(make, backend, clock):
    burst, rate = 50, 20
    v = make(unknown_burst=burst, unknown_per_second=rate)
    backend.responder = _rejecting({})
    for i in range(burst):
        assert await v.validate(f"garbage-{i}") is None
    assert backend.calls == burst

    for i in range(burst, burst + 10):
        with pytest.raises(TokenValidationUnavailable) as exc:
            await v.validate(f"garbage-{i}")
        assert exc.value.status_code == 503
        assert exc.value.headers == {
            "Retry-After": str(hf_auth.WHOAMI_SHED_RETRY_AFTER_SECONDS)
        }
    assert backend.calls == burst
    assert v.stats["unknown_token_shed_total"] == 10

    # A known token's refresh is not subject to the cap...
    v.token_cache["known"] = ("alice", clock() - 1.0)
    backend.responder = _rejecting({"known": "alice"})
    assert await v.validate("known") == "alice"
    await _drain(v)
    assert backend.calls == burst + 1
    assert v.token_cache["known"][1] > clock()
    # ...nor are negatively cached tokens (answered locally).
    assert await v.validate("garbage-0") is None
    assert backend.calls == burst + 1

    # The bucket refills at the configured rate.
    clock.advance(1.0)
    for i in range(rate):
        assert await v.validate(f"later-{i}") is None
    with pytest.raises(TokenValidationUnavailable):
        await v.validate("later-overflow")
    assert backend.calls == burst + 1 + rate


async def test_whoami_calls_are_bounded_under_a_flood(v, backend, clock):
    """20 s of 100 req/s distinct garbage + 100 req/s of one revoked token."""
    backend.responder = _rejecting({})
    shed = 0
    for tick in range(20 * 100):
        if tick % 100 == 0 and tick:
            clock.advance(1.0)
        assert await v.validate("revoked") is None
        try:
            await v.validate(f"garbage-{tick}")
        except TokenValidationUnavailable:
            shed += 1
    max_calls = hf_auth.WHOAMI_UNKNOWN_BURST + hf_auth.WHOAMI_UNKNOWN_MAX_PER_SECOND * 19
    assert backend.calls <= max_calls
    assert shed == 2000 - (backend.calls - 1)


# ----------------------------------------------------------------------
# Jitter, shared client, shutdown
# ----------------------------------------------------------------------


async def test_ttl_jitter_within_bounds_and_spread(v, backend, clock):
    backend.responder = lambda token: (200, "u")
    for i in range(200):
        assert await v.validate(f"t-{i}") == "u"
    ttls = [v.token_cache[f"t-{i}"][1] - clock() for i in range(200)]
    assert all(
        TOKEN_CACHE_TTL_SECONDS <= ttl <= TOKEN_CACHE_TTL_SECONDS + TOKEN_CACHE_TTL_JITTER_SECONDS
        for ttl in ttls
    )
    assert max(ttls) - min(ttls) > TOKEN_CACHE_TTL_JITTER_SECONDS / 2


async def test_client_factory_gets_explicit_timeouts_and_limits(v, backend):
    await v.validate("a")
    await v.validate("b")
    assert len(backend.factory_kwargs) == 1, "one shared client"
    kwargs = backend.factory_kwargs[0]
    assert set(kwargs) == {"timeout", "limits"}
    timeout, limits = kwargs["timeout"], kwargs["limits"]
    assert timeout.connect == hf_auth.WHOAMI_CONNECT_TIMEOUT_SECONDS == 5.0
    assert timeout.read == timeout.pool == hf_auth.WHOAMI_TOTAL_TIMEOUT_SECONDS == 10.0
    assert limits.max_connections == hf_auth.WHOAMI_MAX_CONNECTIONS
    assert limits.max_connections >= hf_auth.WHOAMI_UNKNOWN_BURST + hf_auth.WHOAMI_MAX_BACKGROUND_REFRESHES
    assert limits.max_keepalive_connections == 32
    # The default factory really is httpx's client and accepts them.
    real = TokenValidator().client_factory(**kwargs)
    try:
        assert isinstance(real, httpx.AsyncClient)
        assert real.timeout == timeout
    finally:
        await real.aclose()


async def test_aclose_cancels_inflight_and_closes_client(v, backend, clock):
    v.token_cache["tok-0"] = ("alice", clock() - 1.0)
    await v.validate("ok")
    backend.gate = asyncio.Event()
    assert await v.validate("tok-0") == "alice"
    await asyncio.sleep(0.01)
    assert v.inflight_count == 1
    client = backend.clients[0]

    await v.aclose()
    assert client.is_closed
    assert v.inflight_count == 0 and v.background_count == 0
    assert v.token_cache["tok-0"][0] == "alice"


async def test_lifespan_closes_client_and_shutdown_is_bounded(backend, monkeypatch, caplog):
    v = TokenValidator(client_factory=backend.factory)
    monkeypatch.setattr(app_module, "hf_validator", v)
    published = []

    class _Publisher:
        async def run(self):
            await asyncio.Event().wait()

        async def final_publish(self):
            published.append(True)

    monkeypatch.setattr(app_module, "usage_publisher", _Publisher())

    async with app_module._lifespan(app_module.app):
        await v.validate("tok")
    assert backend.clients[0].is_closed
    assert published == [True]

    # A wedged close must not block fleet usage's final publish.
    published.clear()
    v2 = TokenValidator(client_factory=backend.factory)

    async def wedged_close():
        await asyncio.Event().wait()

    monkeypatch.setattr(v2, "aclose", wedged_close)
    monkeypatch.setattr(app_module, "hf_validator", v2)
    monkeypatch.setattr(app_module, "WHOAMI_SHUTDOWN_TIMEOUT_SECONDS", 0.05)
    started = time.monotonic()
    with caplog.at_level(logging.WARNING, logger="app"):
        async with app_module._lifespan(app_module.app):
            pass
    assert time.monotonic() - started < 1.0
    assert published == [True]
    assert any("shutdown timed out" in r.getMessage() for r in caplog.records)


# ----------------------------------------------------------------------
# Auth logging
# ----------------------------------------------------------------------


def _messages(caplog, needle: str) -> list[logging.LogRecord]:
    return [r for r in caplog.records if needle in r.getMessage()]


async def test_repeated_rejections_log_once(v, backend, clock, caplog):
    backend.responder = lambda token: (401, "")
    with caplog.at_level(logging.DEBUG, logger="hf_auth"):
        for _ in range(100):
            assert await v.validate("revoked") is None
    rejected = _messages(caplog, "Token rejected by HF")
    assert len(rejected) == 1
    assert rejected[0].levelno == logging.WARNING
    assert backend.calls == 1

    # The aggregate line carries the 99 locally answered retries.
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="hf_auth"):
        assert v.maybe_log_summary(clock() + 60.0)
    (summary,) = _messages(caplog, "Auth summary")
    assert "negative_cache_hits=99" in summary.getMessage()
    assert "rejected=1" in summary.getMessage()
    # Quiet minute: no line.
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="hf_auth"):
        assert not v.maybe_log_summary(clock() + 120.0)
    assert not _messages(caplog, "Auth summary")


async def test_distinct_rejections_warn_a_bounded_number_of_times(make, backend, clock, caplog):
    v = make(unknown_burst=1000)
    backend.responder = lambda token: (401, "")
    cap = hf_auth.AUTH_REJECTION_WARNINGS_PER_INTERVAL
    with caplog.at_level(logging.DEBUG, logger="hf_auth"):
        for i in range(cap + 40):
            assert await v.validate(f"garbage-{i}") is None
    rejected = _messages(caplog, "Token rejected by HF")
    assert len(rejected) == cap + 40
    assert [r.levelno for r in rejected].count(logging.WARNING) == cap

    # Next interval: warnings resume.
    caplog.clear()
    clock.advance(hf_auth.AUTH_SUMMARY_LOG_INTERVAL_SECONDS)
    with caplog.at_level(logging.WARNING, logger="hf_auth"):
        assert await v.validate("garbage-next") is None
    assert len(_messages(caplog, "Token rejected by HF")) == 1


async def test_shed_requests_are_summarised_not_logged_each(make, clock, caplog):
    v = make(unknown_burst=0, unknown_per_second=0.0)
    with caplog.at_level(logging.DEBUG, logger="hf_auth"):
        for i in range(50):
            with pytest.raises(TokenValidationUnavailable):
                await v.validate(f"g-{i}")
        assert v.maybe_log_summary(clock() + 60.0)
    assert len(caplog.records) == 1
    assert "unknown_token_shed=50" in caplog.records[0].getMessage()


async def test_summary_reports_extra_counters(make, clock, caplog):
    extra = {"access_log_filtered": 0}
    v = make(summary_extras=lambda: dict(extra))
    extra["access_log_filtered"] = 1234
    with caplog.at_level(logging.INFO, logger="hf_auth"):
        assert v.maybe_log_summary(clock() + 60.0)
        assert not v.maybe_log_summary(clock() + 120.0)  # no movement
    (line,) = _messages(caplog, "Auth summary")
    assert line.getMessage().endswith(" access_log_filtered=1234")


async def test_validated_is_info_first_time_debug_on_refresh(v, clock, caplog, backend):
    with caplog.at_level(logging.DEBUG, logger="hf_auth"):
        assert await v.validate("tok") == "alice"
        clock.advance(TOKEN_CACHE_TTL_SECONDS + TOKEN_CACHE_TTL_JITTER_SECONDS + 1)
        assert await v.validate("tok") == "alice"
        await _drain(v)
    first = _messages(caplog, "Token validated for user: alice")
    assert [r.levelno for r in first] == [logging.INFO]
    refresh = _messages(caplog, "re-validated for user: alice")
    assert [r.levelno for r in refresh] == [logging.DEBUG]
    assert backend.calls == 2


async def test_inconclusive_warning_is_rate_limited(v, backend, clock, caplog):
    backend.responder = lambda token: (503, "")
    for i in range(20):
        v.token_cache[f"tok-{i}"] = ("alice", clock() - 1.0)
    with caplog.at_level(logging.DEBUG, logger="hf_auth"):
        for i in range(20):
            await v.validate(f"tok-{i}")
        await _drain(v)
    inconclusive = _messages(caplog, "Token validation inconclusive")
    assert backend.calls == 20
    assert [r.levelno for r in inconclusive].count(logging.WARNING) == 1


# ----------------------------------------------------------------------
# Log format and access-log hygiene (app.py)
# ----------------------------------------------------------------------


def test_log_lines_carry_iso_utc_timestamps():
    formatter = logging.Formatter(app_module.LOG_FORMAT, app_module.LOG_DATEFMT)
    record = logging.LogRecord("app", logging.INFO, __file__, 1, "hello", None, None)
    record.created = 1790842880.0  # 2026-10-01T08:21:20Z
    assert formatter.format(record) == "2026-10-01T08:21:20Z INFO app: hello"


def _access_record(method, path, status_code):
    return logging.LogRecord(
        "uvicorn.access", logging.INFO, __file__, 1,
        '%s - "%s %s HTTP/%s" %d', ("1.2.3.4:5", method, path, "1.1", status_code), None,
    )


def test_uvicorn_access_log_gets_timestamps():
    from uvicorn.logging import AccessFormatter

    logger = logging.getLogger("uvicorn.access")
    handler = logging.StreamHandler()
    handler.setFormatter(
        AccessFormatter(
            fmt='%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
            use_colors=False,
        )
    )
    logger.addHandler(handler)
    try:
        app_module._timestamp_uvicorn_loggers()
        record = _access_record("GET", "/events", 401)
        record.created = 1790842880.0
        line = handler.formatter.format(record)
        assert line.startswith("2026-10-01T08:21:20Z INFO:")
        assert '"GET /events HTTP/1.1" 401' in line
        # Idempotent: a second pass does not double the prefix.
        app_module._timestamp_uvicorn_loggers()
        assert handler.formatter.format(record).count("2026-10-01T08:21:20Z") == 1
    finally:
        logger.removeHandler(handler)


def test_access_log_redacts_query_tokens():
    flt = app_module._AccessLogFilter()
    record = _access_record("GET", "/events?token=hf_SECRET123&x=1", 200)
    assert flt.filter(record)
    assert record.getMessage() == '1.2.3.4:5 - "GET /events?token=***&x=1 HTTP/1.1" 200'
    record = _access_record("GET", "/api/debug/peers?a=1&TOKEN=hf_SECRET", 401)
    assert flt.filter(record)
    assert "hf_SECRET" not in record.getMessage()
    assert "TOKEN=***" in record.getMessage()
    # Non-access-shaped record: rendered message redacted too.
    odd = logging.LogRecord("uvicorn.access", logging.INFO, __file__, 1, "weird ?token=%s", ("hf_SECRET",), None)
    assert flt.filter(odd)
    assert "hf_SECRET" not in odd.getMessage()
    # Through the real uvicorn formatter, end to end.
    from uvicorn.logging import AccessFormatter

    fmt = AccessFormatter(fmt='%(client_addr)s - "%(request_line)s" %(status_code)s', use_colors=False)
    record = _access_record("GET", "/api/robot-status?token=hf_SECRET", 401)
    assert flt.filter(record)
    line = fmt.format(record)
    assert "hf_SECRET" not in line and "token=***" in line


def test_access_log_drops_only_successful_heartbeats_and_polls():
    flt = app_module._AccessLogFilter()
    dropped = [
        ("POST", "/send", 200),
        ("GET", "/api/robot-status", 200),
        ("GET", "/api/robot-status?token=hf_x", 204),
    ]
    kept = [
        ("POST", "/send", 400),
        ("POST", "/send", 401),
        ("POST", "/send", 429),
        ("POST", "/send", 503),
        ("GET", "/api/robot-status", 401),
        ("GET", "/events", 200),
        ("GET", "/health", 200),
        ("GET", "/api/debug/peers", 200),
        ("GET", "/send", 405),
        ("POST", "/send/x", 200),
    ]
    for method, path, code in dropped:
        assert not flt.filter(_access_record(method, path, code)), (method, path, code)
    for method, path, code in kept:
        assert flt.filter(_access_record(method, path, code)), (method, path, code)
    assert flt.filtered == len(dropped)


def test_access_filter_is_installed_and_feeds_the_summary():
    assert app_module._access_log_filter in logging.getLogger("uvicorn.access").filters
    extras = app_module.hf_validator.summary_extras()
    assert extras == {"access_log_filtered": app_module._access_log_filter.filtered}


def test_httpx_whoami_request_lines_are_filtered():
    flt = app_module._DropWhoamiRequestLines()

    def rec(msg, level=logging.INFO):
        return logging.LogRecord("httpx", level, __file__, 1, msg, None, None)

    assert not flt.filter(rec('HTTP Request: GET https://huggingface.co/api/whoami-v2 "HTTP/1.1 401"'))
    assert flt.filter(rec('HTTP Request: GET https://huggingface.co/api/datasets/x "HTTP/1.1 200"'))
    assert flt.filter(rec("whoami-v2 /api/whoami-v2 failure", logging.WARNING))


async def test_producer_registered_logged_only_on_change(caplog):
    server = app_module.SignalingServer()
    peer = server.get_or_create_peer("tok-log", "alice")
    meta = {"name": "r1", "hardware_id": "hw1"}
    msg = {"type": "setPeerStatus", "roles": ["producer"], "meta": meta}

    def infos():
        return [
            r for r in caplog.records
            if "Producer registered" in r.getMessage() and r.levelno == logging.INFO
        ]

    with caplog.at_level(logging.DEBUG, logger="app"):
        await server.handle_message(peer, dict(msg, meta=dict(meta)))
        await server.handle_message(peer, dict(msg, meta=dict(meta)))  # heartbeat
        assert len(infos()) == 1
        debugs = [r for r in caplog.records if "Producer registered" in r.getMessage()]
        assert [r.levelno for r in debugs] == [logging.INFO, logging.DEBUG]

        await server.handle_message(peer, dict(msg, meta={**meta, "name": "r1-renamed"}))
        assert len(infos()) == 2

        # Withdrawn then re-registered with the same meta: new registration.
        await server.handle_message(peer, {"type": "setPeerStatus", "roles": [], "meta": {**meta, "name": "r1-renamed"}})
        await server.handle_message(peer, dict(msg, meta={**meta, "name": "r1-renamed"}))
        assert len(infos()) == 3

        big = {**meta, "blob": "x" * 5000}
        await server.handle_message(peer, dict(msg, meta=big))
    assert len(infos()[-1].getMessage()) < 400


# ----------------------------------------------------------------------
# Routes
# ----------------------------------------------------------------------


@pytest.fixture
def client(backend, monkeypatch):
    snapshot = dict(app_module.token_cache)
    app_module.token_cache.clear()
    validator = TokenValidator(client_factory=backend.factory, token_cache=app_module.token_cache)
    monkeypatch.setattr(app_module, "hf_validator", validator)
    signaling = app_module.signaling
    for d in (signaling.peers, signaling.producers, signaling.sessions, signaling.token_to_peer):
        d.clear()
    app_module._health_cache_reset()
    try:
        yield TestClient(app_module.app)
    finally:
        for d in (signaling.peers, signaling.producers, signaling.sessions, signaling.token_to_peer):
            d.clear()
        app_module._health_cache_reset()
        app_module.token_cache.clear()
        app_module.token_cache.update(snapshot)


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


ROUTES = [
    ("GET", "/events"),
    ("GET", "/api/robot-status"),
    ("GET", "/api/debug/peers"),
    ("POST", "/send"),
]


@pytest.mark.parametrize("method,path", ROUTES)
def test_negatively_cached_token_is_401_without_hf_call(client, backend, method, path):
    v = app_module.hf_validator
    v._negative_add(token_hash("revoked"), v.clock())
    r = client.request(method, path, headers=_bearer("revoked"), json={"type": "list"})
    assert r.status_code == 401
    assert r.json() == {"detail": "Invalid token"}
    assert backend.calls == 0


@pytest.mark.parametrize("method,path", ROUTES)
def test_shed_unknown_token_is_503_with_retry_after(client, backend, method, path):
    app_module.hf_validator.bucket = hf_auth.TokenBucket(0.0, 0)
    r = client.request(method, path, headers=_bearer("never-seen"), json={"type": "list"})
    assert r.status_code == 503
    assert r.headers["retry-after"] == str(hf_auth.WHOAMI_SHED_RETRY_AFTER_SECONDS)
    assert r.json() == {"detail": "Token validation temporarily unavailable, retry later"}
    assert backend.calls == 0


@pytest.mark.parametrize("outcome", INCONCLUSIVE_OUTCOMES, ids=INCONCLUSIVE_IDS)
@pytest.mark.parametrize("method,path", [("GET", "/events"), ("GET", "/api/robot-status")])
def test_inconclusive_whoami_on_unknown_token_is_503_never_401(client, backend, outcome, method, path):
    backend.responder = lambda token: outcome
    for i in range(3):
        r = client.request(method, path, headers=_bearer(f"never-seen-{i}"))
        assert r.status_code == 503
        assert r.headers["retry-after"] == str(hf_auth.WHOAMI_SHED_RETRY_AFTER_SECONDS)
    assert backend.calls == 3


def test_rejected_then_retried_robot_status_costs_one_call(client, backend):
    backend.responder = lambda token: (401, "")
    for _ in range(20):
        r = client.get("/api/robot-status", headers=_bearer("revoked"))
        assert r.status_code == 401
    assert backend.calls == 1


def test_health_auth_block(client, backend):
    app_module.token_cache["tok-a"] = ("alice", time.monotonic() + 100.0)
    backend.responder = lambda token: (401, "")
    assert client.get("/api/robot-status", headers=_bearer("bad")).status_code == 401
    app_module._health_cache_reset()
    auth = client.get("/health").json()["auth"]
    assert auth == {
        "cache_size": 1,
        "negative_cache_size": 1,
        "whoami_calls_total": 1,
        "whoami_rejected_total": 1,
        "whoami_errors_total": 0,
        "unknown_token_shed_total": 0,
    }
