"""HuggingFace token validation for central: caches, single-flight, budget.

Every authenticated request carries an HF token that central checks
against HF's ``whoami-v2`` endpoint. That is an outbound HTTPS round-trip
counted against the Space's whoami quota, and the fleet (~280 robots
heartbeating every 10 s and polling every 30 s) would otherwise make
thousands per minute. ``TokenValidator`` bounds that cost:

- **Positive cache** (``token_cache``: token -> (username, expires_at)).
  A validated token is trusted for ``TOKEN_CACHE_TTL_SECONDS`` plus a
  random ``TOKEN_CACHE_TTL_JITTER_SECONDS``: a deploy reconnects the
  whole fleet within seconds, and with a fixed TTL every entry would
  expire on the same hour mark since boot, forever.
- **Stale-while-revalidate.** An expired entry is still served while ONE
  background whoami call refreshes it, so no request waits on HF for a
  token central already knows. Only an explicit HF 401/403 drops the
  identity; network errors, timeouts, 429 and 5xx keep serving it
  (retried at most every ``TOKEN_REFRESH_RETRY_SECONDS``) for up to
  ``TOKEN_CACHE_STALE_GRACE_SECONDS`` - an HF outage or an
  attacker-induced whoami 429 must never turn into fleet-wide 401s.
- **Negative cache.** A token HF rejected is remembered for
  ``TOKEN_NEGATIVE_CACHE_SECONDS`` under a SHA-256 prefix (never raw). A
  revoked token stays revoked - a user who logs in again gets a NEW token
  string - so a client retrying a dead token in a tight loop is answered
  locally instead of costing one whoami call per retry.
- **Single-flight.** Concurrent validations of one token share one
  in-flight call; waiters ``shield`` it, so one cancelled request never
  cancels it for the others, and every waiter gets the same outcome.
- **Unknown-token budget.** A token with no cache entry at all draws
  from a global token bucket before reaching HF. When it is empty, or
  when HF gives no verdict for it, the request gets **503 +
  Retry-After** (``TokenValidationUnavailable``), never 401: a 401 says
  "your token is bad", and the daemon's central-status proxy maps 401 to
  ``token_invalid``. Every shipped client retries any other non-200
  with its normal backoff.
- **Shared client.** One keep-alive ``httpx.AsyncClient`` with explicit
  timeouts, sized to absorb a whole-fleet reconnect.
- **Quiet logs.** One WARNING per rejected token per negative-cache
  period (and at most ``AUTH_REJECTION_WARNINGS_PER_INTERVAL`` a
  minute), refreshes at DEBUG, and one per-minute summary line for the
  repetitive outcomes. Raw tokens never reach the logs.

Everything here runs on the server's single event loop, so the plain
dicts need no locking.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import time
from collections import OrderedDict
from typing import Any, Callable, Optional

import httpx
from fastapi import HTTPException, status

logger = logging.getLogger(__name__)

WHOAMI_URL = "https://huggingface.co/api/whoami-v2"
WHOAMI_CONNECT_TIMEOUT_SECONDS = 5.0
# Hard bound on one whoami call, pool wait included.
WHOAMI_TOTAL_TIMEOUT_SECONDS = 10.0

TOKEN_CACHE_TTL_SECONDS = 3600.0
TOKEN_CACHE_TTL_JITTER_SECONDS = 600.0
TOKEN_CACHE_STALE_GRACE_SECONDS = 86400.0
TOKEN_NEGATIVE_CACHE_SECONDS = 300.0
TOKEN_NEGATIVE_CACHE_MAX_ENTRIES = 50_000
TOKEN_REFRESH_RETRY_SECONDS = 60.0

# Unknown-token budget. The burst covers a whole-fleet reconnect after a
# deploy (every robot is a never-seen token for a fresh process); the
# sustained rate caps what a flood of distinct garbage tokens can cost
# once the burst is spent.
WHOAMI_UNKNOWN_MAX_PER_SECOND = float(
    os.getenv("REACHY_CENTRAL_WHOAMI_UNKNOWN_PER_SECOND", "20")
)
WHOAMI_UNKNOWN_BURST = int(os.getenv("REACHY_CENTRAL_WHOAMI_UNKNOWN_BURST", "300"))
WHOAMI_SHED_RETRY_AFTER_SECONDS = 5
# Concurrent background refreshes; beyond it the stale identity is still
# served and the next request for that token tries again.
WHOAMI_MAX_BACKGROUND_REFRESHES = 32
# Connection pool: room for a full unknown-token burst plus every
# background refresh, so a fleet reconnect never queues on the pool
# (a queued call can time out, which would shed a legitimate robot).
WHOAMI_MAX_CONNECTIONS = WHOAMI_UNKNOWN_BURST + WHOAMI_MAX_BACKGROUND_REFRESHES
WHOAMI_MAX_KEEPALIVE_CONNECTIONS = 32
WHOAMI_KEEPALIVE_EXPIRY_SECONDS = 30.0

AUTH_SUMMARY_LOG_INTERVAL_SECONDS = 60.0
AUTH_REJECTION_WARNINGS_PER_INTERVAL = 10

PUBLIC_COUNTERS = (
    "whoami_calls_total",
    "whoami_rejected_total",
    "whoami_errors_total",
    "unknown_token_shed_total",
)

# ``_whoami_and_apply`` outcome when HF gave no verdict.
_INCONCLUSIVE: Any = object()


def token_hash(token: str) -> str:
    """Non-reversible key for a token (negative cache, single-flight, logs).

    128 bits of SHA-256: a false "rejected" verdict for a valid token
    would need a collision, which is not a practical concern.
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:32]


class TokenValidationUnavailable(HTTPException):
    """503 + ``Retry-After``: no verdict for a never-seen token right now.

    Raised by ``TokenValidator.validate`` when the unknown-token budget is
    spent or HF did not answer, so every authenticated route answers the
    same way without per-route handling.
    """

    def __init__(self, retry_after: int = WHOAMI_SHED_RETRY_AFTER_SECONDS):
        super().__init__(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Token validation temporarily unavailable, retry later",
            headers={"Retry-After": str(retry_after)},
        )


class TokenBucket:
    """Plain token bucket: ``rate`` tokens/s, at most ``burst`` banked."""

    def __init__(self, rate: float, burst: float):
        self.rate = rate
        self.burst = burst
        self.tokens = float(burst)
        self.updated: Optional[float] = None

    def try_take(self, now: float) -> bool:
        if self.updated is not None and now > self.updated:
            self.tokens = min(self.burst, self.tokens + (now - self.updated) * self.rate)
        self.updated = now if self.updated is None else max(self.updated, now)
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


class TokenValidator:
    """Validates HF tokens; owns every cache, counter and the whoami client.

    ``client_factory`` is called once, lazily, as
    ``client_factory(timeout=httpx.Timeout, limits=httpx.Limits)`` and must
    return an object with ``async get(url, headers=...)`` and
    ``async aclose()``. ``clock`` is a monotonic clock. ``token_cache`` may
    be passed to share an existing positive cache dict. ``summary_extras``
    returns extra cumulative counters reported (as deltas) on the
    per-minute summary line.
    """

    def __init__(
        self,
        *,
        client_factory: Callable[..., Any] = httpx.AsyncClient,
        clock: Callable[[], float] = time.monotonic,
        token_cache: Optional[dict[str, tuple[str, float]]] = None,
        whoami_url: str = WHOAMI_URL,
        unknown_per_second: float = WHOAMI_UNKNOWN_MAX_PER_SECOND,
        unknown_burst: int = WHOAMI_UNKNOWN_BURST,
        max_background_refreshes: int = WHOAMI_MAX_BACKGROUND_REFRESHES,
        max_connections: int = WHOAMI_MAX_CONNECTIONS,
        total_timeout: float = WHOAMI_TOTAL_TIMEOUT_SECONDS,
        negative_cache_max_entries: int = TOKEN_NEGATIVE_CACHE_MAX_ENTRIES,
        summary_extras: Optional[Callable[[], dict[str, int]]] = None,
    ):
        self.client_factory = client_factory
        self.clock = clock
        self.whoami_url = whoami_url
        self.max_background_refreshes = max_background_refreshes
        self.max_connections = max_connections
        self.total_timeout = total_timeout
        self.negative_cache_max_entries = negative_cache_max_entries
        self.summary_extras = summary_extras

        self.token_cache: dict[str, tuple[str, float]] = (
            token_cache if token_cache is not None else {}
        )
        # token hash -> expires_at. Constant TTL keeps it ordered by expiry.
        self.negative_cache: OrderedDict[str, float] = OrderedDict()
        self.bucket = TokenBucket(unknown_per_second, unknown_burst)
        self.stats: dict[str, int] = dict.fromkeys(
            PUBLIC_COUNTERS + ("negative_cache_hits_total",), 0
        )
        self._refresh_not_before: dict[str, float] = {}  # token hash -> monotonic
        self._inflight: dict[str, asyncio.Task] = {}  # token hash -> task
        self._background: set[asyncio.Task] = set()
        self._client: Any = None

        now = clock()
        self._summary_at = now
        self._summary_snapshot = self._summary_counters()
        self._last_problem_warning_at = float("-inf")
        self._rejection_window_start = float("-inf")
        self._rejection_window_count = 0

    # --- public API ---------------------------------------------------

    def seed(self, token: str, username: str) -> None:
        """Pre-seed a never-expiring identity (``DEV_TOKEN_SEED``)."""
        self.token_cache[token] = (username, float("inf"))

    async def validate(self, token: str) -> Optional[str]:
        """Return the token's username, or None if HF rejected it.

        - Fresh positive entry: served directly.
        - Expired positive entry (within stale grace): the cached username
          is returned at once and ONE background refresh is started.
        - Negatively cached: None, without calling HF.
        - Unknown: joins the in-flight call for the same token if any,
          else draws from the unknown-token bucket and calls whoami.
          Raises ``TokenValidationUnavailable`` (503) when the bucket is
          empty or HF gives no verdict.
        """
        if not token:
            return None

        now = self.clock()
        cached = self.token_cache.get(token)
        if cached is not None:
            username, expires_at = cached
            if now < expires_at:
                return username
            if now <= expires_at + TOKEN_CACHE_STALE_GRACE_SECONDS:
                self._schedule_refresh(token, now)
                return username
            # Past the stale grace and not pruned yet: no longer trusted.
            del self.token_cache[token]

        key = token_hash(token)
        if self._negative_hit(key, now):
            self.stats["negative_cache_hits_total"] += 1
            return None

        task = self._inflight.get(key)
        if task is None:
            if not self.bucket.try_take(now):
                self.stats["unknown_token_shed_total"] += 1
                raise TokenValidationUnavailable()
            task = self._start(token, key, known=False)
        result = await asyncio.shield(task)
        if result is _INCONCLUSIVE:
            raise TokenValidationUnavailable()
        return result

    def prune(self) -> None:
        """Drop expired state (called by the server's periodic sweeper)."""
        now = self.clock()
        for tok, (_, expires_at) in list(self.token_cache.items()):
            if now > expires_at + TOKEN_CACHE_STALE_GRACE_SECONDS:
                del self.token_cache[tok]
        while self.negative_cache:
            key, expires_at = next(iter(self.negative_cache.items()))
            if expires_at > now:
                break
            del self.negative_cache[key]
        for key, not_before in list(self._refresh_not_before.items()):
            if not_before <= now:
                del self._refresh_not_before[key]

    def health(self) -> dict:
        """Aggregate state for ``/health`` (no tokens, hashes or users)."""
        return {
            "cache_size": len(self.token_cache),
            "negative_cache_size": len(self.negative_cache),
            **{key: self.stats[key] for key in PUBLIC_COUNTERS},
        }

    @property
    def inflight_count(self) -> int:
        return len(self._inflight)

    @property
    def background_count(self) -> int:
        return len(self._background)

    async def aclose(self) -> None:
        """Cancel in-flight whoami calls and close the client."""
        tasks = list(self._inflight.values())
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass  # cancelled (or failed and already logged): done
        self._inflight.clear()
        self._background.clear()
        client, self._client = self._client, None
        if client is not None:
            await client.aclose()

    def maybe_log_summary(self, now: Optional[float] = None) -> bool:
        """Once per ``AUTH_SUMMARY_LOG_INTERVAL_SECONDS``, one INFO line of deltas.

        Stands in for per-request lines in the repetitive cases (rejections
        answered from the negative cache, shed unknown tokens, whoami
        errors) plus any ``summary_extras``. Silent when none of those
        moved. Returns whether a line was logged.
        """
        if now is None:
            now = self.clock()
        if now - self._summary_at < AUTH_SUMMARY_LOG_INTERVAL_SECONDS:
            return False
        current = self._summary_counters()
        delta = {k: v - self._summary_snapshot.get(k, 0) for k, v in current.items()}
        window = now - self._summary_at
        self._summary_at, self._summary_snapshot = now, current
        extra_keys = [k for k in current if k not in self.stats]
        if not (
            delta["negative_cache_hits_total"]
            or delta["unknown_token_shed_total"]
            or delta["whoami_rejected_total"]
            or delta["whoami_errors_total"]
            or any(delta[k] for k in extra_keys)
        ):
            return False
        logger.info(
            "Auth summary (last %.0fs): whoami_calls=%d rejected=%d errors=%d "
            "negative_cache_hits=%d unknown_token_shed=%d cache_size=%d "
            "negative_cache_size=%d%s",
            window,
            delta["whoami_calls_total"],
            delta["whoami_rejected_total"],
            delta["whoami_errors_total"],
            delta["negative_cache_hits_total"],
            delta["unknown_token_shed_total"],
            len(self.token_cache),
            len(self.negative_cache),
            "".join(f" {k}={delta[k]}" for k in extra_keys),
        )
        return True

    # --- internals ----------------------------------------------------

    def _summary_counters(self) -> dict[str, int]:
        counters = dict(self.stats)
        if self.summary_extras is not None:
            for key, value in self.summary_extras().items():
                counters.setdefault(key, value)
        return counters

    def _negative_hit(self, key: str, now: float) -> bool:
        expires_at = self.negative_cache.get(key)
        if expires_at is None:
            return False
        if now < expires_at:
            return True
        del self.negative_cache[key]
        return False

    def _negative_add(self, key: str, now: float) -> None:
        self.negative_cache.pop(key, None)
        self.negative_cache[key] = now + TOKEN_NEGATIVE_CACHE_SECONDS
        while len(self.negative_cache) > self.negative_cache_max_entries:
            self.negative_cache.popitem(last=False)

    def _get_client(self) -> Any:
        if self._client is None:
            self._client = self.client_factory(
                timeout=httpx.Timeout(
                    self.total_timeout, connect=WHOAMI_CONNECT_TIMEOUT_SECONDS
                ),
                limits=httpx.Limits(
                    max_connections=self.max_connections,
                    max_keepalive_connections=WHOAMI_MAX_KEEPALIVE_CONNECTIONS,
                    keepalive_expiry=WHOAMI_KEEPALIVE_EXPIRY_SECONDS,
                ),
            )
        return self._client

    def _start(self, token: str, key: str, *, known: bool) -> asyncio.Task:
        task = asyncio.get_running_loop().create_task(
            self._whoami_and_apply(token, key, known=known)
        )
        self._inflight[key] = task
        task.add_done_callback(lambda t: self._task_done(key, t))
        return task

    def _task_done(self, key: str, task: asyncio.Task) -> None:
        if self._inflight.get(key) is task:
            del self._inflight[key]
        self._background.discard(task)
        if task.cancelled():
            return
        exc = task.exception()  # retrieving it also silences "never retrieved"
        if exc is not None:
            logger.error("whoami task failed unexpectedly", exc_info=exc)

    def _schedule_refresh(self, token: str, now: float) -> None:
        key = token_hash(token)
        if key in self._inflight:
            return
        not_before = self._refresh_not_before.get(key)
        if not_before is not None and now < not_before:
            return
        if len(self._background) >= self.max_background_refreshes:
            return
        self._background.add(self._start(token, key, known=True))

    def _inconclusive(self, key: str, known: bool, reason: str) -> Any:
        """HF gave no verdict (network, timeout, 429, 5xx, bad body): change nothing.

        A known token keeps its (stale) identity and is retried no sooner
        than ``TOKEN_REFRESH_RETRY_SECONDS``; an unknown one gets a 503.
        """
        self.stats["whoami_errors_total"] += 1
        now = self.clock()
        if known:
            self._refresh_not_before[key] = now + TOKEN_REFRESH_RETRY_SECONDS
        if now - self._last_problem_warning_at >= AUTH_SUMMARY_LOG_INTERVAL_SECONDS:
            self._last_problem_warning_at = now
            logger.warning(
                "Token validation inconclusive (%s); known tokens keep their "
                "cached identity, unknown ones get 503, further failures are "
                "counted in the auth summary",
                reason,
            )
        else:
            logger.debug("Token validation inconclusive (%s)", reason)
        return _INCONCLUSIVE

    def _rejection_warning_allowed(self, now: float) -> bool:
        if now - self._rejection_window_start >= AUTH_SUMMARY_LOG_INTERVAL_SECONDS:
            self._rejection_window_start, self._rejection_window_count = now, 0
        self._rejection_window_count += 1
        return self._rejection_window_count <= AUTH_REJECTION_WARNINGS_PER_INTERVAL

    async def _whoami_and_apply(self, token: str, key: str, *, known: bool) -> Any:
        """One whoami call; applies its verdict to the caches.

        Returns the username, None (explicit rejection) or ``_INCONCLUSIVE``.
        Runs as the single in-flight task for ``key``; never raises except
        ``CancelledError``.
        """
        self.stats["whoami_calls_total"] += 1
        try:
            response = await asyncio.wait_for(
                self._get_client().get(
                    self.whoami_url, headers={"Authorization": f"Bearer {token}"}
                ),
                timeout=self.total_timeout,
            )
            status_code = response.status_code
            username: Optional[str] = None
            if status_code == 200:
                data = response.json()
                username = data.get("name") if isinstance(data, dict) else None
                if not isinstance(username, str) or not username:
                    # Never cache a non-identity, never overwrite a good one.
                    return self._inconclusive(key, known, "HF 200 without a username")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._inconclusive(key, known, f"{type(e).__name__}: {e}")

        now = self.clock()
        if status_code == 200:
            self.token_cache[token] = (
                username,
                now
                + TOKEN_CACHE_TTL_SECONDS
                + random.uniform(0.0, TOKEN_CACHE_TTL_JITTER_SECONDS),
            )
            self.negative_cache.pop(key, None)
            self._refresh_not_before.pop(key, None)
            if known:
                logger.debug("Token re-validated for user: %s", username)
            else:
                logger.info("Token validated for user: %s", username)
            return username

        if status_code in (401, 403):
            # Explicit rejection: the token is revoked or invalid. The ONLY
            # outcome that may drop a cached identity.
            self.stats["whoami_rejected_total"] += 1
            self.token_cache.pop(token, None)
            self._refresh_not_before.pop(key, None)
            already_rejected = self._negative_hit(key, now)
            self._negative_add(key, now)
            loud = not already_rejected and self._rejection_warning_allowed(now)
            (logger.warning if loud else logger.debug)(
                "Token rejected by HF: %d (token_hash=%s%s)",
                status_code,
                key[:12],
                ", cached identity dropped" if known else "",
            )
            return None

        return self._inconclusive(key, known, f"HF {status_code}")
