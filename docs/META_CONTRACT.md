# Producer metadata contract

Every peer registers with `POST /send` `{"type": "setPeerStatus", "roles": [...], "meta": {...}}`.
Central stores `meta` **verbatim** and forwards it unchanged to every
owner-scoped surface: the SSE `list` / `peerStatusChanged` /
`sessionStateChanged` frames, `GET /api/robot-status` and
`GET /api/debug/peers`. Daemons may add fields at any time without a
central change; clients must tolerate unknown keys.

This page lists the few keys central *reads*. Anything not listed is
opaque to the server.

## Keys interpreted by central

| Key | Sent by | Server-side use |
| --- | --- | --- |
| `name` | producer, consumer | Producer: `robotName` in `/api/robot-status`. Consumer: reported as `activeApp` while it holds a session. Display only. |
| `hardware_id` | producer (daemons >= v1.7.2) | Stable per-robot identity: a new registration with the same `hardware_id` under the same HF user evicts the older producer (last-writer-wins). Its presence also marks the daemon as heartbeat-capable, which opts it into the stale-producer sweep. |
| `install_id` | producer (reserved) | Same dedup semantics as `hardware_id`, checked independently. Not emitted by any shipped daemon yet. |
| `kind` | producer | Robot family, used **only** for the public per-kind counters (`/health` `producers_by_kind`, status page). See below. |
| `robot_type` | producer | Alias for `kind`, consulted only when `kind` is absent or empty. |

Keys such as `transport`, `release` or `api_version` are passed through
untouched and never interpreted.

## `kind`

- **Missing means Reachy Mini.** Reachy Mini daemons send no `kind` at
  all (their meta is `{name, transport, hardware_id}`), so when neither
  `kind` nor `robot_type` holds a non-blank string the producer
  classifies as `reachy_mini`. A `kind` that is absent, `null`, blank or
  not a string falls through to `robot_type`.
- **Known values:** `reachy_mini`, `microduck`. Matching is
  case-insensitive and ignores every character outside `a-z0-9`, so
  `Micro-Duck`, `micro duck`, `microduck`, `Reachy Mini`, `reachy-mini`
  and `reachymini` all resolve to their canonical kind. A raw value
  longer than 64 characters is classified as `other` without any
  normalisation.
- **`meta` must be a JSON object.** A `setPeerStatus` whose `meta` is
  not an object is rejected with HTTP 400 and leaves the previously
  registered `meta` untouched.
- **Anything else counts as `other`** on the public counters. `kind` is
  untrusted input (any authenticated HF user can send any meta), so the
  public surfaces only ever expose the fixed key set
  `{reachy_mini, microduck, other}` - an unknown value can bump the
  `other` counter and nothing more. The status page labels for those
  keys are server-side constants; no meta string is ever rendered there.
- **The raw value is still forwarded verbatim** to the owner's own
  listeners and `/api/*` endpoints. Classification is a read-only view
  for the public counters; it never rewrites `meta`.

Adding a robot family is a one-line change to `KNOWN_ROBOT_KINDS` (and a
label in `ROBOT_KIND_LABELS`) in `robot_kinds.py`; the tests in
`test_robot_kind.py` and `test_routes.py` pin the behaviour above.

## Public counters (`GET /health`)

```json
{
  "status": "healthy",
  "peers": 3,
  "producers": 2,
  "sessions": 1,
  "producers_by_kind": {"reachy_mini": 1, "microduck": 1, "other": 0},
  "started_at": "2026-09-16T08:00:00Z",
  "uptime_seconds": 12345,
  "usage_publisher": {"enabled": true, "last_published_at": "2026-09-16T08:30:00Z", "pending_rows": 0, "dropped_rows": 0},
  "auth": {
    "cache_size": 280,
    "negative_cache_size": 3,
    "whoami_calls_total": 1200,
    "whoami_rejected_total": 4,
    "whoami_errors_total": 0,
    "unknown_token_shed_total": 0
  },
  "sse": {
    "grace_seconds": 15.0,
    "detached_now": 0,
    "detach_total": 120,
    "reattach_total": 118,
    "grace_expired_total": 2,
    "reattach_latency_s_max": 8.8,
    "sessions_ended_at_detach_total": 7,
    "consumer_session_replaced_total": 1
  }
}
```

`peers`, `producers` and `producers_by_kind` count **connected** peers
only. A peer whose SSE stream closed less than the reconnect grace ago
(`sse.grace_seconds`, see the README's "Liveness and SSE reconnect
grace") is still connected for these counters: it is logically online
and keeps its peerId if it comes back. `started_at` / `uptime_seconds` are wall-clock and exist so an
operator can tell a fresh redeploy (all counters reset) from a quiet
fleet. Both `/` and `/health` are served with `Cache-Control: no-store`.
`usage_publisher` is the fleet usage publisher's aggregate state (see
[`FLEET_USAGE.md`](FLEET_USAGE.md)).

`auth` is the token-validation layer's aggregate state (no tokens,
hashes or usernames): `cache_size` / `negative_cache_size` are the
current number of validated tokens (fresh or stale) and of tokens HF
recently rejected; the `_total` fields count since boot the whoami calls
made, the explicit HF 401/403 among them, the inconclusive ones
(network error, timeout, HF 429/5xx), and the requests carrying a
never-seen token that were answered `503` because the whoami budget for
unknown tokens was exhausted (a never-seen token HF gave no verdict for
is also answered `503`, and counted in `whoami_errors_total`). See the
README's "Token validation and caching" section.

`sse` is the SSE reconnect grace's aggregate state (no peer ids, tokens
or usernames): the configured `grace_seconds` (`0` = disabled), the
number of peers currently detached (SSE closed, grace running, still
counted above), and since-boot counts of detaches, reattaches within the
grace and grace expiries (evictions, including peers whose newest SSE
connection never started streaming), plus the longest reattach latency
seen in seconds. `sessions_ended_at_detach_total` counts producer
sessions ended because the producer's SSE stream closed or was
superseded; `consumer_session_replaced_total` counts sessions a consumer
replaced by starting a new one on the robot it already held.
