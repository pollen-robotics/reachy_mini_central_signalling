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
label in `ROBOT_KIND_LABELS`) in `app.py`; the tests in
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
  "uptime_seconds": 12345
}
```

`peers`, `producers` and `producers_by_kind` count **connected** peers
only. `started_at` / `uptime_seconds` are wall-clock and exist so an
operator can tell a fresh redeploy (all counters reset) from a quiet
fleet. Both `/` and `/health` are served with `Cache-Control: no-store`.
