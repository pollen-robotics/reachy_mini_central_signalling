---
title: Reachy Mini Central
emoji: 🤖
colorFrom: blue
colorTo: purple
sdk: docker
pinned: false
app_port: 7860
---

# Reachy Mini Central

WebRTC signaling server for Reachy Mini robot.

## Features

- GStreamer-compatible WebRTC signaling protocol over HTTP (SSE + POST)
- Producer/Consumer session management with per-user isolation
- Stale-producer sweep (half-open socket eviction, heartbeat-driven)
- SSE reconnect grace: a peer whose SSE stream is cut keeps its peerId
  and listing for 15 s while it reconnects
- Real-time status monitoring

## Endpoints

- `GET /events` - SSE stream (server to client messages)
- `POST /send` - client to server messages
- `GET /api/robot-status` - busy/free status of the caller's robots
- `GET /api/debug/peers` - owner-filtered peer dump for debugging
- `GET /health` - public counters, incl. a per-robot-kind breakdown
  (`producers_by_kind`: `reachy_mini` / `microduck` / `other`) and uptime
  (computed at most once per 2 s and served from a server-side cache, so
  values may lag by up to 2 s; the status page polls it every 15 s, only
  while its tab is visible), plus aggregate token-validation counters
  under `auth` and SSE reconnect-grace counters under `sse`

Authentication: `Authorization: Bearer <HF token>` on all authenticated
endpoints (`?token=` query form is deprecated).

### Token validation and caching

Tokens are checked against HF `whoami-v2` and cached so the fleet's
heartbeats and polls almost never reach HF (code: `hf_auth.py`):

- A validated token is trusted for 1 h plus a random 0-10 min jitter
  (so a fleet that reconnected together after a deploy does not
  re-validate in lockstep). After that the cached identity keeps being
  served while **one** background whoami call refreshes it; no request
  waits on HF for a token central already knows.
- Only an explicit HF 401/403 revokes a cached identity. Network errors,
  timeouts, 429, 5xx and malformed answers keep serving the stale
  identity (retried at most once a minute per token) for up to 24 h.
- A token HF rejected is remembered (as a SHA-256 hash, never raw) for
  5 min: retries get `401` locally, without a whoami call.
- Concurrent validations of the same token share a single whoami call.
- Tokens central has never seen draw from a global budget of 20 whoami
  calls/s with a burst of 300, sized so a whole fleet reconnecting after
  a deploy is not throttled. Past the budget - or when HF gives no
  verdict (timeout, 429, 5xx) for a never-seen token - the request gets
  `503` with `Retry-After`, never `401`: clients treat it like any
  transient error and retry with their normal backoff, and nothing
  suggests the token is bad.
- **Trade-off:** the budget is global, so a sustained flood of
  *distinct* invalid tokens at R req/s (R > 20) competes with
  never-seen robots for it: each attempt of such a robot succeeds with
  probability ~20/R, so with retries every ~5 s it is delayed by roughly
  R/4 s. Robots with a cached token are unaffected (a refresh never draws
  from the budget), and a repeated invalid token costs nothing after its
  first rejection.
- Tunables: `REACHY_CENTRAL_WHOAMI_UNKNOWN_PER_SECOND` (default 20) and
  `REACHY_CENTRAL_WHOAMI_UNKNOWN_BURST` (default 300; the whoami
  connection pool is sized to the burst).
- `/health` exposes aggregate counters under `auth` (see
  [`docs/META_CONTRACT.md`](docs/META_CONTRACT.md)).

### Logs

Lines carry ISO-8601 UTC timestamps (`2026-10-01T08:21:20Z INFO app: ...`),
uvicorn's access log included, and are kept quiet so HF's short log
window covers more time:

- Repetitive auth outcomes (cached rejections, shed requests, whoami
  errors) are summarised in one `Auth summary` line per minute.
- Successful (2xx) `POST /send` and `GET /api/robot-status` access lines
  - the fleet's heartbeats and status polls - are dropped and counted
  (`access_log_filtered=` on the summary line); every non-2xx line and
  every other route is kept.
- `Producer registered` is INFO only for a new registration or a meta
  change (heartbeats re-send it every 10 s), with meta capped at 300
  characters.
- A legacy `?token=` query value is printed as `token=***` in access
  lines.
- SSE detaches, reattaches and grace expiries (see below) are summarised
  in one `SSE summary` line per minute (only when something happened);
  per-peer lines for them are DEBUG only.

### Liveness and SSE reconnect grace

- **Producer lease.** A heartbeat-capable producer (daemon >= v1.7.2,
  `meta.hardware_id` present) silent on `POST /send` for 30 s is swept
  (`REACHY_CENTRAL_PRODUCER_LEASE_SECONDS`); the SSE `welcome` advertises
  a 10 s heartbeat.
- **SSE reconnect grace.** Hugging Face's ingress cuts SSE connections
  from the outside (whole ingress pools at once, roughly every 2 h);
  robots reconnect on the same token after their 5 s relay backoff,
  5-9 s later. When a peer's current SSE stream closes, central no
  longer evicts it at once: the peer is **detached** for
  `REACHY_CENTRAL_SSE_GRACE_SECONDS` (default `15`; `0` disables the
  grace, see "Grace 0" below).
  - While detached, the peer stays registered: it is still in
    `list` / `/api/robot-status` / `/api/debug/peers` (with
    `detached: true` and `detached_age_seconds`) and counts in `/health`
    `peers` / `producers` / `producers_by_kind`. No `peerStatusChanged`
    removal is broadcast and no fleet usage event is recorded. Messages
    for it are queued, and its `POST /send` calls keep working.
  - A reconnect on the same token within the grace resumes the **same
    peer and peerId**: the usual `welcome` + `list`, then everything
    queued while it was detached, in order - except `endSession` frames
    for sessions the peer ended itself (its own `endSession` POST, or a
    `startSession` that replaced its session), which are never replayed.
    The relay's re-sent `setPeerStatus` is the same idempotent
    `peerStatusChanged(producer)` broadcast that every 10 s heartbeat
    already produces.
  - The grace deadline is re-armed (now + grace) whenever a new SSE
    connection is bound and cleared once its stream starts, so a
    reconnect in progress at the end of the grace is not expired under
    it, and a peer whose newest connection never starts streaming
    (including one that superseded a live connection) is evicted after
    the grace instead of lingering forever.
  - When the grace runs out, the 5 s sweeper evicts the peer exactly
    like an SSE close used to (removal broadcast, session end cause
    `peer_disconnected`, fleet usage `producer_gone`), so eviction lands
    15-20 s after the cut. The stale-producer sweep skips detached
    peers (the grace expiry owns them); a stable-id collision (e.g. the
    robot came back with a rotated token and the same `hardware_id`)
    still evicts the detached peer immediately. `setPeerStatus(roles=[])`
    behaves exactly as before. Each sweeper step (and each expiring
    peer) runs in its own try/except, so one failure never skips the
    rest.
  - **Trade-offs:** a client that closes its SSE without withdrawing
    (graceful shutdown without `setPeerStatus(roles=[])`, crash, power
    cut with a clean FIN) now stays listed up to grace + one sweep
    (~20 s) instead of disappearing at once; and a consumer that closes
    its SSE without sending `endSession` now holds its robot busy for
    up to ~20 s (its session survives the grace, see below).
- **Sessions of a detaching peer** (fixed rule, chosen from the clients'
  reconnect behaviour):
  - A detaching **producer**'s session ends at detach, exactly as
    before (`endSession` to the consumer, `sessionStateChanged`
    busy=false, cause `peer_disconnected`), so the robot is listed and
    free during the grace. The daemon relay tears down its local WebRTC
    sessions and forgets their ids whenever its SSE drops and never
    tells central, so keeping its session would only leave a phantom
    busy lock. Nothing about that session stays queued for the robot.
    The same applies when a new SSE stream of a producer supersedes its
    still-attached old one (half-open socket): the relay reconnecting
    has dropped its sessions too.
  - A detaching **consumer**'s session survives the grace: its media is
    peer-to-peer and outlives the SSE channel. Clients that do restart
    (the Python consumer, the JS SDK's re-dial) send `endSession` for
    their old session, which now lands since the peer is still
    registered.
  - If a consumer sends `startSession` to the robot whose session it
    still holds, its old session is replaced (the robot gets
    `endSession` for it, the consumer does not; fleet usage end cause
    `consumer_replaced`) rather than rejected as `robot_busy`. Any
    other consumer, attached or not, still gets `robot_busy`.
- **Grace 0** (`REACHY_CENTRAL_SSE_GRACE_SECONDS=0`) restores
  evict-on-close: no detach, no queue carry-over, no deadlines, and a
  producer's session survives a superseding reconnect as it used to.
  These fixes stay active at 0 because they are not grace-specific:
  - consumer self-replacement on `startSession` (above) instead of
    `robot_busy` from its own session;
  - a stream that dies during its `welcome` / `list` handshake is
    cleaned up (previously the peer leaked, registered forever);
  - a peer's `endSession` POST drops `endSession` frames for that
    session still queued for itself;
  - a `POST /send` whose peer was evicted while its body was being read
    is answered `400 Peer not found` instead of re-registering a ghost
    producer.
- `/health` exposes the grace's aggregate state under `sse` (see
  [`docs/META_CONTRACT.md`](docs/META_CONTRACT.md)).

## Protocol

Implements the GStreamer webrtcsink/webrtcsrc signaling protocol
semantics over SSE + HTTP POST (works through HTTP/2 proxies like
HuggingFace Spaces). The lifecycle contract (verbatim `meta`
forwarding, `setPeerStatus(roles=[])` withdraw, stable-id eviction,
`endSession`) is documented in the module docstring of `app.py`.

## Producer metadata contract

Central forwards producer `meta` verbatim and interprets only a handful
of keys: `name`, `hardware_id`, `install_id` and `kind` (alias
`robot_type`; missing = `reachy_mini`; known values `reachy_mini`,
`microduck`; anything else is counted as `other` on the public
counters while the raw value still reaches the owner's listeners).
See [`docs/META_CONTRACT.md`](docs/META_CONTRACT.md).

## Fleet usage statistics

Central aggregates robot and session activity into UTC 10-minute windows
plus a daily rollup (per robot kind, no identifiers) and, when configured,
publishes them to a public HF dataset
(`pollen-robotics/pollen_robotic_fleet_usage` in production). The status
page then charts them by fetching `summary.json` from huggingface.co
directly. Disabled unless `FLEET_USAGE_DATASET` / `FLEET_USAGE_HF_TOKEN`
(or the dev-only `FLEET_USAGE_LOCAL_DIR`) are set; `/health` reports the
publisher's state under `usage_publisher`. Code in `fleet_usage.py`.
Metric definitions, dataset schema and operator setup:
[`docs/FLEET_USAGE.md`](docs/FLEET_USAGE.md).
