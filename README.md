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
  while its tab is visible)

Authentication: `Authorization: Bearer <HF token>` on all authenticated
endpoints (`?token=` query form is deprecated).

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
