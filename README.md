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
