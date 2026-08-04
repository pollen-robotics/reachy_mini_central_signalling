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
- `GET /health` - public counters

Authentication: `Authorization: Bearer <HF token>` on all authenticated
endpoints (`?token=` query form is deprecated).

## Protocol

Implements the GStreamer webrtcsink/webrtcsrc signaling protocol
semantics over SSE + HTTP POST (works through HTTP/2 proxies like
HuggingFace Spaces). See `reachy_mini/docs/SIGNALING.md` for the
canonical lifecycle contract.
