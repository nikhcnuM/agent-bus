# AGENTS.md — agent-bus

Local durable broker for the Agent Assistant system.

This repo is one child repo inside the multi-repo workspace at
`/Users/carlosledesma/projects/agent-assistant`. Read the workspace-level
`../AGENTS.md` first when working across repos.

## Current Responsibility

`agent-bus` owns:

- Event/command envelope validation.
- Redis Streams based persistence and fan-out.
- HTTP publication and consumption APIs.
- WebSocket fan-out for live clients.
- Derived snapshots for companion/widget startup.
- Dead-letter stream for invalid Redis payloads.

It does not own business behavior such as recording audio, calling Hermes,
rendering Launchpad LEDs, or playing TTS.

## Source Map

All source lives under `src/agent_bus/`.

- `models.py`
  - `BusEnvelope`, `StoredEnvelope`, consume/ack/health/snapshot DTOs.
- `config.py`
  - `BusConfig`, stream names, config loading.
- `backend.py`
  - `RedisStreamBackend` for production.
  - `InMemoryBackend` for tests and contract development only.
  - Snapshot derivation from important event types.
- `app.py`
  - FastAPI routes and WebSocket hub.
- `cli.py`
  - `agent-bus --config config.example.yaml`.

## API

- `GET /health`
- `GET /snapshot`
- `POST /events`
- `POST /commands`
- `GET /consume/{events|commands}?group=...&consumer=...`
- `POST /consume/{events|commands}/ack?group=...`
- `WS /ws`

WebSocket clients receive an initial snapshot frame:

```json
{ "snapshot": {} }
```

Published frames use:

```json
{
  "stream_id": "1-0",
  "envelope": { "type": "...", "source": "...", "payload": {} }
}
```

## Streams

Default stream prefix: `agentbus`.

- `agentbus.commands`
- `agentbus.events`
- `agentbus.deadletter`
- `agentbus.snapshots`

Production uses Redis Streams. Use Redis AOF locally for durability:

```bash
redis-server --appendonly yes
```

`memory://` is test-only. Do not document or use it as a deployment mode.

## Envelope

Required logical fields:

```json
{
  "type": "voice.ptt.start",
  "source": "launchpad-system-actions",
  "payload": {}
}
```

Common optional fields:

- `target` for commands addressed to one service.
- `correlation_id` to connect a PTT chain.
- `session_id` for voice/Hermes/agent session state.

Keep event names stable. Add contract tests before renaming or changing payload
shape.

## Run And Test

```bash
../agent-voice-gateway/.venv/bin/python -m pip install -e .
../agent-voice-gateway/.venv/bin/agent-bus --config config.example.yaml
../agent-voice-gateway/.venv/bin/python -m pytest
```

Full suite status after migration: `4 passed`.

The repo currently reuses the Python environment from `../agent-voice-gateway`
in local verification because it already has FastAPI/httpx/pytest installed.
A dedicated `.venv` is also fine.

## Pitfalls

- Redis integration tests against a real Redis instance are still needed.
- Pending-message recovery and retry counters are first-pass; harden before
  treating this as production.
- `InMemoryBackend` is intentionally simple and does not model every Redis
  consumer-group edge case.
- WebSocket broadcast is best-effort.
- Do not put service-specific state machines here; keep the bus generic.

## Sibling Repos

- `../launchpad-system-actions`: publishes PTT commands and consumes agent sessions.
- `../agent-voice-gateway`: consumes voice commands and publishes STT/TTS events.
- `../mac-widget-hermes`: consumes STT, calls Hermes, publishes agent sessions.
