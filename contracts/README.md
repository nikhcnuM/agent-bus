# Agent Bus Contracts

Canonical source of truth for the event/command shapes shared by `agent-bus`,
`agent-voice-gateway`, `launchpad-system-actions` and `mac-widget-hermes`.

## Layout

- `event-types.json` - registry of every supported `type`. For each type it
  lists the kind (`command` or `event`), default stream, whether `target` is
  required, and the path of its payload schema and canonical fixture.
- `schemas/<type>.json` - JSON-Schema-lite description of the `payload` for one
  type. Supported keywords: `type`, `required`, `properties`,
  `additionalProperties`, `items`, `enum`, `minLength`. `type` may be a string
  or a list (e.g. `["string", "null"]`) to allow nullables.
- `fixtures/<type>.json` - one full envelope (envelope + payload) per type.
  Fixtures are exercised by the agent-bus contract tests and by the producer
  and consumer tests in the satellite repos.

## Envelope

Every message published to the bus carries the following envelope fields:

| Field            | Required | Notes                                                  |
| ---------------- | -------- | ------------------------------------------------------ |
| `id`             | yes      | Producer-assigned unique id, e.g. `evt_<uuid>`.        |
| `type`           | yes      | One of the registered keys in `event-types.json`.      |
| `source`         | yes      | Logical producer name.                                 |
| `timestamp`      | yes      | ISO-8601 UTC, with `Z` suffix.                         |
| `correlation_id` | no       | Carries the parent intent (typically the PTT id).      |
| `session_id`     | no       | Carries the agent session id when applicable.          |
| `target`         | depends  | Required for `command` kinds, omitted for events.      |
| `payload`        | yes      | Object validated against the per-type schema.          |

## Validation policy

- `POST /events` and `POST /commands` reject invalid envelopes or invalid
  payloads with HTTP `422` and a body of the form
  `{ "type": "...", "reason": "...", "field": "..." }`.
- `WS /ws` closes with code `1003` (unsupported data) when the incoming
  message is not a valid envelope or its payload does not match the registered
  schema. Code `1011` is reserved for genuine internal errors.
- Messages read from Redis that fail validation are written to the
  `agentbus.deadletter` stream with a human-readable `reason` and the original
  message is `XACK`-ed so it does not get redelivered forever.
- Unknown `type` values are rejected at HTTP/WS boundaries and dead-lettered
  on Redis.

## Versioning and compatibility

This is `v1.0.0`. Until the registry declares a new major version, treat the
following as breaking changes:

- Renaming or removing any required field of the envelope.
- Renaming or removing any required field listed in a payload schema.
- Tightening an existing field (e.g. flipping `additionalProperties: true` to
  `false`, narrowing an `enum`, widening a `required` list).
- Renaming a registered `type`.

The following are non-breaking:

- Adding a new `type` to the registry.
- Adding optional fields to a payload (with `additionalProperties: true`).
- Relaxing an `enum` to accept new values.

When a breaking change is unavoidable, bump the registry `version` and update
the consumers in lockstep. Read-side compatibility shims are only acceptable
while a documented migration is open. Otherwise, prefer to fail loudly over
accepting ambiguous payloads.

## Updating contracts

1. Edit the schema under `schemas/`.
2. Edit (or add) the fixture under `fixtures/`.
3. Run `python -m pytest` in `agent-bus`, `agent-voice-gateway` and
   `launchpad-system-actions`. The contract tests in each repo replay every
   fixture, so changes that break a downstream consumer surface immediately.
4. If a new `type` is added, register it in `event-types.json` and add at
   least one producer- or consumer-side test in the relevant satellite repo.
