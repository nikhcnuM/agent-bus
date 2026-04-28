from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Protocol

from agent_bus.config import BusConfig
from agent_bus.contracts import ContractError, validate_envelope
from agent_bus.models import BusEnvelope, PendingKind, PendingMessage, StoredEnvelope, StreamKind


class BusBackend(Protocol):
    name: str

    async def publish(self, kind: StreamKind, envelope: BusEnvelope) -> StoredEnvelope: ...

    async def consume(
        self,
        kind: StreamKind,
        *,
        group: str,
        consumer: str,
        count: int = 10,
        block_ms: int = 1,
    ) -> list[StoredEnvelope]: ...

    async def ack(self, kind: StreamKind, *, group: str, stream_ids: list[str]) -> None: ...

    async def snapshot(self) -> dict[str, Any]: ...

    async def deadletter_recent(self, count: int = 20) -> list[dict[str, Any]]: ...

    async def recover_pending(self, kind: StreamKind, *, group: str, consumer: str) -> int: ...

    async def pending_messages(self, kind: PendingKind, *, group: str) -> list[PendingMessage]: ...

    async def close(self) -> None: ...


def stream_name(config: BusConfig, kind: StreamKind) -> str:
    return config.commands_stream if kind == "commands" else config.events_stream


def _kind_to_stream(config: BusConfig, kind: PendingKind) -> str:
    if kind == "commands":
        return config.commands_stream
    if kind == "events":
        return config.events_stream
    if kind == "snapshots":
        return config.snapshots_stream
    return config.deadletter_stream


def build_backend(config: BusConfig) -> BusBackend:
    if config.redis_url == "memory://":
        return InMemoryBackend(config)
    return RedisStreamBackend(config)


@dataclass
class InMemoryBackend:
    config: BusConfig
    name: str = "memory"
    _streams: dict[str, list[StoredEnvelope]] = field(default_factory=lambda: defaultdict(list))
    _deadletter: list[dict[str, Any]] = field(default_factory=list)
    _offsets: dict[tuple[str, str], int] = field(default_factory=dict)
    _snapshot: dict[str, Any] = field(default_factory=dict)
    _next_id: int = 0

    async def publish(self, kind: StreamKind, envelope: BusEnvelope) -> StoredEnvelope:
        self._next_id += 1
        stored = StoredEnvelope(stream_id=f"{self._next_id}-0", envelope=envelope)
        self._streams[kind].append(stored)
        self._apply_snapshot(envelope)
        return stored

    async def consume(
        self,
        kind: StreamKind,
        *,
        group: str,
        consumer: str,
        count: int = 10,
        block_ms: int = 1,
    ) -> list[StoredEnvelope]:
        key = (kind, group)
        start = self._offsets.get(key, 0)
        messages = self._streams[kind][start : start + count]
        self._offsets[key] = start + len(messages)
        return list(messages)

    async def ack(self, kind: StreamKind, *, group: str, stream_ids: list[str]) -> None:
        return None

    async def snapshot(self) -> dict[str, Any]:
        return dict(self._snapshot)

    async def deadletter_recent(self, count: int = 20) -> list[dict[str, Any]]:
        return list(reversed(self._deadletter[-count:]))

    async def recover_pending(self, kind: StreamKind, *, group: str, consumer: str) -> int:
        # In-memory backend has no real pending; nothing to recover.
        return 0

    async def pending_messages(self, kind: PendingKind, *, group: str) -> list[PendingMessage]:
        return []

    async def close(self) -> None:
        return None

    def _apply_snapshot(self, envelope: BusEnvelope) -> None:
        apply_snapshot(self._snapshot, envelope)


class RedisStreamBackend:
    name = "redis"

    def __init__(self, config: BusConfig):
        self.config = config
        from redis.asyncio import Redis

        self._redis = Redis.from_url(config.redis_url, decode_responses=True)
        self._snapshot: dict[str, Any] = {}

    async def publish(self, kind: StreamKind, envelope: BusEnvelope) -> StoredEnvelope:
        stream = stream_name(self.config, kind)
        payload = {"envelope": envelope.model_dump_json()}
        stream_id = await self._redis.xadd(
            stream,
            payload,
            maxlen=self.config.retention_maxlen,
            approximate=True,
        )
        self._apply_snapshot(envelope)
        await self._redis.xadd(
            self.config.snapshots_stream,
            {"snapshot": json.dumps(self._snapshot, sort_keys=True)},
            maxlen=100,
            approximate=True,
        )
        return StoredEnvelope(stream_id=str(stream_id), envelope=envelope)

    async def consume(
        self,
        kind: StreamKind,
        *,
        group: str,
        consumer: str,
        count: int = 10,
        block_ms: int = 1,
    ) -> list[StoredEnvelope]:
        stream = stream_name(self.config, kind)
        await self._ensure_group(stream, group)
        response = await self._redis.xreadgroup(
            group,
            consumer,
            {stream: ">"},
            count=count,
            block=block_ms,
        )
        messages: list[StoredEnvelope] = []
        for _, entries in response:
            for stream_id, fields in entries:
                stored = await self._parse_or_deadletter(stream, stream_id, fields, group, consumer)
                if stored is not None:
                    messages.append(stored)
        return messages

    async def recover_pending(self, kind: StreamKind, *, group: str, consumer: str) -> int:
        """Claim idle pending messages and re-deliver or dead-letter them.

        Returns the number of messages processed (claimed and either
        re-delivered or sent to dead-letter).
        """
        stream = stream_name(self.config, kind)
        await self._ensure_group(stream, group)

        # XPENDING summary: returns list of (msg_id, consumer, idle_ms, delivery_count)
        pending_entries = await self._redis.xpending_range(
            stream,
            group,
            min="-",
            max="+",
            count=self.config.pending_claim_count,
        )

        processed = 0
        for entry in pending_entries:
            msg_id = entry["message_id"]
            idle_ms = entry["time_since_delivered"]
            delivery_count = entry["times_delivered"]

            if idle_ms < self.config.pending_idle_ms:
                continue

            if delivery_count >= self.config.max_delivery_attempts:
                # Move to dead-letter without re-claiming content
                claimed = await self._redis.xclaim(
                    stream, group, consumer, min_idle_time=0, message_ids=[msg_id]
                )
                for c_id, c_fields in claimed:
                    raw = c_fields.get("envelope") if c_fields else None
                    envelope_data: dict[str, Any] | None = None
                    if raw:
                        try:
                            envelope_data = json.loads(raw)
                        except Exception:  # noqa: BLE001
                            pass
                    dl_entry: dict[str, str] = {
                        "reason": "max_delivery_attempts_exceeded",
                        "original_stream": stream,
                        "original_message_id": str(c_id),
                        "group": group,
                        "consumer": consumer,
                        "delivery_count": str(delivery_count),
                    }
                    if envelope_data is not None:
                        dl_entry["envelope"] = json.dumps(envelope_data)
                    elif raw is not None:
                        dl_entry["raw_message"] = raw
                    await self._redis.xadd(self.config.deadletter_stream, dl_entry)
                    await self._redis.xack(stream, group, str(c_id))
                    processed += 1
                continue

            # Still within retry budget — re-claim so this consumer picks it up
            claimed = await self._redis.xclaim(
                stream, group, consumer, min_idle_time=self.config.pending_idle_ms, message_ids=[msg_id]
            )
            for c_id, c_fields in claimed:
                if c_fields is None:
                    continue
                stored = await self._parse_or_deadletter(stream, str(c_id), c_fields, group, consumer)
                if stored is not None:
                    # Re-queuing is handled by caller via the normal consume path;
                    # here we just ack invalid messages that were dead-lettered inside _parse_or_deadletter.
                    pass
                processed += 1

        return processed

    async def pending_messages(self, kind: PendingKind, *, group: str) -> list[PendingMessage]:
        stream = _kind_to_stream(self.config, kind)
        await self._ensure_group(stream, group)
        entries = await self._redis.xpending_range(
            stream,
            group,
            min="-",
            max="+",
            count=100,
        )
        return [
            PendingMessage(
                message_id=str(e["message_id"]),
                consumer=e["consumer"],
                idle_ms=e["time_since_delivered"],
                delivery_count=e["times_delivered"],
            )
            for e in entries
        ]

    async def ack(self, kind: StreamKind, *, group: str, stream_ids: list[str]) -> None:
        if not stream_ids:
            return
        await self._redis.xack(stream_name(self.config, kind), group, *stream_ids)

    async def snapshot(self) -> dict[str, Any]:
        if self._snapshot:
            return dict(self._snapshot)
        entries = await self._redis.xrevrange(self.config.snapshots_stream, count=1)
        if entries:
            raw = entries[0][1].get("snapshot")
            if raw:
                self._snapshot = json.loads(raw)
        return dict(self._snapshot)

    async def deadletter_recent(self, count: int = 20) -> list[dict[str, Any]]:
        entries = await self._redis.xrevrange(self.config.deadletter_stream, count=count)
        return [{"stream_id": str(stream_id), "fields": fields} for stream_id, fields in entries]

    async def close(self) -> None:
        await self._redis.aclose()

    async def _ensure_group(self, stream: str, group: str) -> None:
        try:
            await self._redis.xgroup_create(stream, group, id="0", mkstream=True)
        except Exception as exc:  # redis raises BUSYGROUP as ResponseError
            if "BUSYGROUP" not in str(exc):
                raise

    async def _parse_or_deadletter(
        self,
        stream: str,
        stream_id: str,
        fields: dict[str, str],
        group: str,
        consumer: str,
    ) -> StoredEnvelope | None:
        """Parse a Redis message, dead-lettering it with full metadata on failure."""
        raw = fields.get("envelope")

        async def _dl(reason: str, extra: dict[str, str] | None = None) -> None:
            entry: dict[str, str] = {
                "reason": reason,
                "original_stream": stream,
                "original_message_id": stream_id,
                "group": group,
                "consumer": consumer,
            }
            if extra:
                entry.update(extra)
            await self._redis.xadd(self.config.deadletter_stream, entry)
            await self._redis.xack(stream, group, stream_id)

        if not raw:
            await _dl("missing_envelope")
            return None

        try:
            envelope = BusEnvelope.model_validate_json(raw)
        except Exception as exc:  # noqa: BLE001
            await _dl("parse_error", {"raw_message": raw[:2000], "error": str(exc)[:500]})
            return None

        try:
            validate_envelope(envelope)
        except ContractError as exc:
            await _dl(
                "contract_error",
                {
                    "field": exc.field or "",
                    "error": exc.reason,
                    "envelope": raw[:2000],
                },
            )
            return None

        return StoredEnvelope(stream_id=str(stream_id), envelope=envelope)

    def _apply_snapshot(self, envelope: BusEnvelope) -> None:
        apply_snapshot(self._snapshot, envelope)


def apply_snapshot(snapshot: dict[str, Any], envelope: BusEnvelope) -> None:
    snapshot["last_event"] = envelope.model_dump(mode="json")
    if envelope.type == "agent.session.updated":
        snapshot["agent_session"] = envelope.payload
    elif envelope.type == "voice.transcription.completed":
        snapshot["transcript"] = envelope.payload
    elif envelope.type == "hermes.response.completed":
        snapshot["hermes_response"] = envelope.payload
    elif envelope.type == "agent.option.selected":
        snapshot["selected_option"] = envelope.payload
    elif envelope.type.startswith("voice.recording."):
        snapshot["recording"] = envelope.payload | {"event": envelope.type}
    elif envelope.type.startswith("voice.tts."):
        snapshot["tts"] = envelope.payload | {"event": envelope.type}
