from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Protocol

from agent_bus.config import BusConfig
from agent_bus.models import BusEnvelope, StoredEnvelope, StreamKind


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

    async def close(self) -> None: ...


def stream_name(config: BusConfig, kind: StreamKind) -> str:
    return config.commands_stream if kind == "commands" else config.events_stream


def build_backend(config: BusConfig) -> BusBackend:
    if config.redis_url == "memory://":
        return InMemoryBackend(config)
    return RedisStreamBackend(config)


@dataclass
class InMemoryBackend:
    config: BusConfig
    name: str = "memory"
    _streams: dict[str, list[StoredEnvelope]] = field(default_factory=lambda: defaultdict(list))
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
                raw = fields.get("envelope")
                if not raw:
                    await self._redis.xadd(self.config.deadletter_stream, {"stream_id": stream_id, "reason": "missing envelope"})
                    continue
                try:
                    envelope = BusEnvelope.model_validate_json(raw)
                except Exception as exc:  # noqa: BLE001 - dead-letter boundary
                    await self._redis.xadd(
                        self.config.deadletter_stream,
                        {"stream_id": stream_id, "reason": str(exc), "payload": raw},
                    )
                    await self._redis.xack(stream, group, stream_id)
                    continue
                messages.append(StoredEnvelope(stream_id=str(stream_id), envelope=envelope))
        return messages

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

    async def close(self) -> None:
        await self._redis.aclose()

    async def _ensure_group(self, stream: str, group: str) -> None:
        try:
            await self._redis.xgroup_create(stream, group, id="0", mkstream=True)
        except Exception as exc:  # redis raises BUSYGROUP as ResponseError
            if "BUSYGROUP" not in str(exc):
                raise

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
