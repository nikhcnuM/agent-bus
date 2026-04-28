from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator

StreamKind = Literal["commands", "events"]


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def event_id() -> str:
    return "evt_" + uuid4().hex


class BusEnvelope(BaseModel):
    id: str = Field(default_factory=event_id)
    type: str
    source: str
    timestamp: str = Field(default_factory=utc_timestamp)
    correlation_id: str | None = None
    session_id: str | None = None
    target: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("type", "source")
    @classmethod
    def _required_string(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("must not be empty")
        return normalized


class StoredEnvelope(BaseModel):
    stream_id: str
    envelope: BusEnvelope


class ConsumeResponse(BaseModel):
    messages: list[StoredEnvelope] = Field(default_factory=list)


class AckRequest(BaseModel):
    stream_ids: list[str]


class HealthResponse(BaseModel):
    ok: bool
    backend: str
    streams: dict[str, str]


class SnapshotResponse(BaseModel):
    snapshot: dict[str, Any] = Field(default_factory=dict)


class DeadletterResponse(BaseModel):
    entries: list[dict[str, Any]] = Field(default_factory=list)
