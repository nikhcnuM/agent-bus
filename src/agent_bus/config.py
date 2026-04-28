from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class ServerConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8790


class BusConfig(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    redis_url: str = "redis://127.0.0.1:6379/0"
    stream_prefix: str = "agentbus"
    retention_maxlen: int = 10000
    replay_window: int = 100
    default_group: str = "agent-assistant"

    @property
    def commands_stream(self) -> str:
        return f"{self.stream_prefix}.commands"

    @property
    def events_stream(self) -> str:
        return f"{self.stream_prefix}.events"

    @property
    def deadletter_stream(self) -> str:
        return f"{self.stream_prefix}.deadletter"

    @property
    def snapshots_stream(self) -> str:
        return f"{self.stream_prefix}.snapshots"


def load_config(path: str | Path | None = None) -> BusConfig:
    if path is None:
        return BusConfig()
    config_path = Path(path)
    data: dict[str, Any] = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    return BusConfig.model_validate(data)
