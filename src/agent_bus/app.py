from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect, status
from pydantic import ValidationError
from starlette.websockets import WebSocketState

from agent_bus.backend import BusBackend, build_backend
from agent_bus.config import BusConfig
from agent_bus.models import AckRequest, BusEnvelope, ConsumeResponse, DeadletterResponse, HealthResponse, SnapshotResponse, StoredEnvelope, StreamKind


class WebsocketHub:
    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._clients.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        self._clients.discard(websocket)

    async def broadcast(self, stored: StoredEnvelope) -> None:
        stale: list[WebSocket] = []
        payload = stored.model_dump(mode="json")
        for websocket in list(self._clients):
            try:
                await websocket.send_json(payload)
            except Exception:  # noqa: BLE001 - websocket best effort
                stale.append(websocket)
        for websocket in stale:
            self.disconnect(websocket)


def create_app(config: BusConfig | None = None, backend: BusBackend | None = None) -> FastAPI:
    config = config or BusConfig()
    hub = WebsocketHub()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.config = config
        app.state.backend = backend or build_backend(config)
        app.state.hub = hub
        try:
            yield
        finally:
            await app.state.backend.close()

    app = FastAPI(title="Agent Bus", version="0.1.0", lifespan=lifespan)

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        active_backend: BusBackend = app.state.backend
        return HealthResponse(
            ok=True,
            backend=active_backend.name,
            streams={
                "commands": config.commands_stream,
                "events": config.events_stream,
                "deadletter": config.deadletter_stream,
                "snapshots": config.snapshots_stream,
            },
        )

    @app.get("/snapshot", response_model=SnapshotResponse)
    async def snapshot() -> SnapshotResponse:
        return SnapshotResponse(snapshot=await app.state.backend.snapshot())

    @app.get("/deadletter/recent", response_model=DeadletterResponse)
    async def deadletter_recent(count: int = 20) -> DeadletterResponse:
        bounded_count = min(max(count, 1), 100)
        return DeadletterResponse(entries=await app.state.backend.deadletter_recent(bounded_count))

    @app.post("/events", response_model=StoredEnvelope)
    async def publish_event(envelope: BusEnvelope) -> StoredEnvelope:
        return await _publish(app, "events", envelope)

    @app.post("/commands", response_model=StoredEnvelope)
    async def publish_command(envelope: BusEnvelope) -> StoredEnvelope:
        return await _publish(app, "commands", envelope)

    @app.get("/consume/{kind}", response_model=ConsumeResponse)
    async def consume(
        kind: StreamKind,
        group: Annotated[str, Query(min_length=1)],
        consumer: Annotated[str, Query(min_length=1)],
        count: int = 10,
        block_ms: int = 1,
    ) -> ConsumeResponse:
        messages = await app.state.backend.consume(
            kind,
            group=group,
            consumer=consumer,
            count=count,
            block_ms=block_ms,
        )
        return ConsumeResponse(messages=messages)

    @app.post("/consume/{kind}/ack")
    async def ack(kind: StreamKind, payload: AckRequest, group: Annotated[str, Query(min_length=1)]):
        await app.state.backend.ack(kind, group=group, stream_ids=payload.stream_ids)
        return {"ok": True}

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await hub.connect(websocket)
        try:
            await websocket.send_json({"snapshot": await app.state.backend.snapshot()})
            while True:
                envelope = BusEnvelope.model_validate(await websocket.receive_json())
                kind: StreamKind = "commands" if envelope.target else "events"
                await _publish(app, kind, envelope)
        except WebSocketDisconnect:
            hub.disconnect(websocket)
        except (ValidationError, ValueError, TypeError):
            hub.disconnect(websocket)
            if websocket.client_state != WebSocketState.DISCONNECTED:
                await websocket.close(code=status.WS_1003_UNSUPPORTED_DATA)
        except Exception:  # noqa: BLE001
            hub.disconnect(websocket)
            if websocket.client_state != WebSocketState.DISCONNECTED:
                await websocket.close(code=status.WS_1011_INTERNAL_ERROR)

    return app


async def _publish(app: FastAPI, kind: StreamKind, envelope: BusEnvelope) -> StoredEnvelope:
    stored = await app.state.backend.publish(kind, envelope)
    await app.state.hub.broadcast(stored)
    return stored
