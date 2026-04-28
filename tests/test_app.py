from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from agent_bus.app import create_app
from agent_bus.backend import RedisStreamBackend
from agent_bus.config import BusConfig


def client() -> TestClient:
    return TestClient(create_app(BusConfig(redis_url="memory://")))


def test_health_reports_streams() -> None:
    with client() as c:
        response = c.get("/health")

    assert response.status_code == 200
    assert response.json()["backend"] == "memory"
    assert response.json()["streams"]["commands"] == "agentbus.commands"


def test_publish_event_updates_snapshot_and_consumes_by_group() -> None:
    with client() as c:
        publish = c.post(
            "/events",
            json={
                "type": "voice.transcription.completed",
                "source": "agent-voice-gateway",
                "correlation_id": "ptt-1",
                "payload": {"transcript": "hola"},
            },
        )
        consumed = c.get("/consume/events?group=mac-widget-hermes&consumer=test")
        snapshot = c.get("/snapshot")

    assert publish.status_code == 200
    assert consumed.json()["messages"][0]["envelope"]["type"] == "voice.transcription.completed"
    assert snapshot.json()["snapshot"]["transcript"]["transcript"] == "hola"


def test_command_stream_is_separate_from_events() -> None:
    with client() as c:
        c.post(
            "/commands",
            json={
                "type": "voice.ptt.start",
                "source": "launchpad-system-actions",
                "target": "agent-voice-gateway",
                "payload": {"control": "grid:0,0"},
            },
        )
        commands = c.get("/consume/commands?group=agent-voice-gateway&consumer=test")
        events = c.get("/consume/events?group=agent-voice-gateway&consumer=test")

    assert commands.json()["messages"][0]["envelope"]["type"] == "voice.ptt.start"
    assert events.json()["messages"] == []


def test_websocket_receives_published_events() -> None:
    with client() as c:
        with c.websocket_connect("/ws") as websocket:
            assert "snapshot" in websocket.receive_json()
            c.post(
                "/events",
                json={
                    "type": "agent.option.selected",
                    "source": "launchpad-system-actions",
                    "payload": {"option_id": "ack"},
                },
            )
            message = websocket.receive_json()

    assert message["envelope"]["type"] == "agent.option.selected"


def test_websocket_closes_cleanly_for_invalid_envelope() -> None:
    with client() as c:
        with c.websocket_connect("/ws") as websocket:
            assert "snapshot" in websocket.receive_json()
            websocket.send_json({"source": "manual-test", "payload": {}})
            with pytest.raises(WebSocketDisconnect) as exc_info:
                websocket.receive_json()

    assert exc_info.value.code == 1003


def test_deadletter_recent_defaults_to_empty_list() -> None:
    with client() as c:
        response = c.get("/deadletter/recent")

    assert response.status_code == 200
    assert response.json() == {"entries": []}


class FakeRedis:
    def __init__(self):
        self.xadds = []
        self.xacks = []
        self.groups = []

    async def xgroup_create(self, stream, group, id="0", mkstream=True):
        self.groups.append((stream, group, id, mkstream))

    async def xreadgroup(self, group, consumer, streams, count=10, block=1):
        return [("agentbus.events", [("1-0", {"not_envelope": "{}"})])]

    async def xadd(self, stream, fields, **kwargs):
        self.xadds.append((stream, fields, kwargs))
        return "2-0"

    async def xack(self, stream, group, stream_id):
        self.xacks.append((stream, group, stream_id))


@pytest.mark.anyio
async def test_redis_backend_deadletters_and_acks_missing_envelope() -> None:
    config = BusConfig()
    backend = RedisStreamBackend.__new__(RedisStreamBackend)
    backend.config = config
    backend._redis = FakeRedis()
    backend._snapshot = {}

    messages = await backend.consume("events", group="launchpad", consumer="test")

    assert messages == []
    assert backend._redis.xadds == [
        (
            "agentbus.deadletter",
            {"stream_id": "1-0", "reason": "missing envelope"},
            {},
        )
    ]
    assert backend._redis.xacks == [("agentbus.events", "launchpad", "1-0")]
