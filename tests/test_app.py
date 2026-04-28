from __future__ import annotations

from fastapi.testclient import TestClient

from agent_bus.app import create_app
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
