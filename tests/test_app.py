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
                "payload": {"session_id": "ptt-1", "transcript": "hola"},
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
                    "payload": {"option_id": "ack", "selected_control": "top_1"},
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
        self._pending: list[dict] = []
        self._xclaim_results: list[tuple] = []

    async def xgroup_create(self, stream, group, id="0", mkstream=True):
        self.groups.append((stream, group, id, mkstream))

    async def xreadgroup(self, group, consumer, streams, count=10, block=1):
        return [("agentbus.events", [("1-0", {"not_envelope": "{}"})])]

    async def xadd(self, stream, fields, **kwargs):
        self.xadds.append((stream, fields, kwargs))
        return "2-0"

    async def xack(self, stream, group, stream_id):
        self.xacks.append((stream, group, stream_id))

    async def xpending_range(self, stream, group, min="-", max="+", count=100):
        return list(self._pending)

    async def xclaim(self, stream, group, consumer, min_idle_time=0, message_ids=None):
        return list(self._xclaim_results)

    async def xrevrange(self, stream, count=20):
        return []

    async def aclose(self):
        pass


@pytest.mark.anyio
async def test_redis_backend_deadletters_and_acks_missing_envelope() -> None:
    config = BusConfig()
    backend = RedisStreamBackend.__new__(RedisStreamBackend)
    backend.config = config
    backend._redis = FakeRedis()
    backend._snapshot = {}

    messages = await backend.consume("events", group="launchpad", consumer="test")

    assert messages == []
    # Dead-letter entry should have enriched metadata
    assert len(backend._redis.xadds) == 1
    dl_stream, dl_fields, _ = backend._redis.xadds[0]
    assert dl_stream == "agentbus.deadletter"
    assert dl_fields["reason"] == "missing_envelope"
    assert dl_fields["original_stream"] == "agentbus.events"
    assert dl_fields["original_message_id"] == "1-0"
    assert dl_fields["group"] == "launchpad"
    assert dl_fields["consumer"] == "test"
    assert backend._redis.xacks == [("agentbus.events", "launchpad", "1-0")]


@pytest.mark.anyio
async def test_recover_pending_skips_non_idle_messages() -> None:
    config = BusConfig(pending_idle_ms=30_000, pending_claim_count=10, max_delivery_attempts=5)
    backend = RedisStreamBackend.__new__(RedisStreamBackend)
    backend.config = config
    backend._redis = FakeRedis()
    backend._snapshot = {}
    backend._redis._pending = [
        {"message_id": "1-0", "consumer": "consumer-a", "time_since_delivered": 1000, "times_delivered": 1}
    ]

    processed = await backend.recover_pending("events", group="grp", consumer="consumer-b")

    assert processed == 0
    assert backend._redis.xadds == []


@pytest.mark.anyio
async def test_recover_pending_deadletters_over_max_attempts() -> None:
    config = BusConfig(pending_idle_ms=30_000, pending_claim_count=10, max_delivery_attempts=3)
    backend = RedisStreamBackend.__new__(RedisStreamBackend)
    backend.config = config
    backend._redis = FakeRedis()
    backend._snapshot = {}
    backend._redis._pending = [
        {"message_id": "5-0", "consumer": "consumer-a", "time_since_delivered": 60_000, "times_delivered": 5}
    ]
    raw_env = '{"id":"evt_x","type":"voice.ptt.start","source":"launchpad","timestamp":"2026-01-01T00:00:00Z","payload":{"control":"grid:0,0"}}'
    backend._redis._xclaim_results = [("5-0", {"envelope": raw_env})]

    processed = await backend.recover_pending("events", group="grp", consumer="consumer-b")

    assert processed == 1
    assert len(backend._redis.xadds) == 1
    dl_stream, dl_fields, _ = backend._redis.xadds[0]
    assert dl_stream == "agentbus.deadletter"
    assert dl_fields["reason"] == "max_delivery_attempts_exceeded"
    assert dl_fields["original_message_id"] == "5-0"
    assert dl_fields["group"] == "grp"
    assert dl_fields["delivery_count"] == "5"
    assert "envelope" in dl_fields
    assert backend._redis.xacks == [("agentbus.events", "grp", "5-0")]


@pytest.mark.anyio
async def test_recover_pending_reclaims_idle_within_budget() -> None:
    config = BusConfig(pending_idle_ms=30_000, pending_claim_count=10, max_delivery_attempts=5)
    backend = RedisStreamBackend.__new__(RedisStreamBackend)
    backend.config = config
    backend._redis = FakeRedis()
    backend._snapshot = {}
    backend._redis._pending = [
        {"message_id": "3-0", "consumer": "consumer-a", "time_since_delivered": 60_000, "times_delivered": 2}
    ]
    raw_env = '{"id":"evt_y","type":"agent.option.selected","source":"launchpad-system-actions","timestamp":"2026-01-01T00:00:00Z","payload":{"option_id":"retry","selected_control":"top_1"}}'
    backend._redis._xclaim_results = [("3-0", {"envelope": raw_env})]

    processed = await backend.recover_pending("events", group="grp", consumer="consumer-b")

    assert processed == 1
    # No dead-letter entries for a valid message within retry budget
    assert backend._redis.xadds == []


def test_pending_endpoint_returns_empty_for_memory_backend() -> None:
    with client() as c:
        response = c.get("/streams/events/pending?group=test-group")

    assert response.status_code == 200
    body = response.json()
    assert body["stream"] == "agentbus.events"
    assert body["group"] == "test-group"
    assert body["pending"] == []


def test_pending_endpoint_rejects_invalid_kind() -> None:
    with client() as c:
        response = c.get("/streams/foobar/pending?group=test-group")

    assert response.status_code == 422


def test_pending_endpoint_requires_group() -> None:
    with client() as c:
        response = c.get("/streams/events/pending")

    assert response.status_code == 422
