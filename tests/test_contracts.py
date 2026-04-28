from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from agent_bus.app import create_app
from agent_bus.backend import RedisStreamBackend, apply_snapshot
from agent_bus.config import BusConfig
from agent_bus.contracts import CONTRACTS_DIR, KNOWN_TYPES, ContractError, validate_envelope
from agent_bus.models import BusEnvelope


SPRINT_TYPES: tuple[str, ...] = (
    "voice.ptt.start",
    "voice.ptt.stop",
    "voice.ptt.cancel",
    "voice.tts.speak",
    "voice.recording.started",
    "voice.recording.stopped",
    "voice.recording.cancelled",
    "voice.transcription.completed",
    "voice.transcription.empty",
    "voice.tts.started",
    "voice.tts.completed",
    "voice.tts.failed",
    "hermes.request.started",
    "hermes.response.completed",
    "hermes.request.failed",
    "agent.session.updated",
    "agent.option.selected",
)


def _load_fixture(type_name: str) -> dict:
    fixture_path = CONTRACTS_DIR / "fixtures" / f"{type_name}.json"
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def client() -> TestClient:
    return TestClient(create_app(BusConfig(redis_url="memory://")))


def test_registry_lists_every_type_required_by_sprint() -> None:
    assert set(SPRINT_TYPES).issubset(set(KNOWN_TYPES.keys()))


def test_every_registered_type_has_a_fixture_and_schema() -> None:
    fixtures_dir = CONTRACTS_DIR / "fixtures"
    schemas_dir = CONTRACTS_DIR / "schemas"
    for type_name in SPRINT_TYPES:
        assert (fixtures_dir / f"{type_name}.json").is_file(), f"missing fixture for {type_name}"
        assert (schemas_dir / f"{type_name}.json").is_file(), f"missing schema for {type_name}"


@pytest.mark.parametrize("type_name", SPRINT_TYPES)
def test_each_fixture_validates_against_its_schema(type_name: str) -> None:
    fixture = _load_fixture(type_name)
    assert fixture["type"] == type_name
    validate_envelope(fixture)


@pytest.mark.parametrize("type_name", SPRINT_TYPES)
def test_apply_snapshot_does_not_silently_drop_known_types(type_name: str) -> None:
    fixture = _load_fixture(type_name)
    envelope = BusEnvelope.model_validate(fixture)
    snapshot: dict = {}
    apply_snapshot(snapshot, envelope)
    assert snapshot["last_event"]["type"] == type_name


def test_voice_transcription_completed_requires_transcript_string() -> None:
    fixture = _load_fixture("voice.transcription.completed")
    fixture["payload"].pop("transcript")
    with pytest.raises(ContractError) as info:
        validate_envelope(fixture)
    assert info.value.field == "payload.transcript"


def test_agent_session_updated_options_must_carry_option_id() -> None:
    fixture = _load_fixture("agent.session.updated")
    fixture["payload"]["options"][0].pop("option_id")
    with pytest.raises(ContractError) as info:
        validate_envelope(fixture)
    assert "option_id" in (info.value.field or "")


def test_command_kind_rejects_publishing_on_events_endpoint() -> None:
    fixture = _load_fixture("voice.ptt.start")
    with client() as c:
        response = c.post("/events", json=fixture)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["reason"].startswith("type voice.ptt.start must be published as command")
    assert detail["field"] == "type"


def test_event_kind_rejects_publishing_on_commands_endpoint() -> None:
    fixture = _load_fixture("voice.transcription.completed")
    with client() as c:
        response = c.post("/commands", json=fixture)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["reason"].startswith("type voice.transcription.completed must be published as event")


def test_unknown_type_is_rejected_with_422() -> None:
    payload = {
        "type": "voice.something.invented",
        "source": "test",
        "payload": {},
    }
    with client() as c:
        response = c.post("/events", json=payload)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["reason"] == "unknown type: voice.something.invented"
    assert detail["field"] == "type"


def test_invalid_payload_is_rejected_with_field_in_422() -> None:
    fixture = _load_fixture("voice.transcription.completed")
    fixture["payload"].pop("transcript")
    with client() as c:
        response = c.post("/events", json=fixture)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["field"] == "payload.transcript"


def test_command_publish_requires_target() -> None:
    fixture = _load_fixture("voice.ptt.start")
    fixture["target"] = None
    with client() as c:
        response = c.post("/commands", json=fixture)
    assert response.status_code == 422
    assert response.json()["detail"]["field"] == "target"


def test_websocket_closes_for_unknown_type() -> None:
    with client() as c:
        with c.websocket_connect("/ws") as websocket:
            assert "snapshot" in websocket.receive_json()
            websocket.send_json({
                "type": "voice.something.invented",
                "source": "manual-test",
                "payload": {},
            })
            with pytest.raises(WebSocketDisconnect) as info:
                websocket.receive_json()
    assert info.value.code == 1003


def test_websocket_closes_for_invalid_payload_for_known_type() -> None:
    with client() as c:
        with c.websocket_connect("/ws") as websocket:
            assert "snapshot" in websocket.receive_json()
            websocket.send_json({
                "type": "voice.transcription.completed",
                "source": "manual-test",
                "payload": {"session_id": "x"},
            })
            with pytest.raises(WebSocketDisconnect) as info:
                websocket.receive_json()
    assert info.value.code == 1003


class _FakeRedis:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.xadds: list = []
        self.xacks: list = []

    async def xgroup_create(self, stream, group, id="0", mkstream=True):
        return None

    async def xreadgroup(self, group, consumer, streams, count=10, block=1):
        return [("agentbus.events", [("9-0", {"envelope": self.payload})])]

    async def xadd(self, stream, fields, **kwargs):
        self.xadds.append((stream, fields))
        return "10-0"

    async def xack(self, stream, group, stream_id):
        self.xacks.append((stream, group, stream_id))


@pytest.mark.anyio
async def test_redis_backend_deadletters_invalid_payload_for_known_type() -> None:
    config = BusConfig()
    raw = json.dumps(
        {
            "id": "evt_x",
            "type": "voice.transcription.completed",
            "source": "agent-voice-gateway",
            "timestamp": "2026-04-28T16:00:00Z",
            "payload": {"session_id": "ptt-1"},
        }
    )
    fake = _FakeRedis(raw)
    backend = RedisStreamBackend.__new__(RedisStreamBackend)
    backend.config = config
    backend._redis = fake
    backend._snapshot = {}

    messages = await backend.consume("events", group="g", consumer="c")

    assert messages == []
    assert fake.xacks == [("agentbus.events", "g", "9-0")]
    assert fake.xadds and fake.xadds[0][0] == "agentbus.deadletter"
    fields = fake.xadds[0][1]
    assert fields["reason"] == "payload.transcript is required"
    assert fields["field"] == "payload.transcript"
