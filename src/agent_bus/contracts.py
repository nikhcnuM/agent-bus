"""Contract registry and lightweight payload validation.

The contracts directory at ``<repo-root>/contracts`` is the source of truth.
This module loads ``event-types.json`` and the per-type schemas at import time
and exposes:

- ``KNOWN_TYPES``  - mapping of type -> registry entry.
- ``validate_envelope`` - validates the envelope shape and the payload for the
  registered type. Raises :class:`ContractError` on failure.

The validator implements only the JSON Schema subset we use in this repo:
``type`` (string or list of strings, including ``"null"``), ``required``,
``properties``, ``additionalProperties``, ``items``, ``enum`` and
``minLength``. That keeps the runtime free of extra dependencies and aligned
with the schemas we actually ship in ``contracts/schemas``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CONTRACTS_DIR = Path(__file__).resolve().parents[2] / "contracts"
REGISTRY_PATH = CONTRACTS_DIR / "event-types.json"


class ContractError(ValueError):
    """Raised when an envelope or payload fails contract validation."""

    def __init__(self, reason: str, *, field: str | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.field = field

    def to_dict(self) -> dict[str, str]:
        body: dict[str, str] = {"type": "contract_error", "reason": self.reason}
        if self.field is not None:
            body["field"] = self.field
        return body


@dataclass(frozen=True)
class TypeEntry:
    name: str
    kind: str  # "command" | "event"
    stream: str  # "commands" | "events"
    target_required: bool
    schema: dict[str, Any]


def _load_registry() -> tuple[dict[str, TypeEntry], list[str]]:
    raw = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    envelope_required = list(raw.get("envelope_required", []))
    entries: dict[str, TypeEntry] = {}
    for type_name, info in raw.get("types", {}).items():
        schema_path = CONTRACTS_DIR / info["schema"]
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        entries[type_name] = TypeEntry(
            name=type_name,
            kind=info["kind"],
            stream=info["stream"],
            target_required=bool(info.get("target_required", False)),
            schema=schema,
        )
    return entries, envelope_required


KNOWN_TYPES, ENVELOPE_REQUIRED = _load_registry()


def reload_registry() -> None:
    """Reload the registry from disk. Mostly useful for tests."""
    global KNOWN_TYPES, ENVELOPE_REQUIRED
    KNOWN_TYPES, ENVELOPE_REQUIRED = _load_registry()


def is_known_type(type_name: str) -> bool:
    return type_name in KNOWN_TYPES


def validate_envelope(envelope: Any) -> None:
    """Validate envelope shape and payload against the registered schema.

    Accepts either a Pydantic ``BusEnvelope`` (anything with the envelope
    attributes) or a plain dict. Raises :class:`ContractError` on the first
    problem found, with ``reason`` and ``field`` populated.
    """
    data = _coerce_envelope_dict(envelope)
    for required in ENVELOPE_REQUIRED:
        if required not in data or data[required] is None:
            if required == "payload" and isinstance(data.get("payload"), dict):
                continue
            raise ContractError("envelope field missing", field=required)

    type_name = data.get("type")
    if not isinstance(type_name, str) or not type_name:
        raise ContractError("envelope type must be a non-empty string", field="type")

    entry = KNOWN_TYPES.get(type_name)
    if entry is None:
        raise ContractError(f"unknown type: {type_name}", field="type")

    if entry.target_required:
        target = data.get("target")
        if not isinstance(target, str) or not target.strip():
            raise ContractError(
                f"target is required for command type {type_name}",
                field="target",
            )

    payload = data.get("payload")
    if not isinstance(payload, dict):
        raise ContractError("payload must be an object", field="payload")

    _validate(payload, entry.schema, path="payload")


def _coerce_envelope_dict(envelope: Any) -> dict[str, Any]:
    if isinstance(envelope, dict):
        return envelope
    if hasattr(envelope, "model_dump"):
        return envelope.model_dump(mode="json")
    fields = ("id", "type", "source", "timestamp", "correlation_id", "session_id", "target", "payload")
    return {name: getattr(envelope, name, None) for name in fields}


_BASIC_TYPES: dict[str, type | tuple[type, ...]] = {
    "string": str,
    "integer": int,
    "number": (int, float),
    "boolean": bool,
    "object": dict,
    "array": list,
    "null": type(None),
}


def _validate(value: Any, schema: dict[str, Any], *, path: str) -> None:
    expected = schema.get("type")
    if expected is not None and not _matches_type(value, expected):
        raise ContractError(f"{path} must be of type {expected}", field=path)

    if "enum" in schema and value not in schema["enum"]:
        raise ContractError(f"{path} must be one of {schema['enum']}", field=path)

    if isinstance(value, str):
        min_length = schema.get("minLength")
        if isinstance(min_length, int) and len(value) < min_length:
            raise ContractError(f"{path} must have minLength {min_length}", field=path)

    if isinstance(value, dict):
        required = schema.get("required") or []
        for key in required:
            if key not in value or value[key] is None:
                raise ContractError(f"{path}.{key} is required", field=f"{path}.{key}")
        properties = schema.get("properties") or {}
        for key, sub_schema in properties.items():
            if key in value:
                _validate(value[key], sub_schema, path=f"{path}.{key}")
        if schema.get("additionalProperties") is False:
            for key in value:
                if key not in properties:
                    raise ContractError(
                        f"{path}.{key} is not allowed",
                        field=f"{path}.{key}",
                    )

    if isinstance(value, list):
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for index, item in enumerate(value):
                _validate(item, item_schema, path=f"{path}[{index}]")


def _matches_type(value: Any, expected: Any) -> bool:
    if isinstance(expected, list):
        return any(_matches_type(value, item) for item in expected)
    expected_py = _BASIC_TYPES.get(expected)
    if expected_py is None:
        return True  # unknown -> don't reject; the schema is wrong, not the data
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    return isinstance(value, expected_py)
