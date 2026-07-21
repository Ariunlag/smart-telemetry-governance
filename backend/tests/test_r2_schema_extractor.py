from __future__ import annotations

import json

import pytest

import app.services.schema_extractor as extractor_module
from app.services.schema_extractor import (
    SchemaExtractionFailure,
    SchemaExtractor,
    SchemaObservation,
)

pytestmark = pytest.mark.schema_observation


def extract(value: object) -> SchemaObservation:
    return SchemaExtractor().extract(json.dumps(value, ensure_ascii=False).encode())


def test_fingerprint_ignores_key_order_and_scalar_values() -> None:
    assert (
        extract({"a": 1, "b": "first"}).fingerprint
        == extract({"b": "second", "a": 999}).fingerprint
    )
    assert extract({"a": 1}).fingerprint != extract({"a": 1.5}).fingerprint


def test_json_bracket_paths_prevent_source_key_collisions() -> None:
    dotted = extract({"a.b": 1})
    nested = extract({"a": {"b": 1}})
    assert dotted.fingerprint != nested.fingerprint
    assert '$["a.b"]' in {field.path for field in dotted.fields}
    assert '$["a"]["b"]' in {field.path for field in nested.fields}
    paths = {field.path for field in extract({'"\\[]/~items[]': 1, "": 2, "µ": 3}).fields}
    assert '$["\\"\\\\[]/~items[]"]' in paths and '$[""]' in paths and '$["µ"]' in paths


def test_arrays_are_index_free_and_order_independent() -> None:
    one = extract([{"a": 1}])
    many = extract([{"a": 3}, {"a": 2}])
    reversed_items = extract([{"a": 2}, {"a": 3}])
    assert one.fingerprint == many.fingerprint == reversed_items.fingerprint
    assert all("0" not in field.path and "1" not in field.path for field in many.fields)
    mixed = extract([1, 2.0])
    assert next(field for field in mixed.fields if field.path == "$[]").value_type == "mixed"


def test_nullability_is_structural_for_scalars_and_arrays() -> None:
    scalar = extract({"temperature": None})
    assert next(field for field in scalar.fields if field.path == '$["temperature"]').nullable
    values = extract({"values": [1, None]})
    item = next(field for field in values.fields if field.path == '$["values"][]')
    assert item.value_type == "mixed" and item.nullable


def test_metadata_excludes_scalar_values_and_supports_all_roots() -> None:
    value = {"secret": "PRIVATE_SENSOR_SECRET_7319", "identifier": "device-unique-identifier-8492"}
    observation = extract(value)
    rendered = json.dumps(observation.document)
    assert (
        "PRIVATE_SENSOR_SECRET_7319" not in rendered
        and "device-unique-identifier-8492" not in rendered
    )
    roots: tuple[object, ...] = ({}, [], "x", 1, 1.5, True, None)
    assert [extract(value).root_type for value in roots] == [
        "object",
        "array",
        "string",
        "integer",
        "number",
        "boolean",
        "null",
    ]


def test_depth_bound_is_bounded_and_safe() -> None:
    value: object = 1
    for _ in range(17):
        value = {"a": value}
    with pytest.raises(SchemaExtractionFailure, match="schema_depth_exceeded"):
        extract(value)


@pytest.mark.parametrize(
    ("limit_name", "limit", "value", "code"),
    [
        ("MAX_FIELDS", 2, {"a": 1, "b": 2}, "schema_field_limit_exceeded"),
        ("MAX_NODES", 2, {"a": {"b": 1}}, "schema_node_limit_exceeded"),
        ("MAX_PATH_LENGTH", 4, {"secret-key": 1}, "schema_path_too_long"),
        ("MAX_DOCUMENT_BYTES", 10, {"a": 1}, "schema_document_too_large"),
    ],
)
def test_configured_bounds_are_deterministic_and_sanitized(
    monkeypatch: pytest.MonkeyPatch, limit_name: str, limit: int, value: object, code: str
) -> None:
    monkeypatch.setattr(extractor_module, limit_name, limit)
    for _ in range(2):
        with pytest.raises(SchemaExtractionFailure) as raised:
            extract(value)
        assert raised.value.code == code
        assert json.dumps(value) not in str(raised.value)
