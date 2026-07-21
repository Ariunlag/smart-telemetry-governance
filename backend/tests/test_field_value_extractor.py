from __future__ import annotations

import json

import pytest

from app.services.field_value_extractor import (
    FieldValueExtractionFailure,
    FieldValueExtractor,
    ScalarFieldValue,
)


def extract(value: object) -> tuple[ScalarFieldValue, ...]:
    return FieldValueExtractor().extract(json.dumps(value, ensure_ascii=False).encode()).values


def test_extracts_nested_scalars_with_deterministic_paths() -> None:
    result = extract({"temperature": 21, "device": {"status": "ready"}, "enabled": True})
    assert result == (
        ScalarFieldValue('$["device"]["status"]', "string", "ready"),
        ScalarFieldValue('$["enabled"]', "boolean", True),
        ScalarFieldValue('$["temperature"]', "integer", 21),
    )


def test_paths_are_collision_safe_for_special_object_keys() -> None:
    result = extract({'a"b': 1, "a[b]": 2, "a.b": 3, "a b": 4, "温度": 5})
    assert [field.field_path for field in result] == [
        '$["a b"]',
        '$["a.b"]',
        '$["a[b]"]',
        '$["a\\"b"]',
        '$["温度"]',
    ]


def test_distinguishes_booleans_integers_and_finite_floats() -> None:
    result = extract({"false": False, "integer": 1, "float": 1.25})
    assert result == (
        ScalarFieldValue('$["false"]', "boolean", False),
        ScalarFieldValue('$["float"]', "float", 1.25),
        ScalarFieldValue('$["integer"]', "integer", 1),
    )


def test_nulls_and_containers_are_skipped_without_array_flattening() -> None:
    result = FieldValueExtractor().extract(
        b'{"keep":"value","null":null,"nested":{"number":2},"items":[1,{"ignored":true}]}'
    )
    assert result.values == (
        ScalarFieldValue('$["keep"]', "string", "value"),
        ScalarFieldValue('$["nested"]["number"]', "integer", 2),
    )
    assert result.skipped_null_count == 1
    assert result.skipped_container_count == 3


def test_ordering_and_repeated_extraction_are_identical() -> None:
    payload = b'{"z":1,"a":{"z":"last","a":"first"},"m":true}'
    extractor = FieldValueExtractor()
    assert extractor.extract(payload) == extractor.extract(payload)
    assert [field.field_path for field in extractor.extract(payload).values] == [
        '$["a"]["a"]',
        '$["a"]["z"]',
        '$["m"]',
        '$["z"]',
    ]


@pytest.mark.parametrize(
    ("payload", "code"),
    (
        (b"not-json", "invalid_json"),
        (b"[]", "root_not_object"),
        (b'"scalar"', "root_not_object"),
    ),
)
def test_rejects_invalid_json_and_non_object_roots(payload: bytes, code: str) -> None:
    with pytest.raises(FieldValueExtractionFailure) as error:
        FieldValueExtractor().extract(payload)
    assert error.value.code == code
    assert payload.decode(errors="replace") not in str(error.value)


def test_depth_bound_is_enforced() -> None:
    value: object = 1
    for _ in range(17):
        value = {"nested": value}
    with pytest.raises(FieldValueExtractionFailure, match="field_depth_exceeded"):
        FieldValueExtractor().extract(json.dumps(value).encode())


def test_field_count_bound_is_enforced() -> None:
    payload = json.dumps({f"field-{number}": number for number in range(257)}).encode()
    with pytest.raises(FieldValueExtractionFailure, match="field_count_exceeded"):
        FieldValueExtractor().extract(payload)


def test_node_count_bound_is_enforced_without_traversing_array_members() -> None:
    payload = json.dumps(
        {f"object-{number}": {"a": [], "b": [], "c": []} for number in range(256)}
    ).encode()
    with pytest.raises(FieldValueExtractionFailure, match="field_node_limit_exceeded"):
        FieldValueExtractor().extract(payload)


def test_path_length_and_document_size_bounds_are_enforced() -> None:
    value: object = 1
    for _ in range(16):
        value = {"x" * 64: value}
    with pytest.raises(FieldValueExtractionFailure, match="field_path_too_long"):
        FieldValueExtractor().extract(json.dumps(value).encode())
    oversized = json.dumps({"value": "x" * 65536}).encode()
    with pytest.raises(FieldValueExtractionFailure, match="document_too_large"):
        FieldValueExtractor().extract(oversized)


@pytest.mark.parametrize("payload", (b'{"value":NaN}', b'{"value":Infinity}'))
def test_non_finite_numbers_are_rejected(payload: bytes) -> None:
    with pytest.raises(FieldValueExtractionFailure, match="non_finite_number"):
        FieldValueExtractor().extract(payload)
