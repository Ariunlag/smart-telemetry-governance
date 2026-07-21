from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Literal

from app.services.schema_extractor import (
    MAX_DEPTH,
    MAX_DOCUMENT_BYTES,
    MAX_FIELDS,
    MAX_NODES,
    MAX_PATH_LENGTH,
    json_object_path_segment,
)

ScalarValueType = Literal["integer", "float", "boolean", "string"]
ScalarValue = int | float | bool | str


class FieldValueExtractionFailure(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class ScalarFieldValue:
    field_path: str
    value_type: ScalarValueType
    value: ScalarValue


@dataclass(frozen=True)
class ScalarExtraction:
    values: tuple[ScalarFieldValue, ...]
    skipped_null_count: int
    skipped_container_count: int


class FieldValueExtractor:
    """Bounded scalar extraction using the schema extractor's JSON-bracket paths."""

    def extract(self, payload: bytes) -> ScalarExtraction:
        if len(payload) > MAX_DOCUMENT_BYTES:
            raise FieldValueExtractionFailure("document_too_large")
        try:
            root = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise FieldValueExtractionFailure("invalid_json") from error
        if not isinstance(root, dict):
            raise FieldValueExtractionFailure("root_not_object")
        values: dict[str, ScalarFieldValue] = {}
        skipped_null_count = [0]
        skipped_container_count = [0]
        self._walk(
            root,
            "$",
            0,
            values,
            [0],
            skipped_null_count,
            skipped_container_count,
            is_root=True,
        )
        return ScalarExtraction(
            tuple(values[path] for path in sorted(values)),
            skipped_null_count[0],
            skipped_container_count[0],
        )

    def _walk(
        self,
        value: object,
        path: str,
        depth: int,
        values: dict[str, ScalarFieldValue],
        nodes: list[int],
        skipped_null_count: list[int],
        skipped_container_count: list[int],
        *,
        is_root: bool,
    ) -> None:
        nodes[0] += 1
        if nodes[0] > MAX_NODES:
            raise FieldValueExtractionFailure("field_node_limit_exceeded")
        if depth > MAX_DEPTH:
            raise FieldValueExtractionFailure("field_depth_exceeded")
        if len(path) > MAX_PATH_LENGTH:
            raise FieldValueExtractionFailure("field_path_too_long")
        if value is None:
            skipped_null_count[0] += 1
            return
        if isinstance(value, dict):
            skipped_container_count[0] += 1
            for key in sorted(value):
                self._walk(
                    value[key],
                    json_object_path_segment(path, key),
                    depth + 1,
                    values,
                    nodes,
                    skipped_null_count,
                    skipped_container_count,
                    is_root=False,
                )
            return
        if isinstance(value, list):
            skipped_container_count[0] += 1
            return
        if is_root:
            return
        field_value = self._scalar(path, value)
        if len(values) >= MAX_FIELDS:
            raise FieldValueExtractionFailure("field_count_exceeded")
        values[path] = field_value

    @staticmethod
    def _scalar(path: str, value: object) -> ScalarFieldValue:
        if isinstance(value, bool):
            return ScalarFieldValue(path, "boolean", value)
        if isinstance(value, int):
            return ScalarFieldValue(path, "integer", value)
        if isinstance(value, float):
            if not math.isfinite(value):
                raise FieldValueExtractionFailure("non_finite_number")
            return ScalarFieldValue(path, "float", value)
        if isinstance(value, str):
            return ScalarFieldValue(path, "string", value)
        raise FieldValueExtractionFailure("root_not_object")
