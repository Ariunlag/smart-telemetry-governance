from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Literal

FINGERPRINT_VERSION = "r2.schema-fingerprint.v1"
MAX_DEPTH, MAX_FIELDS, MAX_NODES, MAX_PATH_LENGTH, MAX_DOCUMENT_BYTES = 16, 256, 1024, 1024, 65536
StructuralType = Literal[
    "object", "array", "string", "integer", "number", "boolean", "null", "mixed"
]


class SchemaExtractionFailure(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def json_object_path_segment(path: str, key: str) -> str:
    segment = json.dumps(key, ensure_ascii=False, separators=(",", ":"))
    return f"{path}[{segment}]"


@dataclass(frozen=True)
class ObservedFieldDefinition:
    path: str
    value_type: StructuralType
    depth: int
    nullable: bool


@dataclass(frozen=True)
class SchemaObservation:
    root_type: StructuralType
    fields: tuple[ObservedFieldDefinition, ...]
    document: dict[str, object]
    fingerprint: str


class SchemaExtractor:
    """Bounded structural JSON extraction; array length and scalar values are ignored."""

    def extract(self, payload: bytes) -> SchemaObservation:
        try:
            value = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise SchemaExtractionFailure("invalid_json") from error
        fields: dict[str, ObservedFieldDefinition] = {}
        nodes = [0]
        root = self._walk(value, "$", 0, fields, nodes)
        ordered = tuple(fields[path] for path in sorted(fields))
        document: dict[str, object] = {
            "fingerprint_version": FINGERPRINT_VERSION,
            "root_type": root,
            "fields": [
                {
                    "path": field.path,
                    "type": field.value_type,
                    "depth": field.depth,
                    "nullable": field.nullable,
                }
                for field in ordered
            ],
        }
        encoded = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
        if len(encoded) > MAX_DOCUMENT_BYTES:
            raise SchemaExtractionFailure("schema_document_too_large")
        return SchemaObservation(root, ordered, document, hashlib.sha256(encoded).hexdigest())

    def _walk(
        self,
        value: object,
        path: str,
        depth: int,
        fields: dict[str, ObservedFieldDefinition],
        nodes: list[int],
    ) -> StructuralType:
        nodes[0] += 1
        if nodes[0] > MAX_NODES:
            raise SchemaExtractionFailure("schema_node_limit_exceeded")
        if depth > MAX_DEPTH:
            raise SchemaExtractionFailure("schema_depth_exceeded")
        if len(path) > MAX_PATH_LENGTH:
            raise SchemaExtractionFailure("schema_path_too_long")
        kind: StructuralType = self._type(value)
        if path not in fields:
            if len(fields) >= MAX_FIELDS:
                raise SchemaExtractionFailure("schema_field_limit_exceeded")
            fields[path] = ObservedFieldDefinition(path, kind, depth, kind == "null")
        if isinstance(value, dict):
            for key in sorted(value):
                self._walk(
                    value[key], json_object_path_segment(path, key), depth + 1, fields, nodes
                )
        elif isinstance(value, list):
            element_types = {
                self._walk(item, f"{path}[]", depth + 1, fields, nodes) for item in value
            }
            if len(element_types) > 1:
                fields[f"{path}[]"] = ObservedFieldDefinition(
                    f"{path}[]", "mixed", depth + 1, "null" in element_types
                )
        return kind

    @staticmethod
    def _type(value: object) -> StructuralType:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "integer"
        if isinstance(value, float):
            return "number"
        if isinstance(value, str):
            return "string"
        if isinstance(value, list):
            return "array"
        return "object"
