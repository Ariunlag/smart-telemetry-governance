from __future__ import annotations

import hashlib


def normalize_identifier(value: str) -> str:
    normalized = value.strip().lower()
    if not normalized:
        raise ValueError("identifier must not be empty")
    return normalized


def normalize_topic(topic: str) -> str:
    normalized = "/".join(part.strip() for part in topic.strip().split("/") if part.strip())
    if not normalized:
        raise ValueError("topic must not be empty")
    return normalized


def stream_key(source_id: str, topic: str, tenant: str | None = None) -> str:
    parts = [
        normalize_identifier(source_id),
        normalize_topic(topic),
        normalize_identifier(tenant) if tenant else "",
    ]
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()
