from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from types import TracebackType

import pytest

from app.core.config import Settings
from app.services.mqtt_adapter import ConnectedMqttClient, MqttAdapter, MqttMessageLike


async def empty_messages() -> AsyncIterator[MqttMessageLike]:
    if False:
        yield FakeMessage()


class FakeMessage:
    topic: object = "site/one"
    payload: bytes = b"payload"
    qos: object = 0
    retain = False


class FakeClient:
    def __init__(self) -> None:
        self.messages = empty_messages()
        self.subscriptions: list[str] = []

    async def subscribe(self, topic: str) -> object:
        self.subscriptions.append(topic)
        return None


class FakeClientContext:
    def __init__(self, client: ConnectedMqttClient) -> None:
        self.client = client

    async def __aenter__(self) -> ConnectedMqttClient:
        return self.client

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return None


async def handler(_: object) -> None:
    return None


def enabled_settings() -> Settings:
    return Settings(
        mqtt_enabled=True,
        database_url="sqlite+aiosqlite:///test.db",
        mqtt_host="broker",
        mqtt_source_id="source",
        mqtt_topic_allowlist=["site/#"],
    )


@pytest.mark.asyncio
async def test_disabled_adapter_does_not_create_client() -> None:
    adapter = MqttAdapter(Settings(), handler)
    await adapter.start()
    assert adapter.status == "disabled"


@pytest.mark.asyncio
async def test_successful_connection_subscribes_to_allowlist() -> None:
    client = FakeClient()

    def client_context_factory() -> AbstractAsyncContextManager[ConnectedMqttClient]:
        return FakeClientContext(client)

    adapter = MqttAdapter(enabled_settings(), handler, client_context_factory)
    await adapter.start()
    await asyncio.sleep(0)
    assert adapter.status == "running"
    assert client.subscriptions == ["site/#"]
    await adapter.stop()


@pytest.mark.asyncio
async def test_stop_is_idempotent() -> None:
    adapter = MqttAdapter(enabled_settings(), handler, lambda: FakeClientContext(FakeClient()))
    await adapter.start()
    await adapter.stop()
    await adapter.stop()
    assert adapter.status == "stopped"
