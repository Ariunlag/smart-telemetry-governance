from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from types import ModuleType, TracebackType

import pytest

from app.core.config import Settings
from app.services.mqtt_adapter import ConnectedMqttClient, MqttAdapter, MqttMessageLike


async def empty_messages() -> AsyncIterator[MqttMessageLike]:
    if False:
        yield FakeMessage()


class FakeMessage:
    def __init__(self, payload: bytes | bytearray = b"payload") -> None:
        self.topic: object = "site/one"
        self.payload: bytes | bytearray | memoryview | str = payload
        self.qos: object = 1
        self.retain = True


class FakeClient:
    def __init__(self, values: list[MqttMessageLike] | None = None) -> None:
        self.messages = empty_messages() if values is None else message_iterator(values)
        self.subscriptions: list[str] = []

    async def subscribe(self, topic: str) -> object:
        self.subscriptions.append(topic)
        return None


async def message_iterator(values: list[MqttMessageLike]) -> AsyncIterator[MqttMessageLike]:
    for value in values:
        yield value


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


def test_tls_context_modes() -> None:
    verified = MqttAdapter(enabled_settings(), handler)._tls_context()
    unverified = MqttAdapter(
        enabled_settings().model_copy(update={"mqtt_tls_verify": False}), handler
    )._tls_context()
    disabled = MqttAdapter(
        enabled_settings().model_copy(update={"mqtt_tls_enabled": False}), handler
    )._tls_context()
    assert verified is not None and verified.check_hostname
    assert unverified is not None and not unverified.check_hostname
    assert disabled is None


@pytest.mark.asyncio
async def test_message_is_transferred() -> None:
    received: list[object] = []

    async def receive(command: object) -> None:
        received.append(command)

    client = FakeClient([FakeMessage(bytearray(b"bytes"))])
    adapter = MqttAdapter(enabled_settings(), receive, lambda: FakeClientContext(client))
    await adapter.start()
    await asyncio.sleep(0)
    command = received[0]
    assert getattr(command, "source_id") == "source"
    assert getattr(command, "payload") == b"bytes"
    await adapter.stop()


@pytest.mark.asyncio
async def test_reconnect_uses_injected_sleep() -> None:
    calls = 0
    delays: list[float] = []

    def factory() -> AbstractAsyncContextManager[ConnectedMqttClient]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("connection failed")
        return FakeClientContext(FakeClient())

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    adapter = MqttAdapter(enabled_settings(), handler, factory, fake_sleep)
    await adapter.start()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert calls == 2 and delays == [0.1] and adapter.status == "running"
    await adapter.stop()


def test_production_client_uses_configured_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    received: dict[str, object] = {}

    class ProductionClient(FakeClientContext):
        def __init__(self, **kwargs: object) -> None:
            received.update(kwargs)
            super().__init__(FakeClient())

    module = ModuleType("aiomqtt")
    setattr(module, "Client", ProductionClient)
    monkeypatch.setitem(sys.modules, "aiomqtt", module)
    configured = enabled_settings().model_copy(
        update={
            "mqtt_port": 1884,
            "mqtt_client_id": "test-client",
            "mqtt_username": "user",
            "mqtt_password": "sentinel",
        }
    )
    MqttAdapter(configured, handler)._client_context()
    assert received["hostname"] == "broker"
    assert received["port"] == 1884
    assert received["identifier"] == "test-client"
    assert received["username"] == "user"
    assert received["password"] == "sentinel"
    assert received["tls_context"] is not None


@pytest.mark.asyncio
async def test_stop_cancels_reconnect_sleep() -> None:
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    def failing_factory() -> AbstractAsyncContextManager[ConnectedMqttClient]:
        raise RuntimeError("connection failed")

    async def blocked_sleep(_: float) -> None:
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    adapter = MqttAdapter(enabled_settings(), handler, failing_factory, blocked_sleep)
    await adapter.start()
    await entered.wait()
    assert adapter.status == "reconnecting"
    await adapter.stop()
    assert cancelled.is_set() and adapter.status == "stopped"


@pytest.mark.asyncio
async def test_failed_task_cleanup_is_safe() -> None:
    adapter = MqttAdapter(enabled_settings(), handler, lambda: FakeClientContext(FakeClient()))

    async def fail() -> None:
        raise RuntimeError("task failure")

    adapter._task = asyncio.create_task(fail())
    await asyncio.sleep(0)
    await adapter.stop()
    await adapter.stop()
    assert adapter.status == "stopped"


@pytest.mark.asyncio
async def test_connection_failure_does_not_expose_password() -> None:
    sentinel = "test-only-password"

    def failing_factory() -> AbstractAsyncContextManager[ConnectedMqttClient]:
        raise RuntimeError("connection failed")

    adapter = MqttAdapter(
        enabled_settings().model_copy(update={"mqtt_password": sentinel}),
        handler,
        failing_factory,
        lambda _: asyncio.sleep(0),
    )
    await adapter.start()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert sentinel not in adapter.status
    await adapter.stop()
