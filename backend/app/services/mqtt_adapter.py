from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import Protocol, cast

from app.core.config import Settings
from app.services.stream_catalog import ObservationCommand

ObservationHandler = Callable[[ObservationCommand], Awaitable[None]]
SleepCallable = Callable[[float], Awaitable[None]]


class MqttMessageLike(Protocol):
    topic: object
    payload: bytes | bytearray | memoryview | str
    qos: object
    retain: bool


class ConnectedMqttClient(Protocol):
    messages: AsyncIterator[MqttMessageLike]

    async def subscribe(self, topic: str) -> object: ...


MqttClientContextFactory = Callable[[], AbstractAsyncContextManager[ConnectedMqttClient]]


class MqttAdapter:
    def __init__(
        self,
        settings: Settings,
        handler: ObservationHandler,
        client_context_factory: MqttClientContextFactory | None = None,
        sleep: SleepCallable = asyncio.sleep,
    ) -> None:
        self._settings = settings
        self._handler = handler
        self._client_context_factory = client_context_factory
        self._sleep = sleep
        self._status = "disabled"
        self._task: asyncio.Task[None] | None = None

    @property
    def status(self) -> str:
        return self._status

    async def start(self) -> None:
        if not self._settings.mqtt_enabled:
            self._status = "disabled"
            return
        if self._task is None or self._task.done():
            self._status = "starting"
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None
        self._status = "stopped" if self._settings.mqtt_enabled else "disabled"

    def _tls_context(self) -> ssl.SSLContext | None:
        if not self._settings.mqtt_tls_enabled:
            return None
        context = ssl.create_default_context()
        if not self._settings.mqtt_tls_verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        return context

    def _client_context(self) -> AbstractAsyncContextManager[ConnectedMqttClient]:
        if self._client_context_factory is not None:
            return self._client_context_factory()
        from aiomqtt import Client

        hostname = self._settings.mqtt_host
        if hostname is None:
            raise RuntimeError("MQTT configuration is invalid")
        return cast(
            AbstractAsyncContextManager[ConnectedMqttClient],
            Client(
                hostname=hostname,
                port=self._settings.mqtt_port,
                identifier=self._settings.mqtt_client_id,
                username=self._settings.mqtt_username,
                password=self._settings.mqtt_password,
                tls_context=self._tls_context(),
            ),
        )

    async def _run(self) -> None:
        attempts = 0
        while attempts < 2:
            try:
                async with self._client_context() as client:
                    for topic in self._settings.mqtt_topic_allowlist:
                        await client.subscribe(topic)
                    self._status = "running"
                    async for message in client.messages:
                        payload_value = message.payload
                        if isinstance(payload_value, bytes):
                            payload = payload_value
                        elif isinstance(payload_value, bytearray | memoryview):
                            payload = bytes(payload_value)
                        else:
                            payload = payload_value.encode()
                        await self._handler(
                            ObservationCommand(
                                source_id=self._settings.mqtt_source_id or "unknown",
                                topic=str(message.topic),
                                payload=payload,
                                broker_metadata={
                                    "qos": int(str(message.qos)),
                                    "retain": message.retain,
                                },
                            )
                        )
                    raise ConnectionError("MQTT message stream ended")
            except asyncio.CancelledError:
                raise
            except Exception:
                attempts += 1
                if attempts == 2:
                    self._status = "failed"
                    return
                self._status = "reconnecting"
                await self._sleep(0.1)
