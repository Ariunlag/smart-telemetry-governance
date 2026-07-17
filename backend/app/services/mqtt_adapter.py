from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from app.core.config import Settings
from app.services.stream_catalog import ObservationCommand

ObservationHandler = Callable[[ObservationCommand], Awaitable[None]]


class MqttAdapter:
    """Lifecycle-owned MQTT boundary; only starts when explicitly enabled."""

    def __init__(self, settings: Settings, handler: ObservationHandler) -> None:
        self._settings = settings
        self._handler = handler
        self._running = False
        self._task: asyncio.Task[None] | None = None

    @property
    def status(self) -> str:
        return "running" if self._running else "disabled"

    async def start(self) -> None:
        if not self._settings.mqtt_enabled:
            return
        self._task = asyncio.create_task(self._run(), name="r1-mqtt-ingestion")
        self._running = True

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._running = False

    async def _run(self) -> None:
        from aiomqtt import Client

        hostname = self._settings.mqtt_host
        if hostname is None:
            raise RuntimeError("MQTT_HOST is required when MQTT_ENABLED is true")

        async with Client(
            hostname=hostname,
            port=self._settings.mqtt_port,
            identifier=self._settings.mqtt_client_id,
            username=self._settings.mqtt_username,
            password=self._settings.mqtt_password,
        ) as client:
            for topic in self._settings.mqtt_topic_allowlist:
                await client.subscribe(topic)
            async for message in client.messages:
                await self._handler(
                    ObservationCommand(
                        source_id=self._settings.mqtt_source_id or "unknown",
                        topic=str(message.topic),
                        payload=(
                            message.payload
                            if isinstance(message.payload, bytes)
                            else str(message.payload).encode()
                        ),
                        broker_metadata={"qos": int(message.qos), "retain": message.retain},
                    )
                )
