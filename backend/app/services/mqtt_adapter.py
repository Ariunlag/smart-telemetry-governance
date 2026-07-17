from __future__ import annotations

import asyncio
import ssl
from collections.abc import Awaitable, Callable

from app.core.config import Settings
from app.services.stream_catalog import ObservationCommand

ObservationHandler = Callable[[ObservationCommand], Awaitable[None]]


class MqttAdapter:
    """Lifecycle-owned MQTT boundary; only starts when explicitly enabled."""

    def __init__(self, settings: Settings, handler: ObservationHandler) -> None:
        self._settings = settings
        self._handler = handler
        self._status = "disabled"
        self._task: asyncio.Task[None] | None = None

    @property
    def status(self) -> str:
        return self._status

    async def start(self) -> None:
        if not self._settings.mqtt_enabled:
            self._status = "disabled"
            return
        self._status = "starting"
        self._task = asyncio.create_task(self._run(), name="r1-mqtt-ingestion")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._status = "stopped"

    async def _run(self) -> None:
        from aiomqtt import Client

        hostname = self._settings.mqtt_host
        if hostname is None:
            raise RuntimeError("MQTT_HOST is required when MQTT_ENABLED is true")

        tls_context: ssl.SSLContext | None = None
        if self._settings.mqtt_tls_enabled:
            tls_context = ssl.create_default_context()
            if not self._settings.mqtt_tls_verify:
                tls_context.check_hostname = False
                tls_context.verify_mode = ssl.CERT_NONE
        try:
            async with Client(
                hostname=hostname,
                port=self._settings.mqtt_port,
                identifier=self._settings.mqtt_client_id,
                username=self._settings.mqtt_username,
                password=self._settings.mqtt_password,
                tls_context=tls_context,
            ) as client:
                for topic in self._settings.mqtt_topic_allowlist:
                    await client.subscribe(topic)
                self._status = "running"
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
        except asyncio.CancelledError:
            raise
        except Exception:
            self._status = "failed"
