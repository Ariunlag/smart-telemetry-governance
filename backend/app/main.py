from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.delivery import router as delivery_router
from app.api.routes.health import router as health_router
from app.api.routes.modules import router as modules_router
from app.api.routes.streams import router as streams_router
from app.api.routes.tools import router as tools_router
from app.core.config import get_settings
from app.core.event_bus import EventBus
from app.core.logging import CorrelationIdMiddleware, configure_logging
from app.core.module_registry import ModuleRegistry
from app.core.tool_registry import ToolRegistry
from app.db.session import Database
from app.modules.system_status.module import SystemStatusModule
from app.services.influx_observation_writer import InfluxObservationWriter
from app.services.mqtt_adapter import MqttAdapter
from app.services.observation_delivery_worker import ObservationDeliveryWorker
from app.services.stream_catalog import ObservationCommand, StreamCatalogService
from app.tools.system_tools import PingTool


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    app.state.settings = settings
    configure_logging(settings.log_level)
    app.state.database = Database(settings)
    app.state.event_bus = EventBus()
    app.state.module_registry = ModuleRegistry()
    app.state.tool_registry = ToolRegistry()
    app.state.stream_catalog = StreamCatalogService(settings)
    app.state.influx_writer = InfluxObservationWriter(settings)
    app.state.delivery_worker = ObservationDeliveryWorker(
        settings, app.state.database, app.state.influx_writer
    )

    async def record_observation(command: ObservationCommand) -> None:
        async with app.state.database.transaction() as session:
            await app.state.stream_catalog.record(session, command)

    app.state.mqtt_adapter = MqttAdapter(settings, record_observation)
    try:
        await app.state.database.initialize()
        app.state.module_registry.register(SystemStatusModule())
        app.state.tool_registry.register(PingTool())
        await app.state.module_registry.start_all()
        await app.state.influx_writer.initialize()
        await app.state.delivery_worker.start()
        await app.state.mqtt_adapter.start()
    except BaseException:
        try:
            await app.state.mqtt_adapter.stop()
        except Exception:
            pass
        try:
            await app.state.delivery_worker.stop()
            await app.state.influx_writer.close()
        except Exception:
            pass
        try:
            await app.state.module_registry.stop_all()
        except Exception:
            pass
        try:
            await app.state.database.dispose()
        except Exception:
            pass
        raise
    try:
        yield
    finally:
        await app.state.mqtt_adapter.stop()
        await app.state.delivery_worker.stop()
        await app.state.influx_writer.close()
        await app.state.module_registry.stop_all()
        await app.state.database.dispose()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name, version="0.1.0", lifespan=lifespan)
    app.add_middleware(CorrelationIdMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in settings.cors_origins],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(health_router)
    app.include_router(delivery_router)
    app.include_router(modules_router)
    app.include_router(streams_router)
    app.include_router(tools_router)
    return app


app = create_app()
