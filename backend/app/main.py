from fastapi import FastAPI

from app.api.routes.health import router as health_router
from app.api.routes.modules import router as modules_router
from app.api.routes.tools import router as tools_router
from app.core.event_bus import EventBus
from app.core.module_registry import ModuleRegistry
from app.core.tool_registry import ToolRegistry


def create_app() -> FastAPI:
    app = FastAPI(
        title="Smart Telemetry Governance",
        version="0.1.0",
    )

    app.state.event_bus = EventBus()
    app.state.module_registry = ModuleRegistry()
    app.state.tool_registry = ToolRegistry()

    app.include_router(health_router)
    app.include_router(modules_router)
    app.include_router(tools_router)

    return app


app = create_app()