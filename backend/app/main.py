from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.health import router as health_router
from app.api.routes.modules import router as modules_router
from app.api.routes.tools import router as tools_router
from app.core.event_bus import EventBus
from app.core.module_registry import ModuleRegistry
from app.core.tool_registry import ToolRegistry
from app.modules.system_status.module import SystemStatusModule
from app.tools.system_tools import PingTool


def create_app() -> FastAPI:
    app = FastAPI(
        title="Smart Telemetry Governance",
        version="0.1.0",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.state.event_bus = EventBus()
    app.state.module_registry = ModuleRegistry()
    app.state.tool_registry = ToolRegistry()

    app.state.module_registry.register(SystemStatusModule())
    app.state.tool_registry.register(PingTool())

    app.include_router(health_router)
    app.include_router(modules_router)
    app.include_router(tools_router)

    @app.on_event("startup")
    async def startup() -> None:
        await app.state.module_registry.start_all()

    @app.on_event("shutdown")
    async def shutdown() -> None:
        await app.state.module_registry.stop_all()

    return app


app = create_app()