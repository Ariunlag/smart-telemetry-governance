from fastapi import APIRouter, Request

router = APIRouter(prefix="/modules", tags=["modules"])


@router.get("")
async def list_modules(request: Request) -> list[dict]:
    registry = request.app.state.module_registry

    return [
        {
            "module_id": module.module_id,
            "version": module.version,
            "healthy": module.health_check(),
        }
        for module in registry.list_all()
    ]