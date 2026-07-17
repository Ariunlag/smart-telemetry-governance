from typing import Any

from fastapi import APIRouter, Request

router = APIRouter(prefix="/tools", tags=["tools"])


@router.get("")
async def list_tools(request: Request) -> list[dict[str, Any]]:
    registry = request.app.state.tool_registry

    return [
        {
            "tool_id": tool.tool_id,
            "description": tool.description,
            "capabilities": tool.capabilities,
        }
        for tool in registry.list_all()
    ]
