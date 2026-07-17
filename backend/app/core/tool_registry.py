from __future__ import annotations

from typing import Any

from app.core.contracts import BaseTool


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, BaseTool] = {}

    def register(self, tool: BaseTool) -> None:
        if tool.tool_id in self._tools:
            raise ValueError(f"Tool already registered: {tool.tool_id}")

        self._tools[tool.tool_id] = tool

    def get(self, tool_id: str) -> BaseTool:
        if tool_id not in self._tools:
            raise KeyError(f"Tool not found: {tool_id}")

        return self._tools[tool_id]

    def list_all(self) -> list[BaseTool]:
        return list(self._tools.values())

    def list_by_capability(self, capability: str) -> list[BaseTool]:
        return [tool for tool in self._tools.values() if capability in tool.capabilities]

    async def execute(self, tool_id: str, params: dict[str, Any]) -> dict[str, Any]:
        tool = self.get(tool_id)
        return await tool.execute(params)
