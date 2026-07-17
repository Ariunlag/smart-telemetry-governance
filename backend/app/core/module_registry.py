from __future__ import annotations

from app.core.contracts import BaseModule


class ModuleRegistry:
    def __init__(self) -> None:
        self._modules: dict[str, BaseModule] = {}
        self._started_module_ids: list[str] = []

    def register(self, module: BaseModule) -> None:
        if module.module_id in self._modules:
            raise ValueError(f"Module already registered: {module.module_id}")

        self._modules[module.module_id] = module

    def unregister(self, module_id: str) -> None:
        self._modules.pop(module_id, None)

    def get(self, module_id: str) -> BaseModule:
        if module_id not in self._modules:
            raise KeyError(f"Module not found: {module_id}")

        return self._modules[module_id]

    def list_all(self) -> list[BaseModule]:
        return list(self._modules.values())

    def list_healthy(self) -> list[BaseModule]:
        return [module for module in self._modules.values() if module.health_check()]

    async def start_all(self) -> None:
        for module in self._modules.values():
            await module.start()
            self._started_module_ids.append(module.module_id)

    async def stop_all(self) -> None:
        while self._started_module_ids:
            module_id = self._started_module_ids.pop()
            await self._modules[module_id].stop()
