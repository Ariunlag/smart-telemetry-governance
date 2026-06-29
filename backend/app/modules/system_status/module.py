from app.core.contracts import BaseModule


class SystemStatusModule(BaseModule):
    module_id = "system_status"
    version = "0.1.0"

    def __init__(self) -> None:
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    def health_check(self) -> bool:
        return self.started