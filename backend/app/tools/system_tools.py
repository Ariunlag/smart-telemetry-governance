from app.core.contracts import BaseTool


class PingTool(BaseTool):
    tool_id = "ping"
    description = "Simple test tool that verifies the tool registry works"
    capabilities = ["test", "system"]

    input_schema = {
        "type": "object",
        "properties": {},
    }

    output_schema = {
        "type": "object",
        "properties": {
            "message": {"type": "string"},
        },
    }

    async def execute(self, params: dict) -> dict:
        return {"message": "pong"}