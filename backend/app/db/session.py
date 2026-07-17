from __future__ import annotations

import asyncio

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.core.config import Settings


class Database:
    """Owns a lazily-created SQLAlchemy engine for the future relational store."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._engine: Engine | None = None

    @property
    def configured(self) -> bool:
        return self._settings.database_url is not None

    def _get_engine(self) -> Engine:
        if self._settings.database_url is None:
            raise RuntimeError("DATABASE_URL is not configured")
        if self._engine is None:
            connect_args: dict[str, int] = {}
            if self._settings.database_url.startswith("postgresql"):
                connect_args["connect_timeout"] = 5
            elif self._settings.database_url.startswith("sqlite"):
                connect_args["timeout"] = 5
            self._engine = create_engine(
                self._settings.database_url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=5,
                pool_timeout=5,
                connect_args=connect_args,
            )
        return self._engine

    async def check_connection(self) -> bool:
        if not self.configured:
            return not self._settings.database_required

        def _check() -> bool:
            with self._get_engine().connect() as connection:
                connection.execute(text("SELECT 1"))
            return True

        try:
            return await asyncio.wait_for(asyncio.to_thread(_check), timeout=5)
        except Exception:
            return False

    async def dispose(self) -> None:
        if self._engine is not None:
            await asyncio.to_thread(self._engine.dispose)
            self._engine = None
