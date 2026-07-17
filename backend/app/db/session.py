from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import Settings

metadata = MetaData()


class DatabaseNotInitializedError(RuntimeError):
    """Raised when database access is attempted before lifecycle initialization."""


def get_database_url(settings: Settings) -> str:
    """Return the configured application database URL without exposing it in errors."""
    if settings.database_url is None:
        raise ValueError("DATABASE_URL must be configured before database access")
    return settings.database_url


def to_sync_migration_url(database_url: str) -> str:
    """Convert an async-only SQLite URL for Alembic's synchronous migration engine."""
    return database_url.replace("sqlite+aiosqlite://", "sqlite://", 1)


class Database:
    """Owns lifecycle-managed async SQLAlchemy resources without domain tables."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker[AsyncSession] | None = None

    @property
    def configured(self) -> bool:
        return self._settings.database_url is not None

    async def initialize(self) -> None:
        """Create the lazy async engine and verify required dependencies."""
        if not self.configured:
            if self._settings.database_required:
                raise ValueError("DATABASE_REQUIRED is true but DATABASE_URL is not configured")
            return
        if self._engine is not None:
            return

        self._engine = create_async_engine(
            get_database_url(self._settings),
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=5,
            pool_timeout=5,
        )
        self._session_factory = async_sessionmaker(self._engine, expire_on_commit=False)

    def get_engine(self) -> AsyncEngine:
        if self._engine is None:
            raise DatabaseNotInitializedError("Database engine has not been initialized")
        return self._engine

    def get_session_factory(self) -> async_sessionmaker[AsyncSession]:
        if self._session_factory is None:
            raise DatabaseNotInitializedError("Database session factory has not been initialized")
        return self._session_factory

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Yield a session that always closes and rolls back failed work."""
        session = self.get_session_factory()()
        try:
            yield session
        except BaseException:
            await session.rollback()
            raise
        finally:
            await session.close()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        """Yield an explicit commit-or-rollback transaction boundary."""
        async with self.session() as session:
            async with session.begin():
                yield session

    async def check_connection(self) -> bool:
        if not self.configured:
            return not self._settings.database_required
        try:
            async with self.get_engine().connect() as connection:
                await connection.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    async def dispose(self) -> None:
        """Dispose the engine once; repeated disposal is safe."""
        if self._engine is not None:
            await self._engine.dispose()
        self._engine = None
        self._session_factory = None
