from __future__ import annotations

from typing import cast
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.source_subscription_repository import SourceSubscriptionRepository


@pytest.mark.asyncio
async def test_repository_rejects_unsupported_mqtt_values_before_persistence() -> None:
    repository = SourceSubscriptionRepository()
    session = cast(AsyncSession, object())
    with pytest.raises(ValueError, match="Only MQTT"):
        await repository.create_source(
            session, uuid4(), uuid4(), "source", "Source", None, protocol="http"
        )
    with pytest.raises(ValueError, match="QoS"):
        await repository.create_subscription(session, uuid4(), uuid4(), uuid4(), "site/topic", 3)
    with pytest.raises(ValueError, match="unrestricted"):
        await repository.create_subscription(session, uuid4(), uuid4(), uuid4(), "#", 0)
