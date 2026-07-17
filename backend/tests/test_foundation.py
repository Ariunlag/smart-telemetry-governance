import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.core.config import Settings, get_settings
from app.main import create_app


def build_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATABASE_REQUIRED", "false")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    get_settings.cache_clear()
    return TestClient(create_app())


def test_health_is_live_and_includes_correlation_id(monkeypatch: pytest.MonkeyPatch) -> None:
    with build_client(monkeypatch) as client:
        response = client.get("/health", headers={"X-Correlation-ID": "test-request"})

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.headers["X-Correlation-ID"] == "test-request"


def test_readiness_is_ready_without_an_optional_database(monkeypatch: pytest.MonkeyPatch) -> None:
    with build_client(monkeypatch) as client:
        response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "database": "not_configured"}


def test_readiness_fails_when_a_required_database_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATABASE_REQUIRED", "true")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///C:/Windows/System32/blocked/foundation.db")
    get_settings.cache_clear()
    app = create_app()
    app.state.database = None

    with TestClient(app) as client:
        response = client.get("/ready")

    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"


def test_settings_rejects_an_invalid_port() -> None:
    with pytest.raises(ValidationError):
        Settings(app_port=70000)
