from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

import backend.main as main
import backend.security as security
from backend.config import Settings


class ForbiddenMarketDataService:
    def get_snapshot(self, **kwargs):
        raise AssertionError(
            "Unauthorized request reached the market-data service."
        )


class ForbiddenMarketDataRepository:
    def save_snapshot(self, snapshot):
        raise AssertionError(
            "Unauthorized request reached the repository."
        )


@pytest.fixture
def api_client(monkeypatch):
    application_settings = SimpleNamespace(
        APP_NAME="GOLD_BOT_TEST",
        DEFAULT_TIMEFRAME="M5",
        MT5_SYMBOL="XAUUSD",
    )

    security_settings = Settings(
        _env_file=None,
        APP_ENV="testing",
        API_AUTH_ENABLED=True,
        API_ACCESS_TOKEN="integration-test-secret",
    )

    monkeypatch.setattr(
        main,
        "get_settings",
        lambda: application_settings,
    )

    monkeypatch.setattr(
        security,
        "get_settings",
        lambda: security_settings,
    )

    application = main.create_app()

    application.state.market_data_service = (
        ForbiddenMarketDataService()
    )
    application.state.market_data_repository = (
        ForbiddenMarketDataRepository()
    )

    return TestClient(application)


def test_market_data_endpoint_rejects_missing_token(api_client):
    response = api_client.get(
        "/api/v1/market-data/candles",
        params={"symbol": "XAUUSD"},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Missing bearer token."
    assert response.headers["www-authenticate"] == "Bearer"


def test_market_data_endpoint_rejects_invalid_token(api_client):
    response = api_client.get(
        "/api/v1/market-data/candles",
        params={"symbol": "XAUUSD"},
        headers={
            "Authorization": "Bearer wrong-token",
        },
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid API access token."
    assert response.headers["www-authenticate"] == "Bearer"


def test_market_data_endpoint_rejects_unconfigured_token(
    monkeypatch,
):
    application_settings = SimpleNamespace(
        APP_NAME="GOLD_BOT_TEST",
        DEFAULT_TIMEFRAME="M5",
        MT5_SYMBOL="XAUUSD",
    )

    security_settings = Settings(
        _env_file=None,
        APP_ENV="testing",
        API_AUTH_ENABLED=True,
        API_ACCESS_TOKEN=SecretStr(""),
    )

    monkeypatch.setattr(
        main,
        "get_settings",
        lambda: application_settings,
    )

    monkeypatch.setattr(
        security,
        "get_settings",
        lambda: security_settings,
    )

    application = main.create_app()
    client = TestClient(application)

    response = client.get(
        "/api/v1/market-data/candles",
        params={"symbol": "XAUUSD"},
        headers={
            "Authorization": "Bearer any-token",
        },
    )

    assert response.status_code == 503
    assert (
        response.json()["detail"]
        == "API access token is not configured."
    )


def test_health_endpoint_is_public_and_exposes_only_status(
    api_client,
):
    response = api_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
