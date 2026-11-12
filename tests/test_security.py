import pytest
from fastapi import HTTPException

from backend.config import Settings
from backend.security import verify_api_token


def test_missing_token_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings(
        _env_file=None,
        API_AUTH_ENABLED=True,
        API_ACCESS_TOKEN="test-secret-token",
    )
    monkeypatch.setattr("backend.security.get_settings", lambda: settings)

    with pytest.raises(HTTPException) as exc:
        verify_api_token(None)

    assert exc.value.status_code == 401


def test_invalid_token_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings(
        _env_file=None,
        API_AUTH_ENABLED=True,
        API_ACCESS_TOKEN="test-secret-token",
    )
    monkeypatch.setattr("backend.security.get_settings", lambda: settings)

    with pytest.raises(HTTPException) as exc:
        verify_api_token("Bearer wrong-token")

    assert exc.value.status_code == 401


def test_valid_token_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings(
        _env_file=None,
        API_AUTH_ENABLED=True,
        API_ACCESS_TOKEN="test-secret-token",
    )
    monkeypatch.setattr("backend.security.get_settings", lambda: settings)

    assert verify_api_token("Bearer test-secret-token") is None


def test_missing_configured_token_returns_service_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        _env_file=None,
        API_AUTH_ENABLED=True,
        API_ACCESS_TOKEN="",
    )
    monkeypatch.setattr("backend.security.get_settings", lambda: settings)

    with pytest.raises(HTTPException) as exc:
        verify_api_token("Bearer any-token")

    assert exc.value.status_code == 503


def test_authentication_cannot_be_disabled_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        _env_file=None,
        APP_ENV="production",
        API_AUTH_ENABLED=False,
    )
    monkeypatch.setattr("backend.security.get_settings", lambda: settings)

    with pytest.raises(HTTPException) as exc:
        verify_api_token(None)

    assert exc.value.status_code == 503
