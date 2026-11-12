import pytest
from pydantic import SecretStr, ValidationError

from backend.config import Settings


def make_settings(**overrides: object) -> Settings:
    return Settings(_env_file=None, **overrides)


def test_mt5_is_disabled_by_default() -> None:
    settings = make_settings()

    assert settings.MT5_ENABLED is False


def test_mt5_credentials_are_optional_by_default() -> None:
    settings = make_settings()

    assert settings.MT5_LOGIN is None
    assert settings.MT5_PASSWORD.get_secret_value() == ""
    assert settings.MT5_SERVER is None


def test_empty_mt5_login_becomes_none() -> None:
    settings = make_settings(MT5_LOGIN="")

    assert settings.MT5_LOGIN is None


def test_mt5_login_accepts_integer() -> None:
    settings = make_settings(MT5_LOGIN=123456)

    assert settings.MT5_LOGIN == 123456


def test_mt5_password_uses_secret_string() -> None:
    settings = make_settings(MT5_PASSWORD="test-password")

    assert isinstance(settings.MT5_PASSWORD, SecretStr)
    assert settings.MT5_PASSWORD.get_secret_value() == "test-password"
    assert "test-password" not in str(settings.MT5_PASSWORD)


def test_mt5_server_is_preserved() -> None:
    settings = make_settings(MT5_SERVER="Demo-Server")

    assert settings.MT5_SERVER == "Demo-Server"


def test_mt5_terminal_path_is_preserved() -> None:
    path = r"C:\Program Files\MetaTrader 5\terminal64.exe"
    settings = make_settings(MT5_TERMINAL_PATH=path)

    assert settings.MT5_TERMINAL_PATH == path


@pytest.mark.parametrize(
    "timeout",
    [0, 999, -1000],
)
def test_invalid_mt5_timeout_is_rejected(timeout: int) -> None:
    with pytest.raises(ValidationError):
        make_settings(MT5_TIMEOUT_MS=timeout)


@pytest.mark.parametrize(
    "timeout",
    [1000, 5000, 10000, 30000],
)
def test_valid_mt5_timeout_is_accepted(timeout: int) -> None:
    settings = make_settings(MT5_TIMEOUT_MS=timeout)

    assert settings.MT5_TIMEOUT_MS == timeout


def test_mt5_can_be_enabled_explicitly() -> None:
    settings = make_settings(MT5_ENABLED=True)

    assert settings.MT5_ENABLED is True


def test_mt5_settings_do_not_enable_trading() -> None:
    settings = make_settings(MT5_ENABLED=True)

    assert settings.TRADING_ENABLED is False
    assert settings.LIVE_TRADING_AUTHORIZED is False
    assert settings.EMERGENCY_STOP is True
    assert settings.trading_must_be_blocked is True
    assert settings.live_trading_must_be_blocked is True
