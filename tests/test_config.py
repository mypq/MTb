import pytest
from pydantic import ValidationError

from backend.config import Settings


def make_settings(**overrides: object) -> Settings:
    return Settings(_env_file=None, **overrides)


def test_default_mode_is_analysis_only() -> None:
    settings = make_settings()

    assert settings.BOT_MODE == "ANALYSIS_ONLY"
    assert settings.TRADING_ENABLED is False
    assert settings.LIVE_TRADING_AUTHORIZED is False
    assert settings.EMERGENCY_STOP is True


def test_trading_is_blocked_by_default() -> None:
    settings = make_settings()

    assert settings.trading_must_be_blocked is True
    assert settings.live_trading_must_be_blocked is True


def test_emergency_stop_blocks_trading() -> None:
    settings = make_settings(
        BOT_MODE="DEMO_TRADING",
        TRADING_ENABLED=True,
        EMERGENCY_STOP=True,
    )

    assert settings.trading_must_be_blocked is True


def test_analysis_only_blocks_trading() -> None:
    settings = make_settings(
        BOT_MODE="ANALYSIS_ONLY",
        TRADING_ENABLED=True,
        EMERGENCY_STOP=False,
    )

    assert settings.trading_must_be_blocked is True


def test_live_trading_requires_explicit_authorization() -> None:
    settings = make_settings(
        BOT_MODE="LIVE_TRADING_AUTHORIZED",
        TRADING_ENABLED=True,
        LIVE_TRADING_AUTHORIZED=False,
        EMERGENCY_STOP=False,
    )

    assert settings.live_trading_must_be_blocked is True


@pytest.mark.parametrize(
    "value",
    [0, -1, 100.1],
)
def test_invalid_risk_percentage_is_rejected(value: float) -> None:
    with pytest.raises(ValidationError):
        make_settings(MAX_RISK_PER_TRADE_PERCENT=value)


@pytest.mark.parametrize(
    "value",
    [0, -1],
)
def test_invalid_max_open_positions_is_rejected(value: int) -> None:
    with pytest.raises(ValidationError):
        make_settings(MAX_OPEN_POSITIONS=value)


@pytest.mark.parametrize(
    "value",
    [0, -1, 65536],
)
def test_invalid_app_port_is_rejected(value: int) -> None:
    with pytest.raises(ValidationError):
        make_settings(APP_PORT=value)


def test_empty_database_url_is_rejected() -> None:
    with pytest.raises(ValidationError):
        make_settings(DATABASE_URL="   ")


def test_empty_mt5_login_is_normalized() -> None:
    settings = make_settings(MT5_LOGIN="")

    assert settings.MT5_LOGIN is None
