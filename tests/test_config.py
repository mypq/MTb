
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
    ("field", "value"),
    [
        ("MAX_RISK_PER_TRADE_PERCENT", 0),
        ("MAX_RISK_PER_TRADE_PERCENT", -1),
        ("MAX_RISK_PER_TRADE_PERCENT", 1.01),
        ("MAX_RISK_PER_TRADE_PERCENT", 100),
        ("MAX_DAILY_LOSS_PERCENT", 0),
        ("MAX_DAILY_LOSS_PERCENT", -1),
        ("MAX_DAILY_LOSS_PERCENT", 2.01),
        ("MAX_DAILY_LOSS_PERCENT", 100),
    ],
)
def test_invalid_risk_limits_are_rejected(
    field: str,
    value: float,
) -> None:
    with pytest.raises(ValidationError):
        make_settings(**{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("MAX_RISK_PER_TRADE_PERCENT", 0.01),
        ("MAX_RISK_PER_TRADE_PERCENT", 0.5),
        ("MAX_RISK_PER_TRADE_PERCENT", 1.0),
        ("MAX_DAILY_LOSS_PERCENT", 0.01),
        ("MAX_DAILY_LOSS_PERCENT", 1.0),
        ("MAX_DAILY_LOSS_PERCENT", 2.0),
    ],
)
def test_valid_risk_limits_are_accepted(
    field: str,
    value: float,
) -> None:
    settings = make_settings(**{field: value})

    assert getattr(settings, field) == value


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
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("APP_PORT", "invalid"),
        ("MT5_TIMEOUT_MS", 999),
        ("MT5_TIMEOUT_MS", 0),
        ("MAX_OPEN_POSITIONS", "invalid"),
        ("MAX_RISK_PER_TRADE_PERCENT", "invalid"),
        ("MAX_DAILY_LOSS_PERCENT", "invalid"),
    ],
)
def test_invalid_configuration_types_are_rejected(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValidationError):
        make_settings(**{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("MT5_TIMEOUT_MS", 1000),
        ("MT5_TIMEOUT_MS", 10_000),
        ("APP_PORT", 8000),
        ("MAX_OPEN_POSITIONS", 1),
    ],
)
def test_valid_configuration_boundaries_are_accepted(
    field: str,
    value: object,
) -> None:
    settings = make_settings(**{field: value})

    assert getattr(settings, field) == value


def test_default_host_is_localhost_only() -> None:
    settings = make_settings()

    assert settings.APP_HOST == "127.0.0.1"


def test_default_timeframe_is_five_minutes() -> None:
    settings = make_settings()

    assert settings.DEFAULT_TIMEFRAME == "M5"


def test_empty_mt5_credentials_are_safe_by_default() -> None:
    settings = make_settings()

    assert settings.MT5_ENABLED is False
    assert settings.MT5_PASSWORD.get_secret_value() == ""
