
from functools import lru_cache
from typing import Literal

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

BotMode = Literal[
    "OFFLINE",
    "ANALYSIS_ONLY",
    "DEMO_TRADING",
    "LIVE_TRADING_LOCKED",
    "LIVE_TRADING_AUTHORIZED",
    "EMERGENCY_STOP",
]

Timeframe = Literal[
    "M1",
    "M5",
    "M15",
    "M30",
    "H1",
    "H4",
    "D1",
]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        env_ignore_empty=True,
        validate_default=True,
    )

    # Application
    APP_NAME: str = "GOLD_BOT"
    APP_ENV: Literal["development", "testing", "production"] = "development"
    APP_HOST: str = "127.0.0.1"
    APP_PORT: int = 8000
    APP_LOG_LEVEL: Literal[
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL",
    ] = "INFO"

    # Trading safety
    BOT_MODE: BotMode = "ANALYSIS_ONLY"
    TRADING_ENABLED: bool = False
    LIVE_TRADING_AUTHORIZED: bool = False
    EMERGENCY_STOP: bool = True

    # MetaTrader 5
    MT5_ENABLED: bool = False
    MT5_TERMINAL_PATH: str | None = None
    MT5_LOGIN: int | None = None
    MT5_PASSWORD: SecretStr = SecretStr("")
    MT5_SERVER: str | None = None
    MT5_TIMEOUT_MS: int = 10_000

    # Market
    MT5_SYMBOL: str = ""
    DEFAULT_TIMEFRAME: Timeframe = "M5"

    # Risk management
    MAX_RISK_PER_TRADE_PERCENT: float = 0.5
    MAX_DAILY_LOSS_PERCENT: float = 2.0
    MAX_OPEN_POSITIONS: int = 1

    # API security
    API_AUTH_ENABLED: bool = True
    API_ACCESS_TOKEN: SecretStr = SecretStr("")

    # Database
    DATABASE_URL: str = "sqlite:///./data/gold_bot.db"

    # Timezone
    APP_TIMEZONE: str = "Asia/Baghdad"

    @field_validator("APP_NAME", "APP_HOST", "APP_TIMEZONE")
    @classmethod
    def reject_blank_required_strings(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("This setting cannot be empty.")
        return value.strip()

    @field_validator("APP_PORT")
    @classmethod
    def validate_app_port(cls, value: int) -> int:
        if not 1 <= value <= 65535:
            raise ValueError("APP_PORT must be between 1 and 65535.")
        return value

    @field_validator("MT5_TIMEOUT_MS")
    @classmethod
    def validate_mt5_timeout(cls, value: int) -> int:
        if value < 1000:
            raise ValueError("MT5_TIMEOUT_MS must be at least 1000.")
        return value

    @field_validator("MAX_RISK_PER_TRADE_PERCENT")
    @classmethod
    def validate_risk_per_trade(cls, value: float) -> float:
        if not 0 < value <= 1.0:
            raise ValueError(
                "MAX_RISK_PER_TRADE_PERCENT must be greater than 0 "
                "and at most 1.0."
            )
        return value

    @field_validator("MAX_DAILY_LOSS_PERCENT")
    @classmethod
    def validate_daily_loss(cls, value: float) -> float:
        if not 0 < value <= 2.0:
            raise ValueError(
                "MAX_DAILY_LOSS_PERCENT must be greater than 0 "
                "and at most 2.0."
            )
        return value

    @field_validator("MAX_OPEN_POSITIONS")
    @classmethod
    def validate_max_positions(cls, value: int) -> int:
        if value < 1:
            raise ValueError("MAX_OPEN_POSITIONS must be at least 1.")
        return value

    @field_validator("DATABASE_URL")
    @classmethod
    def validate_database_url(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("DATABASE_URL cannot be empty.")
        return value

    @field_validator("MT5_LOGIN", mode="before")
    @classmethod
    def normalize_mt5_login(cls, value: object) -> object:
        if value == "":
            return None
        return value

    @property
    def trading_must_be_blocked(self) -> bool:
        return (
            self.EMERGENCY_STOP
            or not self.TRADING_ENABLED
            or self.BOT_MODE
            in {
                "OFFLINE",
                "ANALYSIS_ONLY",
                "LIVE_TRADING_LOCKED",
                "EMERGENCY_STOP",
            }
        )

    @property
    def live_trading_must_be_blocked(self) -> bool:
        return (
            self.trading_must_be_blocked
            or not self.LIVE_TRADING_AUTHORIZED
            or self.BOT_MODE != "LIVE_TRADING_AUTHORIZED"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
