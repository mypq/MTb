
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class MT5ConnectionState(str, Enum):
    DISABLED = "DISABLED"
    DISCONNECTED = "DISCONNECTED"
    CONNECTED = "CONNECTED"
    ERROR = "ERROR"


class MT5ConnectionStatus(BaseModel):
    model_config = ConfigDict(frozen=True)

    state: MT5ConnectionState
    terminal_connected: bool = False
    trading_allowed: bool = False
    message: str = ""
    checked_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class MT5AccountInfo(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        allow_inf_nan=False,
    )

    login: int
    server: str
    currency: str
    balance: float
    equity: float
    margin: float = Field(ge=0)
    free_margin: float
    leverage: int = Field(ge=1)
    trade_allowed: bool
    trade_expert: bool


class MT5SymbolInfo(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        allow_inf_nan=False,
    )

    name: str
    description: str = ""
    digits: int = Field(ge=0, le=12)
    point: float = Field(gt=0)
    trade_contract_size: float = Field(gt=0)
    trade_tick_size: float = Field(gt=0)
    trade_tick_value: float = Field(ge=0)
    volume_min: float = Field(gt=0)
    volume_max: float = Field(gt=0)
    volume_step: float = Field(gt=0)
    spread: int = Field(ge=0)
    visible: bool



class MT5TickHistoryProbeResult(BaseModel):
    """Bounded, non-persistent result from MT5 historical tick APIs.

    Historical API timestamps are retained as raw millisecond values in
    this result. The probe does not feed them into broker-clock calibration.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    checked_at_utc: datetime
    lookback_start_utc: datetime
    lookback_days: int = Field(ge=1, le=365)
    requested_count_per_query: int = Field(ge=1, le=1000)
    all_ticks_count: int = Field(ge=0)
    quote_ticks_count: int = Field(ge=0)
    all_ticks_first_time_msc: int | None = Field(default=None, gt=0)
    all_ticks_last_time_msc: int | None = Field(default=None, gt=0)
    quote_ticks_first_time_msc: int | None = Field(default=None, gt=0)
    quote_ticks_last_time_msc: int | None = Field(default=None, gt=0)

    @field_validator("checked_at_utc", "lookback_start_utc")
    @classmethod
    def require_utc_datetime(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
            raise ValueError("Probe datetimes must be timezone-aware UTC.")
        return value
