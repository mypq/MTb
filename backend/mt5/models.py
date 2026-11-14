
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


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
    model_config = ConfigDict(frozen=True)

    login: int
    server: str
    currency: str
    balance: float
    equity: float
    margin: float
    free_margin: float
    leverage: int
    trade_allowed: bool
    trade_expert: bool


class MT5SymbolInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

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
