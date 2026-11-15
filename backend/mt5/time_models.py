
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BrokerTimeSample(BaseModel):
    """Raw broker quote timestamp with local receipt-clock readings.

    broker_time_msc is deliberately preserved as a raw Unix-millisecond
    value. This model does not assert that the broker timestamp is UTC.
    """

    model_config = ConfigDict(
        frozen=True,
        allow_inf_nan=False,
    )

    symbol: str = Field(min_length=1)
    broker_time_msc: int = Field(gt=0)

    received_at_utc: datetime
    received_monotonic_ns: int = Field(ge=0)

    bid: float = Field(gt=0)
    ask: float = Field(gt=0)

    account_login: int = Field(gt=0)
    broker_server: str = Field(min_length=1)

    source: Literal["symbol_info_tick"] = "symbol_info_tick"

    @field_validator("symbol", "broker_server")
    @classmethod
    def normalize_required_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Value cannot be empty.")
        return normalized

    @field_validator(
        "broker_time_msc",
        "received_monotonic_ns",
        "account_login",
        mode="before",
    )
    @classmethod
    def reject_boolean_integer_fields(cls, value):
        if isinstance(value, bool):
            raise ValueError("Integer fields must not be boolean.")
        return value

    @field_validator("received_at_utc")
    @classmethod
    def require_utc_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(
                "received_at_utc must be timezone-aware."
            )

        if value.utcoffset() != timedelta(0):
            raise ValueError(
                "received_at_utc must already be UTC."
            )

        return value.astimezone(datetime.now().astimezone().tzinfo).astimezone(
            __import__("datetime").timezone.utc
        )

    @field_validator("bid", "ask", mode="before")
    @classmethod
    def reject_boolean_prices(cls, value):
        if isinstance(value, bool):
            raise ValueError("Prices must not be boolean.")
        return value

    @field_validator("ask")
    @classmethod
    def validate_ask_price(cls, value: float, info):
        bid = info.data.get("bid")
        if bid is not None and value < bid:
            raise ValueError("Ask price cannot be lower than bid.")
        return value
