from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from threading import RLock

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class CapabilityStatus(str, Enum):
    """Evidence-backed state of one market-data capability."""

    UNKNOWN = "UNKNOWN"
    AVAILABLE = "AVAILABLE"
    PARTIAL = "PARTIAL"
    UNAVAILABLE = "UNAVAILABLE"


class MarketDataCapability(BaseModel):
    """Immutable, evidence-backed description of a data-source capability.

    AVAILABLE means the capability was actually verified for the recorded
    identity and scope. It must not be inferred from an API existing in code.
    UNKNOWN is the safe default until a real probe or verified observation.
    """

    model_config = ConfigDict(frozen=True, allow_inf_nan=False)

    capability: str = Field(min_length=1, max_length=64)
    status: CapabilityStatus = CapabilityStatus.UNKNOWN
    source: str = Field(min_length=1, max_length=64)
    checked_at: datetime
    symbol: str | None = Field(default=None, min_length=1, max_length=64)
    timeframe: str | None = Field(default=None, min_length=2, max_length=8)
    account_login: int | None = Field(default=None, gt=0)
    broker_server: str | None = Field(default=None, min_length=1, max_length=128)
    evidence: tuple[str, ...] = ()
    limitations: tuple[str, ...] = ()

    @field_validator("capability", "source")
    @classmethod
    def normalize_required_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Value cannot be empty.")
        return value

    @field_validator("symbol", "timeframe", "broker_server")
    @classmethod
    def normalize_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("Optional text values cannot be blank.")
        return value

    @field_validator("timeframe")
    @classmethod
    def normalize_timeframe(cls, value: str | None) -> str | None:
        return value.upper() if value is not None else None

    @field_validator("checked_at")
    @classmethod
    def require_utc_checked_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("checked_at must be timezone-aware.")
        if value.utcoffset() != timedelta(0):
            raise ValueError("checked_at must already be UTC.")
        return value.astimezone(timezone.utc)

    @field_validator("account_login", mode="before")
    @classmethod
    def reject_boolean_account_login(cls, value):
        if isinstance(value, bool):
            raise ValueError("account_login must not be boolean.")
        return value

    @field_validator("evidence", "limitations")
    @classmethod
    def normalize_text_items(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(item.strip() for item in values if item.strip())
        if len(normalized) != len(values):
            raise ValueError("Evidence and limitation entries cannot be blank.")
        return normalized

    @model_validator(mode="after")
    def validate_observation_evidence(self) -> MarketDataCapability:
        if self.status is not CapabilityStatus.UNKNOWN and not self.evidence:
            raise ValueError(
                "A non-UNKNOWN capability status requires supporting evidence."
            )
        if (self.account_login is None) != (self.broker_server is None):
            raise ValueError(
                "account_login and broker_server must be supplied together."
            )
        return self

    @classmethod
    def registry_key(
        cls,
        capability: str,
        symbol: str | None = None,
        timeframe: str | None = None,
    ) -> tuple[str, str, str]:
        return (
            capability.strip().casefold(),
            (symbol or "").strip().casefold(),
            (timeframe or "").strip().upper(),
        )


class CapabilityRegistry:
    """Thread-safe in-memory registry of observed market-data capabilities.

    The registry stores observations only. It does not probe MT5, infer
    historical coverage, or turn an UNKNOWN capability into AVAILABLE.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._items: dict[tuple[str, str, str], MarketDataCapability] = {}

    def record(self, capability: MarketDataCapability) -> None:
        if not isinstance(capability, MarketDataCapability):
            raise TypeError("capability must be a MarketDataCapability.")

        key = MarketDataCapability.registry_key(
            capability.capability,
            capability.symbol,
            capability.timeframe,
        )
        with self._lock:
            self._items[key] = capability

    def get(
        self,
        capability: str,
        *,
        symbol: str | None = None,
        timeframe: str | None = None,
    ) -> MarketDataCapability | None:
        key = MarketDataCapability.registry_key(
            capability,
            symbol,
            timeframe,
        )
        with self._lock:
            return self._items.get(key)

    def snapshot(self) -> tuple[MarketDataCapability, ...]:
        with self._lock:
            return tuple(
                self._items[key]
                for key in sorted(self._items)
            )

    def clear(self) -> None:
        """Clear recorded observations; intended for lifecycle reset and tests."""
        with self._lock:
            self._items.clear()
