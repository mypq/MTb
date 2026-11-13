from __future__ import annotations

from typing import Protocol

from pydantic import BaseModel, ConfigDict, model_validator

from backend.market_data.models import CandleBatch
from backend.market_data.validator import (
    MarketDataValidationReport,
    MarketDataValidator,
)
from backend.mt5.timeframes import normalize_timeframe, timeframe_seconds


class MarketDataSource(Protocol):
    """Interface required from a market-data source."""

    def get_closed_candles(
        self,
        symbol: str,
        timeframe: str,
        count: int,
    ) -> CandleBatch:
        ...


class MarketDataContractError(ValueError):
    """Raised when a market-data source violates the requested data contract."""


class MarketDataSnapshot(BaseModel):
    """A candle batch together with its validation result."""

    model_config = ConfigDict(frozen=True)

    batch: CandleBatch
    validation: MarketDataValidationReport

    @model_validator(mode="after")
    def validate_snapshot_consistency(self) -> MarketDataSnapshot:
        if self.batch.symbol != self.validation.symbol:
            raise ValueError("Snapshot symbol does not match validation report.")

        if self.batch.timeframe != self.validation.timeframe:
            raise ValueError(
                "Snapshot timeframe does not match validation report."
            )

        if self.batch.source != self.validation.source:
            raise ValueError("Snapshot source does not match validation report.")

        if len(self.batch.candles) != self.validation.candle_count:
            raise ValueError(
                "Snapshot candle count does not match validation report."
            )

        return self

    @property
    def analysis_eligible(self) -> bool:
        return self.validation.analysis_eligible

    @property
    def volume_data_available(self) -> bool:
        return self.validation.volume_data_available


class MarketDataRejectedError(RuntimeError):
    """Raised when market data cannot safely enter an analysis operation."""

    def __init__(self, snapshot: MarketDataSnapshot) -> None:
        self.snapshot = snapshot

        issue_codes = sorted(
            {
                issue.code.value
                for issue in snapshot.validation.issues
            }
        )

        if issue_codes:
            reason = ", ".join(issue_codes)
        else:
            reason = "DATA_NOT_ELIGIBLE"

        super().__init__(
            f"Market data rejected for "
            f"{snapshot.batch.symbol}/{snapshot.batch.timeframe}: {reason}"
        )


class MarketDataService:
    """
    Retrieves market data and applies strict contract and data validation.

    The service does not repair candles, fill gaps, infer real volume,
    or override validation failures.
    """

    def __init__(
        self,
        source: MarketDataSource,
        validator: MarketDataValidator | None = None,
    ) -> None:
        self._source = source
        self._validator = validator or MarketDataValidator()

    def get_snapshot(
        self,
        symbol: str,
        timeframe: str,
        count: int,
        checked_at=None,
    ) -> MarketDataSnapshot:
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("Symbol must be a non-empty string.")

        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("Count must be an integer.")

        if count <= 0:
            raise ValueError("Count must be greater than zero.")

        requested_symbol = symbol.strip()
        requested_timeframe = normalize_timeframe(timeframe)
        expected_seconds = timeframe_seconds(requested_timeframe)

        batch = self._source.get_closed_candles(
            symbol=requested_symbol,
            timeframe=requested_timeframe,
            count=count,
        )

        if not isinstance(batch, CandleBatch):
            raise MarketDataContractError(
                "Market-data source must return a CandleBatch."
            )

        if batch.symbol.strip() != requested_symbol:
            raise MarketDataContractError(
                "Returned symbol does not match the requested symbol."
            )

        try:
            returned_timeframe = normalize_timeframe(batch.timeframe)
        except (TypeError, ValueError) as exc:
            raise MarketDataContractError(
                "Market-data source returned an unsupported timeframe."
            ) from exc

        if returned_timeframe != requested_timeframe:
            raise MarketDataContractError(
                "Returned timeframe does not match the requested timeframe."
            )

        if len(batch.candles) != count:
            raise MarketDataContractError(
                f"Requested {count} candles but received "
                f"{len(batch.candles)}."
            )

        report = self._validator.validate(
            batch=batch,
            timeframe_seconds=expected_seconds,
            checked_at=checked_at,
        )

        return MarketDataSnapshot(
            batch=batch,
            validation=report,
        )

    def get_analysis_batch(
        self,
        symbol: str,
        timeframe: str,
        count: int,
        *,
        require_volume: bool = False,
        checked_at=None,
    ) -> CandleBatch:
        snapshot = self.get_snapshot(
            symbol=symbol,
            timeframe=timeframe,
            count=count,
            checked_at=checked_at,
        )

        if not snapshot.analysis_eligible:
            raise MarketDataRejectedError(snapshot)

        if require_volume and not snapshot.volume_data_available:
            raise MarketDataRejectedError(snapshot)

        return snapshot.batch
