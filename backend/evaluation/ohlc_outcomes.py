from __future__ import annotations

from datetime import datetime, timedelta, timezone
from math import isfinite
from uuid import uuid4

from backend.evaluation.contracts import (
    EvaluationDirection,
    EvaluationHorizon,
    EvaluationOutcomeContract,
    EvaluationOutcomeStatus,
    EvaluationSignalContract,
    ExecutionPriceSide,
    OutcomeMeasurementBasis,
    SpreadAccounting,
)
from backend.market_data.contracts import MarketCandleDataContract


class EvaluationOHLCOutcomeCalculator:
    """Calculate explicitly approximate counterfactual outcomes from M1 OHLC.

    Entry is the open of the first M1 candle whose start is strictly after the
    signal. The holding horizon begins at that assumed entry time. Exit is the
    close of the M1 candle ending exactly at entry_time + horizon.

    A complete, duplicate-free sequence of M1 candles is required throughout
    the holding window. No spread, slippage, commission, swap, or net PnL is
    inferred from OHLC data.
    """

    SUPPORTED_HORIZONS = (5, 15, 30, 60)

    def calculate(
        self,
        signal: EvaluationSignalContract,
        horizon_minutes: EvaluationHorizon,
        candles: tuple[MarketCandleDataContract, ...] | list[MarketCandleDataContract],
        *,
        created_at: datetime | None = None,
    ) -> EvaluationOutcomeContract:
        if not isinstance(signal, EvaluationSignalContract):
            raise TypeError("signal must be an EvaluationSignalContract.")
        if isinstance(horizon_minutes, bool) or horizon_minutes not in self.SUPPORTED_HORIZONS:
            raise ValueError("Unsupported evaluation horizon.")
        if not isinstance(candles, (tuple, list)):
            raise TypeError("candles must be a tuple or list of candle contracts.")

        created = created_at or datetime.now(timezone.utc)
        self._require_utc(created, "created_at")
        items = tuple(candles)
        if any(not isinstance(item, MarketCandleDataContract) for item in items):
            raise TypeError("candles must contain MarketCandleDataContract values.")

        for candle in items:
            if candle.instrument != signal.instrument:
                raise ValueError("Candle instrument does not match the signal.")
            if candle.timeframe != "M1":
                raise ValueError("OHLC outcome calculation requires M1 candles.")

        if any(
            items[index].event_time > items[index + 1].event_time
            for index in range(len(items) - 1)
        ):
            raise ValueError("M1 candles must be supplied in chronological order.")

        entry_candidates = [
            candle for candle in items if candle.event_time > signal.signal_at
        ]
        if not entry_candidates:
            return self._incomplete(
                signal, horizon_minutes, EvaluationOutcomeStatus.NEEDS_MORE_DATA, created
            )

        entry = entry_candidates[0]
        exit_at = entry.event_time + timedelta(minutes=horizon_minutes)
        exit_candle_at = exit_at - timedelta(minutes=1)

        window = [
            candle for candle in items
            if entry.event_time <= candle.event_time <= exit_candle_at
        ]
        by_time: dict[datetime, MarketCandleDataContract] = {}
        for candle in window:
            if candle.event_time in by_time:
                return self._incomplete(
                    signal, horizon_minutes, EvaluationOutcomeStatus.INVALID, created
                )
            by_time[candle.event_time] = candle

        expected_count = horizon_minutes
        if len(window) != expected_count:
            return self._incomplete(
                signal, horizon_minutes, EvaluationOutcomeStatus.NEEDS_MORE_DATA, created
            )

        expected_times = tuple(
            entry.event_time + timedelta(minutes=offset)
            for offset in range(horizon_minutes)
        )
        if tuple(candle.event_time for candle in window) != expected_times:
            return self._incomplete(
                signal, horizon_minutes, EvaluationOutcomeStatus.NEEDS_MORE_DATA, created
            )

        for candle in window:
            values = (candle.open, candle.high, candle.low, candle.close)
            if (
                candle.event_time.second != 0
                or candle.event_time.microsecond != 0
                or not candle.is_closed
                or candle.candle_quality != "VERIFIED"
                or candle.clock_verified is not True
                or candle.analysis_eligible is not True
                or not isinstance(candle.validation_report, dict)
                or candle.validation_report.get("analysis_eligible") is not True
                or any(not isfinite(value) or value <= 0 for value in values)
                or candle.low > min(candle.open, candle.close)
                or candle.high < max(candle.open, candle.close)
                or candle.high < candle.low
            ):
                return self._incomplete(
                    signal, horizon_minutes, EvaluationOutcomeStatus.INVALID, created
                )

        exit_candle = by_time[exit_candle_at]
        entry_price = float(entry.open)
        exit_price = float(exit_candle.close)
        gross_pnl = (
            exit_price - entry_price
            if signal.direction == EvaluationDirection.BUY
            else entry_price - exit_price
        )

        return EvaluationOutcomeContract(
            outcome_id=uuid4(),
            run_id=signal.run_id,
            signal_id=signal.signal_id,
            period_id=signal.period_id,
            horizon_minutes=horizon_minutes,
            direction=signal.direction,
            status=EvaluationOutcomeStatus.COMPLETED,
            measurement_basis=OutcomeMeasurementBasis.OHLC_APPROXIMATION,
            entry_at=entry.event_time,
            exit_at=exit_at,
            entry_price=entry_price,
            exit_price=exit_price,
            entry_price_side=ExecutionPriceSide.UNKNOWN,
            exit_price_side=ExecutionPriceSide.UNKNOWN,
            gross_pnl=gross_pnl,
            spread_accounting=SpreadAccounting.UNKNOWN,
            created_at=created,
        )

    @staticmethod
    def _incomplete(
        signal: EvaluationSignalContract,
        horizon_minutes: EvaluationHorizon,
        status: EvaluationOutcomeStatus,
        created_at: datetime,
    ) -> EvaluationOutcomeContract:
        return EvaluationOutcomeContract(
            outcome_id=uuid4(),
            run_id=signal.run_id,
            signal_id=signal.signal_id,
            period_id=signal.period_id,
            horizon_minutes=horizon_minutes,
            direction=signal.direction,
            status=status,
            measurement_basis=OutcomeMeasurementBasis.UNAVAILABLE,
            created_at=created_at,
        )

    @staticmethod
    def _require_utc(value: datetime, name: str) -> None:
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
            or value.utcoffset() != timedelta(0)
        ):
            raise ValueError(f"{name} must be a UTC-aware datetime.")
