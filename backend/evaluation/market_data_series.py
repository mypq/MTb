from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol

from sqlalchemy import select

from backend.database import Database
from backend.database_models import MarketCandleRecord, MarketDataBatchRecord
from backend.evaluation.market_data import (
    EvaluationMarketDataError,
)
from backend.market_data.contracts import MarketCandleDataContract
from backend.market_data.repository import MarketDataRepository
from backend.mt5.timeframes import normalize_timeframe, timeframe_seconds


class EvaluationRangeRepository(Protocol):
    def get_candle_contracts(
        self,
        instrument: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> tuple[MarketCandleDataContract, ...]:
        ...


@dataclass(frozen=True, slots=True)
class EvaluationCandleGap:
    previous_event_time: datetime
    next_event_time: datetime
    expected_interval_seconds: int
    actual_interval_seconds: int
    missing_intervals: int | None


@dataclass(frozen=True, slots=True)
class EvaluationCandleSeriesEvidence:
    instrument: str
    timeframe: str
    start_at: datetime
    end_at: datetime
    available_at_cutoff: datetime
    candles: tuple[MarketCandleDataContract, ...]
    gaps: tuple[EvaluationCandleGap, ...]
    duplicate_event_times: tuple[datetime, ...]
    is_contiguous: bool
    latest_available_at: datetime | None


class EvaluationMarketDataRepository:
    """Read-only range adapter over the existing market-data tables."""

    def __init__(self, database: Database) -> None:
        if not isinstance(database, Database):
            raise TypeError("database must be an initialized Database instance.")
        self._database = database
        self._candle_repository = MarketDataRepository(database)

    def get_candle_contract(self, candle_id: int):
        return self._candle_repository.get_candle_contract(candle_id)

    def get_candle_contracts(
        self,
        instrument: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
    ) -> tuple[MarketCandleDataContract, ...]:
        if not isinstance(instrument, str) or not instrument.strip():
            raise ValueError("instrument must be a non-empty string.")
        _require_utc(start_at, "start_at")
        _require_utc(end_at, "end_at")
        if start_at >= end_at:
            raise ValueError("start_at must be earlier than end_at.")

        normalized_timeframe = normalize_timeframe(timeframe)
        normalized_instrument = instrument.strip()

        with self._database.session() as session:
            rows = session.execute(
                select(MarketCandleRecord, MarketDataBatchRecord)
                .join(
                    MarketDataBatchRecord,
                    MarketCandleRecord.batch_id == MarketDataBatchRecord.id,
                )
                .where(
                    MarketCandleRecord.symbol == normalized_instrument,
                    MarketCandleRecord.timeframe == normalized_timeframe,
                    MarketCandleRecord.timestamp >= start_at,
                    MarketCandleRecord.timestamp < end_at,
                )
                .order_by(
                    MarketCandleRecord.timestamp.asc(),
                    MarketCandleRecord.id.asc(),
                )
            ).all()

        contracts = []
        for candle, batch in rows:
            report = batch.validation_report
            analysis_eligible = (
                report.get("analysis_eligible")
                if isinstance(report, dict)
                else None
            )
            contracts.append(
                MarketCandleDataContract(
                    candle_id=candle.id,
                    batch_id=batch.id,
                    instrument=candle.symbol,
                    timeframe=candle.timeframe,
                    event_time=_as_utc(candle.timestamp),
                    available_at=_as_utc(batch.received_at),
                    recorded_at=_as_utc(batch.created_at),
                    open=candle.open_price,
                    high=candle.high_price,
                    low=candle.low_price,
                    close=candle.close_price,
                    tick_volume=candle.tick_volume,
                    real_volume=candle.real_volume,
                    volume_source=candle.volume_source,
                    candle_quality=candle.quality,
                    is_closed=candle.is_closed,
                    source=batch.source,
                    source_account_login=batch.clock_account_login,
                    source_broker_server=batch.clock_broker_server,
                    clock_verified=batch.clock_verified,
                    analysis_eligible=analysis_eligible,
                    validation_report=report,
                )
            )
        return tuple(contracts)


class EvaluationCandleSeriesReader:
    """Loads a bounded historical candle sequence and reports every gap.

    It never fills, repairs, filters out late data, or assumes a gap is a
    market closure. The caller receives explicit evidence and must not treat
    a non-contiguous sequence as a complete lookback window.
    """

    def __init__(self, repository: EvaluationRangeRepository) -> None:
        if not callable(getattr(repository, "get_candle_contracts", None)):
            raise TypeError("repository must provide get_candle_contracts().")
        self._repository = repository

    def load_series(
        self,
        *,
        instrument: str,
        timeframe: str,
        start_at: datetime,
        end_at: datetime,
        available_at_cutoff: datetime,
    ) -> EvaluationCandleSeriesEvidence:
        if not isinstance(instrument, str) or not instrument.strip():
            raise ValueError("instrument must be a non-empty string.")
        _require_utc(start_at, "start_at")
        _require_utc(end_at, "end_at")
        _require_utc(available_at_cutoff, "available_at_cutoff")
        if start_at >= end_at:
            raise ValueError("start_at must be earlier than end_at.")

        normalized_instrument = instrument.strip()
        normalized_timeframe = normalize_timeframe(timeframe)
        interval = timeframe_seconds(normalized_timeframe)
        candles = self._repository.get_candle_contracts(
            normalized_instrument,
            normalized_timeframe,
            start_at,
            end_at,
        )

        previous_time = None
        seen_times: set[datetime] = set()
        duplicates: list[datetime] = []
        gaps: list[EvaluationCandleGap] = []

        for candle in candles:
            if candle.instrument != normalized_instrument:
                raise EvaluationMarketDataError(
                    f"Candle {candle.candle_id} belongs to another instrument."
                )
            if normalize_timeframe(candle.timeframe) != normalized_timeframe:
                raise EvaluationMarketDataError(
                    f"Candle {candle.candle_id} has a different timeframe."
                )
            if candle.event_time < start_at or candle.event_time >= end_at:
                raise EvaluationMarketDataError(
                    f"Candle {candle.candle_id} is outside the requested range."
                )
            if candle.event_time > available_at_cutoff:
                raise EvaluationMarketDataError(
                    f"Candle {candle.candle_id} occurs after the evaluation cutoff."
                )
            if candle.available_at > available_at_cutoff:
                raise EvaluationMarketDataError(
                    f"Candle {candle.candle_id} became available after the input cutoff."
                )
            if not candle.is_closed or candle.candle_quality != "VERIFIED":
                raise EvaluationMarketDataError(
                    f"Candle {candle.candle_id} is not verified and closed."
                )
            if candle.clock_verified is not True:
                raise EvaluationMarketDataError(
                    f"Candle {candle.candle_id} has no verified source clock."
                )
            if candle.analysis_eligible is not True:
                raise EvaluationMarketDataError(
                    f"Candle {candle.candle_id} is not approved for analysis."
                )
            if (
                not isinstance(candle.validation_report, dict)
                or candle.validation_report.get("analysis_eligible") is not True
            ):
                raise EvaluationMarketDataError(
                    f"Candle {candle.candle_id} lacks positive persisted validation evidence."
                )

            if candle.event_time in seen_times:
                duplicates.append(candle.event_time)
            seen_times.add(candle.event_time)

            if previous_time is not None:
                delta = int((candle.event_time - previous_time).total_seconds())
                if delta < 0:
                    raise EvaluationMarketDataError(
                        "Candle sequence is not chronological."
                    )
                if delta == 0:
                    previous_time = candle.event_time
                    continue
                if delta != interval:
                    missing = (
                        delta // interval - 1
                        if delta > interval and delta % interval == 0
                        else None
                    )
                    gaps.append(
                        EvaluationCandleGap(
                            previous_event_time=previous_time,
                            next_event_time=candle.event_time,
                            expected_interval_seconds=interval,
                            actual_interval_seconds=delta,
                            missing_intervals=missing,
                        )
                    )
            previous_time = candle.event_time

        latest_available = (
            max(item.available_at for item in candles)
            if candles else None
        )
        return EvaluationCandleSeriesEvidence(
            instrument=normalized_instrument,
            timeframe=normalized_timeframe,
            start_at=start_at,
            end_at=end_at,
            available_at_cutoff=available_at_cutoff,
            candles=candles,
            gaps=tuple(gaps),
            duplicate_event_times=tuple(duplicates),
            is_contiguous=bool(candles) and not gaps and not duplicates,
            latest_available_at=latest_available,
        )


def _require_utc(value: datetime, name: str) -> None:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
        or value.utcoffset() != timedelta(0)
    ):
        raise ValueError(f"{name} must be a UTC-aware datetime.")


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
