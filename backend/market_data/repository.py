from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import case, desc, func, select
from sqlalchemy.exc import IntegrityError

from backend.database import Database
from backend.database_models import (
    MarketCandleRecord,
    MarketDataBatchRecord,
)
from backend.market_data.contracts import MarketCandleDataContract
from backend.market_data.models import (
    CandleQuality,
    MarketCandle,
    VolumeSource,
)
from backend.market_data.service import MarketDataSnapshot
from backend.mt5.timeframes import normalize_timeframe


class MarketDataPersistenceError(RuntimeError):
    """Raised when market data cannot safely be persisted."""


class MarketDataPersistenceConflict(MarketDataPersistenceError):
    """Raised when an existing candle conflicts with incoming data."""


@dataclass(frozen=True, slots=True)
class MarketDataDepthSummary:
    timeframe: str
    candle_count: int
    first_candle_at: datetime | None
    last_candle_at: datetime | None
    batch_count: int
    clock_verified_batch_count: int
    last_received_at: datetime | None


@dataclass(frozen=True, slots=True)
class MarketDataSaveResult:
    batch_id: int | None
    inserted_candles: int
    duplicate_candles: int


class MarketDataRepository:
    """Persists and retrieves validated market data."""

    def __init__(self, database: Database) -> None:
        self._database = database

    def save_snapshot(
        self,
        snapshot: MarketDataSnapshot,
    ) -> MarketDataSaveResult:
        if not isinstance(snapshot, MarketDataSnapshot):
            raise TypeError("snapshot must be a MarketDataSnapshot.")

        if not snapshot.analysis_eligible:
            raise MarketDataPersistenceError(
                "Ineligible market data cannot be persisted."
            )

        return self._save_snapshot(
            snapshot,
            retry_on_integrity_error=True,
        )

    def get_history(
        self,
        symbol: str,
        timeframe: str,
        count: int,
    ) -> tuple[MarketCandle, ...]:
        """Return the newest requested candles in chronological order."""

        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("Symbol must be a non-empty string.")

        if not isinstance(timeframe, str) or not timeframe.strip():
            raise ValueError("Timeframe must be a non-empty string.")

        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("Count must be an integer.")

        if not 1 <= count <= 10000:
            raise ValueError("Count must be between 1 and 10000.")

        normalized_symbol = symbol.strip()
        normalized_timeframe = normalize_timeframe(timeframe)

        with self._database.session() as session:
            records = session.scalars(
                select(MarketCandleRecord)
                .where(
                    MarketCandleRecord.symbol == normalized_symbol,
                    MarketCandleRecord.timeframe == normalized_timeframe,
                )
                .order_by(desc(MarketCandleRecord.timestamp))
                .limit(count)
            ).all()

        candles: list[MarketCandle] = []

        try:
            for record in reversed(records):
                candles.append(
                    MarketCandle(
                        symbol=record.symbol,
                        timeframe=record.timeframe,
                        timestamp=self._as_utc(record.timestamp),
                        open=record.open_price,
                        high=record.high_price,
                        low=record.low_price,
                        close=record.close_price,
                        tick_volume=record.tick_volume,
                        real_volume=record.real_volume,
                        volume_source=VolumeSource(record.volume_source),
                        is_closed=record.is_closed,
                        quality=CandleQuality(record.quality),
                    )
                )
        except Exception as exc:
            raise MarketDataPersistenceError(
                "Stored market data failed validation."
            ) from exc

        return tuple(candles)

    def get_candle_contract(
        self,
        candle_id: int,
    ) -> MarketCandleDataContract | None:
        """Return the standardized data contract and its persisted lineage."""
        if isinstance(candle_id, bool) or not isinstance(candle_id, int):
            raise TypeError("candle_id must be an integer.")
        if candle_id <= 0:
            raise ValueError("candle_id must be positive.")

        with self._database.session() as session:
            row = session.execute(
                select(MarketCandleRecord, MarketDataBatchRecord)
                .join(
                    MarketDataBatchRecord,
                    MarketCandleRecord.batch_id == MarketDataBatchRecord.id,
                )
                .where(MarketCandleRecord.id == candle_id)
            ).one_or_none()

        if row is None:
            return None

        candle, batch = row
        validation_report = batch.validation_report
        analysis_eligible = (
            validation_report.get("analysis_eligible")
            if isinstance(validation_report, dict)
            else None
        )

        return MarketCandleDataContract(
            candle_id=candle.id,
            batch_id=batch.id,
            instrument=candle.symbol,
            timeframe=candle.timeframe,
            event_time=self._as_utc(candle.timestamp),
            available_at=self._as_utc(batch.received_at),
            recorded_at=self._as_utc(batch.created_at),
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
            validation_report=validation_report,
        )

    def get_data_depth(
        self,
        symbol: str,
        timeframes: tuple[str, ...],
    ) -> tuple[MarketDataDepthSummary, ...]:
        """Return read-only persisted candle-depth metrics per timeframe.

        This reports observed storage facts only. It does not infer missing
        candles across broker sessions or claim that a history is sufficient
        for any particular analytical horizon.
        """
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("Symbol must be a non-empty string.")
        if not isinstance(timeframes, tuple) or not timeframes:
            raise ValueError("Timeframes must be a non-empty tuple.")

        normalized_symbol = symbol.strip()
        normalized_timeframes = tuple(
            normalize_timeframe(value) for value in timeframes
        )
        if len(set(normalized_timeframes)) != len(normalized_timeframes):
            raise ValueError("Timeframes must not contain duplicates.")

        with self._database.session() as session:
            rows = session.execute(
                select(
                    MarketCandleRecord.timeframe,
                    func.count(MarketCandleRecord.id),
                    func.min(MarketCandleRecord.timestamp),
                    func.max(MarketCandleRecord.timestamp),
                    func.count(func.distinct(MarketDataBatchRecord.id)),
                    func.count(
                        func.distinct(
                            case(
                                (
                                    MarketDataBatchRecord.clock_verified.is_(True),
                                    MarketDataBatchRecord.id,
                                ),
                                else_=None,
                            )
                        )
                    ),
                    func.max(MarketDataBatchRecord.received_at),
                )
                .join(
                    MarketDataBatchRecord,
                    MarketCandleRecord.batch_id == MarketDataBatchRecord.id,
                )
                .where(
                    MarketCandleRecord.symbol == normalized_symbol,
                    MarketCandleRecord.timeframe.in_(normalized_timeframes),
                )
                .group_by(MarketCandleRecord.timeframe)
            ).all()

        by_timeframe = {
            row[0]: row for row in rows
        }
        summaries: list[MarketDataDepthSummary] = []
        for timeframe in normalized_timeframes:
            row = by_timeframe.get(timeframe)
            if row is None:
                summaries.append(
                    MarketDataDepthSummary(
                        timeframe=timeframe,
                        candle_count=0,
                        first_candle_at=None,
                        last_candle_at=None,
                        batch_count=0,
                        clock_verified_batch_count=0,
                        last_received_at=None,
                    )
                )
                continue

            summaries.append(
                MarketDataDepthSummary(
                    timeframe=timeframe,
                    candle_count=int(row[1]),
                    first_candle_at=(
                        self._as_utc(row[2]) if row[2] is not None else None
                    ),
                    last_candle_at=(
                        self._as_utc(row[3]) if row[3] is not None else None
                    ),
                    batch_count=int(row[4]),
                    clock_verified_batch_count=int(row[5]),
                    last_received_at=(
                        self._as_utc(row[6]) if row[6] is not None else None
                    ),
                )
            )

        return tuple(summaries)

    def _save_snapshot(
        self,
        snapshot: MarketDataSnapshot,
        *,
        retry_on_integrity_error: bool,
    ) -> MarketDataSaveResult:
        batch = snapshot.batch
        timestamps = [candle.timestamp for candle in batch.candles]

        try:
            with self._database.session() as session:
                existing_records = session.scalars(
                    select(MarketCandleRecord).where(
                        MarketCandleRecord.symbol == batch.symbol,
                        MarketCandleRecord.timeframe == batch.timeframe,
                        MarketCandleRecord.timestamp >= min(timestamps),
                        MarketCandleRecord.timestamp <= max(timestamps),
                    )
                ).all()

                existing_by_timestamp = {
                    self._as_utc(record.timestamp): record
                    for record in existing_records
                }

                new_candles: list[MarketCandle] = []
                duplicate_count = 0

                for candle in batch.candles:
                    existing = existing_by_timestamp.get(
                        candle.timestamp
                    )

                    if existing is None:
                        new_candles.append(candle)
                        continue

                    if not self._matches(existing, candle):
                        raise MarketDataPersistenceConflict(
                            "Stored candle conflicts with incoming data: "
                            f"{batch.symbol}/{batch.timeframe}/"
                            f"{candle.timestamp.isoformat()}"
                        )

                    duplicate_count += 1

                if not new_candles:
                    return MarketDataSaveResult(
                        batch_id=None,
                        inserted_candles=0,
                        duplicate_candles=duplicate_count,
                    )

                evidence = batch.clock_evidence

                if evidence is None or evidence.verified is not True:
                    raise MarketDataPersistenceError(
                        "Verified clock evidence is required for persistence."
                    )

                now = datetime.now(timezone.utc)

                batch_record = MarketDataBatchRecord(
                    symbol=batch.symbol,
                    timeframe=batch.timeframe,
                    source=batch.source,
                    received_at=batch.received_at,
                    candle_count=len(new_candles),
                    created_at=now,
                    clock_verified=evidence.verified,
                    clock_account_login=evidence.account_login,
                    clock_broker_server=evidence.broker_server,
                    clock_terminal_build=evidence.terminal_build,
                    clock_checked_at=evidence.checked_at,
                    clock_ntp_offset_seconds=evidence.ntp_offset_seconds,
                    clock_ntp_uncertainty_seconds=(
                        evidence.ntp_uncertainty_seconds
                    ),
                    clock_ntp_sample_count=evidence.ntp_sample_count,
                    clock_ntp_servers=list(evidence.ntp_servers),
                    clock_candle_offset_seconds=(
                        evidence.candle_offset_seconds
                    ),
                    clock_candle_uncertainty_seconds=(
                        evidence.candle_uncertainty_seconds
                    ),
                    clock_calibration_sample_count=(
                        evidence.calibration_sample_count
                    ),
                    validation_report=snapshot.validation.model_dump(
                        mode="json"
                    ),
                )

                session.add(batch_record)
                session.flush()

                for candle in new_candles:
                    batch_record.candles.append(
                        MarketCandleRecord(
                            symbol=candle.symbol,
                            timeframe=candle.timeframe,
                            timestamp=candle.timestamp,
                            open_price=candle.open,
                            high_price=candle.high,
                            low_price=candle.low,
                            close_price=candle.close,
                            tick_volume=candle.tick_volume,
                            real_volume=candle.real_volume,
                            volume_source=candle.volume_source.value,
                            is_closed=candle.is_closed,
                            quality=candle.quality.value,
                        )
                    )

                session.flush()

                return MarketDataSaveResult(
                    batch_id=batch_record.id,
                    inserted_candles=len(new_candles),
                    duplicate_candles=duplicate_count,
                )

        except IntegrityError as exc:
            if retry_on_integrity_error:
                return self._save_snapshot(
                    snapshot,
                    retry_on_integrity_error=False,
                )

            raise MarketDataPersistenceError(
                "Database integrity constraints rejected market data."
            ) from exc

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc)

        return value.astimezone(timezone.utc)

    @classmethod
    def _matches(
        cls,
        record: MarketCandleRecord,
        candle: MarketCandle,
    ) -> bool:
        return (
            record.symbol == candle.symbol
            and record.timeframe == candle.timeframe
            and cls._as_utc(record.timestamp) == candle.timestamp
            and record.open_price == candle.open
            and record.high_price == candle.high
            and record.low_price == candle.low
            and record.close_price == candle.close
            and record.tick_volume == candle.tick_volume
            and record.real_volume == candle.real_volume
            and record.volume_source == candle.volume_source.value
            and record.is_closed == candle.is_closed
            and record.quality == candle.quality.value
        )
