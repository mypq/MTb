
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import desc, select
from sqlalchemy.exc import IntegrityError

from backend.database import Database
from backend.database_models import (
    MarketCandleRecord,
    MarketDataBatchRecord,
)
from backend.market_data.models import (
    CandleQuality,
    MarketCandle,
    VolumeSource,
)
from backend.market_data.service import MarketDataSnapshot


class MarketDataPersistenceError(RuntimeError):
    """Raised when market data cannot safely be persisted."""


class MarketDataPersistenceConflict(MarketDataPersistenceError):
    """Raised when an existing candle conflicts with incoming data."""


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
        normalized_timeframe = timeframe.strip().upper()

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

                now = datetime.now(timezone.utc)

                batch_record = MarketDataBatchRecord(
                    symbol=batch.symbol,
                    timeframe=batch.timeframe,
                    source=batch.source,
                    received_at=batch.received_at,
                    candle_count=len(new_candles),
                    created_at=now,
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
