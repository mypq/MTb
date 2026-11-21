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
from backend.market_data.contracts import (
    MarketCandleDataContract,
    MarketDataBatchContract,
)
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
    validation_report_batch_count: int
    analysis_eligible_batch_count: int
    freshness_verified_batch_count: int
    validation_issue_count: int
    unconfirmed_gap_count: int
    verified_candle_count: int
    closed_candle_count: int
    unknown_volume_candle_count: int
    last_validation_at: datetime | None


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

    def get_batch_contract(
        self,
        batch_id: int,
    ) -> MarketDataBatchContract | None:
        """Return the source, validation evidence, and candle IDs for a batch."""
        if isinstance(batch_id, bool) or not isinstance(batch_id, int):
            raise TypeError("batch_id must be an integer.")
        if batch_id <= 0:
            raise ValueError("batch_id must be positive.")

        with self._database.session() as session:
            batch = session.get(MarketDataBatchRecord, batch_id)
            if batch is None:
                return None

            candle_ids = tuple(session.scalars(
                select(MarketCandleRecord.id)
                .where(MarketCandleRecord.batch_id == batch_id)
                .order_by(MarketCandleRecord.timestamp)
            ).all())

            clock_evidence = None
            if batch.clock_verified is not None:
                clock_evidence = {
                    "verified": batch.clock_verified,
                    "checked_at": (
                        self._as_utc(batch.clock_checked_at).isoformat()
                        if batch.clock_checked_at is not None else None
                    ),
                    "terminal_build": batch.clock_terminal_build,
                    "ntp_offset_seconds": batch.clock_ntp_offset_seconds,
                    "ntp_uncertainty_seconds": batch.clock_ntp_uncertainty_seconds,
                    "ntp_sample_count": batch.clock_ntp_sample_count,
                    "ntp_servers": batch.clock_ntp_servers,
                    "candle_offset_seconds": batch.clock_candle_offset_seconds,
                    "candle_uncertainty_seconds": batch.clock_candle_uncertainty_seconds,
                    "calibration_sample_count": batch.clock_calibration_sample_count,
                }

            return MarketDataBatchContract(
                batch_id=batch.id,
                instrument=batch.symbol,
                timeframe=batch.timeframe,
                source=batch.source,
                available_at=self._as_utc(batch.received_at),
                recorded_at=self._as_utc(batch.created_at),
                candle_count=len(candle_ids),
                candle_ids=candle_ids,
                source_account_login=batch.clock_account_login,
                source_broker_server=batch.clock_broker_server,
                clock_verified=batch.clock_verified,
                clock_evidence=clock_evidence,
                validation_report=batch.validation_report,
            )

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
        """Return observed candle depth and persisted validation coverage.

        No market-session gaps are inferred here. Only gaps explicitly
        recorded by the ingestion validator are counted.
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
            candle_rows = session.execute(
                select(
                    MarketCandleRecord.timeframe,
                    func.count(MarketCandleRecord.id),
                    func.min(MarketCandleRecord.timestamp),
                    func.max(MarketCandleRecord.timestamp),
                    func.count(func.distinct(MarketDataBatchRecord.id)),
                    func.count(func.distinct(case(
                        (
                            MarketDataBatchRecord.clock_verified.is_(True),
                            MarketDataBatchRecord.id,
                        ),
                        else_=None,
                    ))),
                    func.max(MarketDataBatchRecord.received_at),
                    func.sum(case(
                        (MarketCandleRecord.quality == "VERIFIED", 1),
                        else_=0,
                    )),
                    func.sum(case(
                        (MarketCandleRecord.is_closed.is_(True), 1),
                        else_=0,
                    )),
                    func.sum(case(
                        (MarketCandleRecord.volume_source == "UNKNOWN", 1),
                        else_=0,
                    )),
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

            batch_rows = session.execute(
                select(
                    MarketDataBatchRecord.timeframe,
                    MarketDataBatchRecord.validation_report,
                ).where(
                    MarketDataBatchRecord.symbol == normalized_symbol,
                    MarketDataBatchRecord.timeframe.in_(normalized_timeframes),
                )
            ).all()

        candle_by_timeframe = {row[0]: row for row in candle_rows}
        validation_by_timeframe: dict[str, dict[str, object]] = {
            timeframe: {
                "reports": 0,
                "eligible": 0,
                "freshness": 0,
                "issues": 0,
                "unconfirmed_gaps": 0,
                "last_validation": None,
            }
            for timeframe in normalized_timeframes
        }

        for timeframe, report in batch_rows:
            stats = validation_by_timeframe[timeframe]
            if not isinstance(report, dict):
                continue

            stats["reports"] = int(stats["reports"]) + 1
            if report.get("analysis_eligible") is True:
                stats["eligible"] = int(stats["eligible"]) + 1
            if report.get("freshness_verified") is True:
                stats["freshness"] = int(stats["freshness"]) + 1

            issues = report.get("issues")
            if isinstance(issues, list):
                stats["issues"] = int(stats["issues"]) + len(issues)

            gaps = report.get("gaps")
            if isinstance(gaps, list):
                stats["unconfirmed_gaps"] = (
                    int(stats["unconfirmed_gaps"])
                    + sum(
                        1 for gap in gaps
                        if isinstance(gap, dict)
                        and gap.get("confirmed_closed") is not True
                    )
                )

            checked_at = report.get("checked_at")
            if isinstance(checked_at, str):
                try:
                    parsed = datetime.fromisoformat(
                        checked_at.replace("Z", "+00:00")
                    )
                    if parsed.tzinfo is not None and parsed.utcoffset() is not None:
                        parsed = parsed.astimezone(timezone.utc)
                        previous = stats["last_validation"]
                        if previous is None or parsed > previous:
                            stats["last_validation"] = parsed
                except ValueError:
                    pass

        summaries: list[MarketDataDepthSummary] = []
        for timeframe in normalized_timeframes:
            row = candle_by_timeframe.get(timeframe)
            stats = validation_by_timeframe[timeframe]
            if row is None:
                summaries.append(MarketDataDepthSummary(
                    timeframe=timeframe,
                    candle_count=0,
                    first_candle_at=None,
                    last_candle_at=None,
                    batch_count=0,
                    clock_verified_batch_count=0,
                    last_received_at=None,
                    validation_report_batch_count=0,
                    analysis_eligible_batch_count=0,
                    freshness_verified_batch_count=0,
                    validation_issue_count=0,
                    unconfirmed_gap_count=0,
                    verified_candle_count=0,
                    closed_candle_count=0,
                    unknown_volume_candle_count=0,
                    last_validation_at=None,
                ))
                continue

            summaries.append(MarketDataDepthSummary(
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
                validation_report_batch_count=int(stats["reports"]),
                analysis_eligible_batch_count=int(stats["eligible"]),
                freshness_verified_batch_count=int(stats["freshness"]),
                validation_issue_count=int(stats["issues"]),
                unconfirmed_gap_count=int(stats["unconfirmed_gaps"]),
                verified_candle_count=int(row[7] or 0),
                closed_candle_count=int(row[8] or 0),
                unknown_volume_candle_count=int(row[9] or 0),
                last_validation_at=stats["last_validation"],
            ))

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
