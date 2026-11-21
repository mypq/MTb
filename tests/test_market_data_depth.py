from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from backend.database import Database
from backend.database_models import (
    DatabaseBase,
    MarketCandleRecord,
    MarketDataBatchRecord,
)
from backend.market_data.data_depth import MarketDataDepthAuditor
from backend.market_data.repository import MarketDataRepository


UTC = timezone.utc


@pytest.fixture
def repository():
    database = Database("sqlite:///:memory:")
    database.initialize()
    DatabaseBase.metadata.create_all(database.engine)
    yield MarketDataRepository(database), database
    database.dispose()


def add_batch(session, *, timeframe, received_at, clock_verified, timestamps, validation_report=None):
    batch = MarketDataBatchRecord(
        symbol="XAUUSD",
        timeframe=timeframe,
        source="MT5",
        received_at=received_at,
        candle_count=len(timestamps),
        created_at=received_at,
        clock_verified=clock_verified,
        validation_report=validation_report,
    )
    for timestamp in timestamps:
        batch.candles.append(
            MarketCandleRecord(
                symbol="XAUUSD",
                timeframe=timeframe,
                timestamp=timestamp,
                open_price=2000.0,
                high_price=2005.0,
                low_price=1995.0,
                close_price=2001.0,
                tick_volume=100,
                real_volume=None,
                volume_source="TICK",
                is_closed=True,
                quality="VERIFIED",
            )
        )
    session.add(batch)
    return batch


def test_depth_audit_reports_empty_timeframes_without_claiming_readiness(
    repository,
):
    repo, _database = repository

    report = MarketDataDepthAuditor(repo).audit("XAUUSD")

    assert report.symbol == "XAUUSD"
    assert len(report.timeframes) == 7
    assert report.candle_data_status == "NO_DATA"
    assert report.data_foundation_status == "PARTIAL"
    assert report.history_sufficiency_assessed is False
    assert [item.horizon_minutes for item in report.evaluation_sample_policy] == [5, 15, 30, 60]
    assert all(
        item.minimum_completed_non_overlapping_trades == 300
        and item.independent_period_count == 3
        and item.minimum_trades_per_period == 75
        and item.insufficient_sample_status == "NEEDS_MORE_DATA"
        for item in report.evaluation_sample_policy
    )
    assert all(item.status == "NO_DATA" for item in report.timeframes)
    assert all(item.ingestion_status == "NO_DATA" for item in report.timeframes)
    assert all(item.candle_count == 0 for item in report.timeframes)
    assert all(item.history_span_seconds is None for item in report.timeframes)


def test_depth_audit_reports_persisted_counts_ranges_and_clock_batches(
    repository,
):
    repo, database = repository
    first = datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    second = datetime(2026, 1, 1, 10, 5, tzinfo=UTC)
    received = datetime(2026, 1, 1, 10, 6, tzinfo=UTC)

    with database.session() as session:
        add_batch(
            session,
            timeframe="M5",
            received_at=received,
            clock_verified=True,
            timestamps=(first, second),
            validation_report={
                "checked_at": received.isoformat(),
                "analysis_eligible": True,
                "freshness_verified": True,
                "issues": [],
                "gaps": [],
            },
        )
        add_batch(
            session,
            timeframe="M15",
            received_at=received,
            clock_verified=False,
            timestamps=(first,),
        )

    report = MarketDataDepthAuditor(repo).audit("XAUUSD")
    by_timeframe = {item.timeframe: item for item in report.timeframes}

    assert by_timeframe["M5"].status == "AVAILABLE"
    assert by_timeframe["M5"].candle_count == 2
    assert by_timeframe["M5"].first_candle_at == first
    assert by_timeframe["M5"].last_candle_at == second
    assert by_timeframe["M5"].history_span_seconds == 300
    assert by_timeframe["M5"].batch_count == 1
    assert by_timeframe["M5"].clock_verified_batch_count == 1
    assert by_timeframe["M5"].validation_report_batch_count == 1
    assert by_timeframe["M5"].analysis_eligible_batch_count == 1
    assert by_timeframe["M5"].freshness_verified_batch_count == 1
    assert by_timeframe["M5"].validation_coverage_percent == 100.0
    assert by_timeframe["M5"].verified_candle_count == 2
    assert by_timeframe["M5"].closed_candle_count == 2
    assert by_timeframe["M5"].ingestion_status == "VALIDATED"
    assert by_timeframe["M5"].last_validation_at == received

    assert by_timeframe["M15"].candle_count == 1
    assert by_timeframe["M15"].clock_verified_batch_count == 0
    assert by_timeframe["M15"].validation_report_batch_count == 0
    assert by_timeframe["M15"].ingestion_status == "LIMITED"
    assert by_timeframe["M1"].status == "NO_DATA"
    assert report.candle_data_status == "LIMITED"


def test_depth_query_rejects_duplicate_or_invalid_timeframes(repository):
    repo, _database = repository

    with pytest.raises(ValueError):
        repo.get_data_depth("XAUUSD", ("M5", "m5"))

    with pytest.raises(ValueError):
        repo.get_data_depth("XAUUSD", ("W1",))

    with pytest.raises(ValueError):
        repo.get_data_depth("  ", ("M5",))



def test_depth_audit_counts_unconfirmed_gaps_and_unknown_volume(repository):
    repo, database = repository
    first = datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    received = datetime(2026, 1, 1, 10, 6, tzinfo=UTC)

    with database.session() as session:
        batch = add_batch(
            session,
            timeframe="M5",
            received_at=received,
            clock_verified=True,
            timestamps=(first,),
            validation_report={
                "checked_at": received.isoformat(),
                "analysis_eligible": True,
                "freshness_verified": True,
                "issues": [{"code": "TIME_GAP"}],
                "gaps": [
                    {"confirmed_closed": False},
                    {"confirmed_closed": True},
                ],
            },
        )
        batch.candles[0].volume_source = "UNKNOWN"

    report = MarketDataDepthAuditor(repo).audit("XAUUSD")
    m5 = next(item for item in report.timeframes if item.timeframe == "M5")

    assert m5.validation_issue_count == 1
    assert m5.unconfirmed_gap_count == 1
    assert m5.unknown_volume_candle_count == 1
    assert m5.ingestion_status == "LIMITED"
    assert any("unknown volume source" in item for item in m5.readiness_limitations)
