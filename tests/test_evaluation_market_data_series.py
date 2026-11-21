from datetime import datetime, timedelta, timezone

import pytest

from backend.database import Database
from backend.database_models import (
    DatabaseBase,
    MarketCandleRecord,
    MarketDataBatchRecord,
)
from backend.evaluation.market_data_series import (
    EvaluationCandleSeriesReader,
    EvaluationMarketDataRepository,
)


UTC = timezone.utc
BASE = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)


@pytest.fixture
def evaluation_repository():
    database = Database("sqlite:///:memory:")
    database.initialize()
    DatabaseBase.metadata.create_all(database.engine)

    with database.session() as session:
        for index, minute in enumerate((0, 5, 15), start=1):
            event_time = BASE + timedelta(minutes=minute)
            available_at = event_time + timedelta(seconds=30)
            batch = MarketDataBatchRecord(
                symbol="XAUUSD",
                timeframe="M5",
                source="MT5",
                received_at=available_at,
                candle_count=1,
                created_at=available_at + timedelta(seconds=1),
                clock_verified=True,
                clock_account_login=123456,
                clock_broker_server="Test-Broker",
                validation_report={
                    "analysis_eligible": True,
                    "freshness_verified": True,
                    "issues": [],
                    "gaps": [],
                },
            )
            session.add(batch)
            session.flush()
            session.add(
                MarketCandleRecord(
                    batch_id=batch.id,
                    symbol="XAUUSD",
                    timeframe="M5",
                    timestamp=event_time,
                    open_price=2000.0,
                    high_price=2002.0,
                    low_price=1999.0,
                    close_price=2001.0,
                    tick_volume=100,
                    real_volume=None,
                    volume_source="TICK",
                    is_closed=True,
                    quality="VERIFIED",
                )
            )

    yield EvaluationMarketDataRepository(database)
    database.dispose()


def test_range_reader_returns_chronological_candles_and_exposes_gap(
    evaluation_repository,
):
    reader = EvaluationCandleSeriesReader(evaluation_repository)

    evidence = reader.load_series(
        instrument="XAUUSD",
        timeframe="M5",
        start_at=BASE,
        end_at=BASE + timedelta(minutes=20),
        available_at_cutoff=BASE + timedelta(minutes=20),
    )

    assert [item.event_time for item in evidence.candles] == [
        BASE,
        BASE + timedelta(minutes=5),
        BASE + timedelta(minutes=15),
    ]
    assert len(evidence.gaps) == 1
    assert evidence.gaps[0].actual_interval_seconds == 600
    assert evidence.gaps[0].missing_intervals == 1
    assert evidence.is_contiguous is False
    assert evidence.duplicate_event_times == ()


def test_range_reader_rejects_data_received_after_input_cutoff(
    evaluation_repository,
):
    reader = EvaluationCandleSeriesReader(evaluation_repository)

    with pytest.raises(ValueError, match="available after"):
        reader.load_series(
            instrument="XAUUSD",
            timeframe="M5",
            start_at=BASE,
            end_at=BASE + timedelta(minutes=20),
            available_at_cutoff=BASE + timedelta(minutes=5),
        )


def test_range_reader_returns_empty_range_as_incomplete(
    evaluation_repository,
):
    reader = EvaluationCandleSeriesReader(evaluation_repository)

    evidence = reader.load_series(
        instrument="XAUUSD",
        timeframe="M15",
        start_at=BASE,
        end_at=BASE + timedelta(minutes=30),
        available_at_cutoff=BASE + timedelta(minutes=30),
    )

    assert evidence.candles == ()
    assert evidence.is_contiguous is False
    assert evidence.latest_available_at is None


def test_range_reader_rejects_non_utc_boundaries(evaluation_repository):
    reader = EvaluationCandleSeriesReader(evaluation_repository)

    with pytest.raises(ValueError, match="UTC-aware"):
        reader.load_series(
            instrument="XAUUSD",
            timeframe="M5",
            start_at=BASE.replace(tzinfo=None),
            end_at=BASE + timedelta(minutes=20),
            available_at_cutoff=BASE + timedelta(minutes=20),
        )
