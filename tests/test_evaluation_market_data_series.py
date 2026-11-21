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
    # A timestamp gap is observable, but without a session calendar it
    # cannot be labelled as a specific number of missing market candles.
    assert evidence.gaps[0].missing_intervals is None
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


def _series_candle(candle_id: int, minute: int):
    from backend.market_data.contracts import MarketCandleDataContract

    event_time = BASE + timedelta(minutes=minute)
    available_at = event_time + timedelta(seconds=30)
    return MarketCandleDataContract(
        candle_id=candle_id,
        batch_id=candle_id,
        instrument="XAUUSD",
        timeframe="M5",
        event_time=event_time,
        available_at=available_at,
        recorded_at=available_at + timedelta(seconds=1),
        open=2000.0,
        high=2002.0,
        low=1999.0,
        close=2001.0,
        tick_volume=100,
        real_volume=None,
        volume_source="TICK",
        candle_quality="VERIFIED",
        is_closed=True,
        source="MT5",
        source_account_login=123456,
        source_broker_server="Test-Broker",
        clock_verified=True,
        analysis_eligible=True,
        validation_report={"analysis_eligible": True},
    )


class _StaticSeriesRepository:
    def __init__(self, candles):
        self._candles = tuple(candles)

    def get_candle_contracts(self, instrument, timeframe, start_at, end_at):
        return self._candles


def test_series_reader_reports_duplicate_event_times_without_hiding_them():
    reader = EvaluationCandleSeriesReader(
        _StaticSeriesRepository(
            (
                _series_candle(1, 0),
                _series_candle(2, 5),
                _series_candle(3, 5),
                _series_candle(4, 10),
            )
        )
    )

    evidence = reader.load_series(
        instrument="XAUUSD",
        timeframe="M5",
        start_at=BASE,
        end_at=BASE + timedelta(minutes=15),
        available_at_cutoff=BASE + timedelta(minutes=15),
    )

    assert evidence.duplicate_event_times == (BASE + timedelta(minutes=5),)
    assert len(evidence.candles) == 4
    assert evidence.is_contiguous is False


def test_series_reader_rejects_out_of_order_repository_results():
    from backend.evaluation.market_data import EvaluationMarketDataError

    reader = EvaluationCandleSeriesReader(
        _StaticSeriesRepository(
            (
                _series_candle(1, 0),
                _series_candle(2, 10),
                _series_candle(3, 5),
            )
        )
    )

    with pytest.raises(EvaluationMarketDataError, match="not chronological"):
        reader.load_series(
            instrument="XAUUSD",
            timeframe="M5",
            start_at=BASE,
            end_at=BASE + timedelta(minutes=15),
            available_at_cutoff=BASE + timedelta(minutes=15),
        )
