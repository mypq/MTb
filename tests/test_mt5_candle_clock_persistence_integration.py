from __future__ import annotations

from datetime import datetime, timezone
from math import floor
from unittest.mock import Mock, patch

import pytest

from backend.config import Settings
from backend.database import Database
from backend.database_models import DatabaseBase
from backend.market_data.broker_candle_clock import (
    BrokerCandleClockCalibrator,
)
from backend.market_data.collector import MarketDataCollector
from backend.market_data.ntp_time import NtpSample, NtpTimeVerifier
from backend.market_data.repository import MarketDataRepository
from backend.market_data.service import MarketDataService
from backend.market_data.validator import MarketDataValidator
from backend.mt5 import (
    MT5Adapter,
    MT5AdapterError,
    MT5ConnectionState,
)


UTC = timezone.utc

# Test fixture only. This is not a production clock setting.
BROKER_OFFSET_SECONDS = 7200


def make_settings() -> Settings:
    return Settings(
        MT5_ENABLED=True,
        MT5_TERMINAL_PATH="",
        MT5_LOGIN=None,
        MT5_PASSWORD="",
        MT5_SERVER="",
        MT5_TIMEOUT_MS=10000,
    )


def make_rate(timestamp: int) -> dict:
    return {
        "time": timestamp,
        "open": 2000.0,
        "high": 2005.0,
        "low": 1995.0,
        "close": 2002.0,
        "tick_volume": 120,
        "spread": 25,
        "real_volume": 0,
    }


def make_mt5_module(
    raw_closed_timestamps: list[int],
) -> Mock:
    mt5 = Mock()

    mt5.initialize.return_value = True
    mt5.terminal_info.return_value = type(
        "Terminal",
        (),
        {
            "connected": True,
            "trade_allowed": True,
        },
    )()

    mt5.account_info.return_value = type(
        "Account",
        (),
        {
            "login": 123456,
            "server": "Demo-Server",
            "trade_allowed": True,
        },
    )()

    mt5.TIMEFRAME_M1 = 1
    mt5.TIMEFRAME_M5 = 5

    def copy_rates(symbol, timeframe, position, count):
        now_epoch = datetime.now(UTC).timestamp()

        if timeframe == mt5.TIMEFRAME_M1 and position == 0:
            active_open = (
                floor(now_epoch / 60) * 60
                + BROKER_OFFSET_SECONDS
            )
            return [{"time": int(active_open)}]

        if timeframe == mt5.TIMEFRAME_M5 and position == 0:
            active_open = (
                floor(now_epoch / 300) * 300
                + BROKER_OFFSET_SECONDS
            )
            return [{"time": int(active_open)}]

        if timeframe == mt5.TIMEFRAME_M5 and position == 1:
            latest_closed = (
                (int(now_epoch) // 300) * 300 - 300
            )

            timestamps = [
                latest_closed - 300 + BROKER_OFFSET_SECONDS,
                latest_closed + BROKER_OFFSET_SECONDS,
            ]

            raw_closed_timestamps.extend(timestamps)

            return [
                make_rate(timestamp)
                for timestamp in timestamps
            ]

        raise AssertionError(
            f"Unexpected MT5 request: {timeframe}/{position}/{count}"
        )

    mt5.copy_rates_from_pos.side_effect = copy_rates
    mt5.last_error.return_value = (1, "Test error")

    return mt5


def test_mt5_candle_clock_is_calibrated_converted_and_persisted_as_utc(
    tmp_path,
):
    verifier = NtpTimeVerifier(
        servers=("ntp-a", "ntp-b", "ntp-c"),
        query=lambda server: NtpSample(
            server=server,
            offset_seconds=0.0,
            round_trip_seconds=0.02,
            root_dispersion_seconds=0.01,
        ),
    )

    ntp_result = verifier.verify()
    assert ntp_result.verified is True
    assert ntp_result.sample_count == 3

    calibrator = BrokerCandleClockCalibrator(
        verifier,
        required_samples=3,
        minimum_span_seconds=0.5,
        maximum_spread_seconds=0.5,
        maximum_sample_age_seconds=30.0,
    )

    raw_closed_timestamps: list[int] = []
    mt5 = make_mt5_module(raw_closed_timestamps)

    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    connection = adapter.connect()
    assert connection.state == MT5ConnectionState.CONNECTED

    base_monotonic = 1000.0

    try:
        with patch(
            "backend.market_data.broker_candle_clock.monotonic",
            side_effect=(
                base_monotonic,
                base_monotonic + 0.3,
                base_monotonic + 0.6,
            ),
        ):
            snapshot = None

            for _ in range(3):
                sample = adapter.get_active_candle_clock_sample(
                    "XAUUSD"
                )

                snapshot = calibrator.observe(**sample)

        assert snapshot is not None
        assert snapshot.status == "VERIFIED"
        assert snapshot.offset_seconds == BROKER_OFFSET_SECONDS
        assert snapshot.sample_count == 3

        adapter.set_candle_clock_calibrator(calibrator)

        database = Database(
            f"sqlite:///{(tmp_path / 'candle_utc.db').as_posix()}"
        )
        database.initialize()
        DatabaseBase.metadata.create_all(database.engine)

        try:
            repository = MarketDataRepository(database)

            service = MarketDataService(
                source=adapter,
                validator=MarketDataValidator(),
            )

            collector = MarketDataCollector(
                service=service,
                repository=repository,
                symbol="XAUUSD",
                timeframe="M5",
                count=2,
            )

            save_result = collector.collect_once()

            assert save_result is not None
            assert save_result.inserted_candles == 2
            assert save_result.duplicate_candles == 0

            stored_candles = repository.get_history(
                symbol="XAUUSD",
                timeframe="M5",
                count=2,
            )

            assert len(stored_candles) == 2
            assert len(raw_closed_timestamps) == 2

            expected_utc_epochs = sorted(
                raw_timestamp - BROKER_OFFSET_SECONDS
                for raw_timestamp in raw_closed_timestamps
            )

            stored_utc_epochs = []

            for candle in stored_candles:
                assert candle.timestamp.tzinfo is not None
                assert candle.timestamp.utcoffset() == (
                    datetime.now(UTC).utcoffset()
                )

                stored_utc_epochs.append(
                    int(candle.timestamp.timestamp())
                )

            # Compare the original raw MT5 timestamps directly
            # with the timestamps actually persisted in the database.
            assert stored_utc_epochs == expected_utc_epochs

            assert stored_candles[0].timestamp < (
                stored_candles[1].timestamp
            )

            # Losing independent NTP trust must block further
            # candle collection and prevent additional persistence.
            verifier.reset()

            with pytest.raises(
                MT5AdapterError,
                match="Candle clock is not verified",
            ):
                collector.collect_once()

            # Previously persisted candles must remain unchanged.
            history_after_ntp_loss = repository.get_history(
                symbol="XAUUSD",
                timeframe="M5",
                count=2,
            )

            assert len(history_after_ntp_loss) == 2

            assert [
                int(candle.timestamp.timestamp())
                for candle in history_after_ntp_loss
            ] == expected_utc_epochs

        finally:
            database.dispose()

    finally:
        adapter.shutdown()
