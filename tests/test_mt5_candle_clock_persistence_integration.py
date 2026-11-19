from __future__ import annotations

from datetime import datetime, timezone
from math import floor
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient
import pytest

import backend.main as main
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
            "build": 6316,
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
    monkeypatch,
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

        # Require a valid independent broker-offset confirmation.
        confirmation = SimpleNamespace(
            event_type="CONFIRMATION",
            offset_seconds=BROKER_OFFSET_SECONDS,
            evidence_sample_count=3,
            evidence_samples=({}, {}, {}),
            ntp_evidence={
                "verified": True,
                "sample_count": 3,
                "servers": [
                    "ntp-a",
                    "ntp-b",
                    "ntp-c",
                ],
            },
        )

        registry = Mock()
        registry.latest.return_value = confirmation

        adapter.set_broker_offset_registry(registry)

        registry.latest.assert_not_called()

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

            # The production API must also reject candle requests
            # after independent NTP trust has been lost.
            monkeypatch.setattr(
                main,
                "get_settings",
                lambda: SimpleNamespace(
                    APP_NAME="GOLD BOT",
                    DEFAULT_TIMEFRAME="M5",
                    MT5_SYMBOL="XAUUSD",
                ),
            )

            application = main.create_app()
            application.state.market_data_service = service
            application.state.market_data_repository = repository

            # Exercise the actual FastAPI HTTP request pipeline.
            # Override authentication only for this isolated test.
            application.dependency_overrides[
                main.verify_api_token
            ] = lambda: None

            client = TestClient(application)

            response = client.get(
                "/api/v1/market-data/candles",
                params={
                    "symbol": "XAUUSD",
                    "timeframe": "M5",
                    "count": 2,
                },
            )

            assert response.status_code == 503
            assert response.json()["detail"] == (
                "Market data is currently unavailable."
            )

            client.close()

            # The API must not persist anything after the clock
            # verification has been invalidated.
            history_after_api_rejection = repository.get_history(
                symbol="XAUUSD",
                timeframe="M5",
                count=2,
            )

            assert len(history_after_api_rejection) == 2

            assert [
                int(candle.timestamp.timestamp())
                for candle in history_after_api_rejection
            ] == expected_utc_epochs

        finally:
            database.dispose()

    finally:
        adapter.shutdown()


def test_mt5_disconnect_reconnect_recalibrates_before_candle_fetch(
    monkeypatch,
):
    from unittest.mock import patch

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

    calibrator = BrokerCandleClockCalibrator(
        verifier,
        required_samples=3,
        minimum_span_seconds=3.0,
        maximum_spread_seconds=0.5,
        maximum_sample_age_seconds=30.0,
    )

    raw_closed_timestamps = []
    mt5 = make_mt5_module(raw_closed_timestamps)

    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    try:
        # 1. Initial MT5 connection.
        connection = adapter.connect()
        assert connection.state == MT5ConnectionState.CONNECTED

        adapter.set_candle_clock_calibrator(calibrator)

        confirmation = SimpleNamespace(
            event_type="CONFIRMATION",
            offset_seconds=BROKER_OFFSET_SECONDS,
            evidence_sample_count=3,
            evidence_samples=({}, {}, {}),
            ntp_evidence={
                "verified": True,
                "sample_count": 3,
                "servers": ["ntp-a", "ntp-b", "ntp-c"],
            },
        )

        registry = Mock()
        registry.latest.return_value = confirmation
        adapter.set_broker_offset_registry(registry)

        # 2. Initial calibration reaches VERIFIED.
        with patch(
            "backend.market_data.broker_candle_clock.monotonic",
            side_effect=(100.0, 102.0, 104.0),
        ):
            for _ in range(3):
                sample = adapter.get_active_candle_clock_sample(
                    "XAUUSD"
                )
                snapshot = calibrator.observe(**sample)

        assert snapshot.status == "VERIFIED"
        assert snapshot.sample_count == 3
        assert snapshot.offset_seconds == BROKER_OFFSET_SECONDS

        # 3. More than 30 seconds pass while MT5 is still connected.
        # The expired calibration must block candle retrieval first.
        with patch(
            "backend.market_data.broker_candle_clock.monotonic",
            return_value=140.0,
        ):
            with pytest.raises(
                MT5AdapterError,
                match="Candle clock is not verified",
            ):
                adapter.get_closed_candles("XAUUSD", "M5", 2)

            stale_snapshot = calibrator.snapshot()

        assert stale_snapshot.status == "DEGRADED"
        assert stale_snapshot.sample_count == 0
        assert stale_snapshot.offset_seconds is None

        # 4. Now simulate MT5 disconnection.
        mt5.terminal_info.return_value.connected = False

        disconnected = adapter.connection_status()
        assert disconnected.state == MT5ConnectionState.DISCONNECTED

        # No closed-candle request was made while disconnected.
        assert not any(
            call.args[2] == 1
            for call in mt5.copy_rates_from_pos.call_args_list
        )

        # 5. Restore MT5 connection.
        mt5.terminal_info.return_value.connected = True

        reconnected = adapter.connect()
        assert reconnected.state == MT5ConnectionState.CONNECTED

        # 6. Collect entirely fresh samples after reconnect.
        with patch(
            "backend.market_data.broker_candle_clock.monotonic",
            side_effect=(141.0, 143.0, 145.0),
        ):
            for _ in range(3):
                sample = adapter.get_active_candle_clock_sample(
                    "XAUUSD"
                )
                snapshot = calibrator.observe(**sample)

        assert snapshot.status == "VERIFIED"
        assert snapshot.sample_count == 3
        assert snapshot.offset_seconds == BROKER_OFFSET_SECONDS

        # 7. Candle fetching is allowed again only after fresh verification.
        batch = adapter.get_closed_candles("XAUUSD", "M5", 2)

        assert len(batch.candles) == 2
        assert all(candle.is_closed for candle in batch.candles)
        assert batch.clock_evidence is not None
        assert batch.clock_evidence.verified is True

    finally:
        adapter.shutdown()
