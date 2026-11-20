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
from backend.market_data.broker_offset_detector import BrokerOffsetDetector
from backend.market_data.broker_offset_registry import BrokerOffsetRegistry
from backend.market_data.clock_history import MT5ClockHistorySample

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
from backend.mt5.monitor import MT5ConnectionMonitor


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



def make_real_offset_decision(
    *,
    offset_seconds: int,
    first_gmt_epoch: int,
    identity: tuple[int, str, str, int],
    run_id: str,
):
    verifier = NtpTimeVerifier(
        servers=("ntp-a", "ntp-b", "ntp-c"),
        wall_clock=lambda: datetime.now(UTC).timestamp(),
        monotonic_clock=lambda: 100.0,
        query=lambda server: NtpSample(
            server=server,
            offset_seconds=0.0,
            round_trip_seconds=0.02,
            root_dispersion_seconds=0.01,
        ),
    )
    assert verifier.verify().verified is True

    detector = BrokerOffsetDetector(verifier)
    decision = None

    for sequence in range(1, 4):
        gmt_epoch = first_gmt_epoch + (sequence - 1) * 3
        sample = MT5ClockHistorySample(
            schema_version=1,
            run_id=run_id,
            sample_sequence=sequence,
            monotonic_ms=1000 + (sequence - 1) * 3000,
            time_gmt_epoch=gmt_epoch,
            time_trade_server_epoch=gmt_epoch + offset_seconds,
            time_current_quote_epoch=gmt_epoch + offset_seconds,
            time_local_epoch=gmt_epoch,
            server_utc_offset_seconds=offset_seconds,
            account_login=identity[0],
            broker_server=identity[1],
            symbol=identity[2],
            terminal_build=identity[3],
            terminal_connected=True,
        )
        decision = detector.observe(sample)

    assert decision is not None
    assert decision.candidate_ready is True
    assert decision.offset_seconds == offset_seconds
    return decision


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

    # Use two genuine registry records with different historical offsets.
    # The two M5 candle opens are 5 minutes apart and each falls inside
    # its own independently verified evidence interval.
    now_epoch = int(datetime.now(UTC).timestamp())
    latest_closed_utc = (now_epoch // 300) * 300 - 300
    first_candle_utc = latest_closed_utc - 300
    second_candle_utc = latest_closed_utc
    identity = (123456, "Demo-Server", "XAUUSD", 6316)

    registry = BrokerOffsetRegistry(
        tmp_path / "real-broker-offsets.jsonl"
    )
    baseline = registry.append_candidate(
        make_real_offset_decision(
            offset_seconds=7200,
            first_gmt_epoch=first_candle_utc - 3,
            identity=identity,
            run_id="integration-baseline",
        ),
        recorded_at_utc=datetime.now(UTC),
    )
    offset_change = registry.append_candidate(
        make_real_offset_decision(
            offset_seconds=3600,
            first_gmt_epoch=second_candle_utc - 3,
            identity=identity,
            run_id="integration-offset-change",
        ),
        recorded_at_utc=datetime.now(UTC) + timedelta(seconds=1),
    )
    assert baseline.event_type == "BASELINE"
    assert offset_change.event_type == "OFFSET_CHANGE"

    original_copy_rates = mt5.copy_rates_from_pos.side_effect

    def copy_rates_with_historical_offsets(symbol, timeframe, position, count):
        if timeframe == mt5.TIMEFRAME_M5 and position == 1:
            rates = [
                make_rate(first_candle_utc + 7200),
                make_rate(second_candle_utc + 3600),
            ]
            raw_closed_timestamps.extend(
                [first_candle_utc + 7200, second_candle_utc + 3600]
            )
            return rates
        return original_copy_rates(symbol, timeframe, position, count)

    mt5.copy_rates_from_pos.side_effect = copy_rates_with_historical_offsets

    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    connection = adapter.connect()
    assert connection.state == MT5ConnectionState.CONNECTED

    base_monotonic = 1000.0
    monotonic_values = iter((
        base_monotonic,
        base_monotonic + 0.3,
        base_monotonic + 0.6,
    ))

    def stable_monotonic():
        try:
            return next(monotonic_values)
        except StopIteration:
            return base_monotonic + 0.6

    monotonic_patcher = patch(
        "backend.market_data.broker_candle_clock.monotonic",
        side_effect=stable_monotonic,
    )
    monotonic_patcher.start()

    try:
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

        # The adapter must resolve each raw candle timestamp against
        # the actual append-only registry, not a mocked latest offset.
        adapter.set_broker_offset_registry(registry)

        assert registry.for_broker_times(
            identity,
            (
                first_candle_utc + 7200,
                second_candle_utc + 3600,
            ),
        ) == (baseline, offset_change)

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
                (first_candle_utc, second_candle_utc)
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

            # Both candles were converted with their own historical
            # offsets, including the offset-change record.
            assert int(stored_candles[0].timestamp.timestamp()) == (
                first_candle_utc
            )
            assert int(stored_candles[1].timestamp.timestamp()) == (
                second_candle_utc
            )

            # The API serializes the same UTC instants without applying
            # another broker-offset conversion.
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
            application.dependency_overrides[
                main.verify_api_token
            ] = lambda: None
            client = TestClient(application)

            api_response = client.get(
                "/api/v1/market-data/candles",
                params={
                    "symbol": "XAUUSD",
                    "timeframe": "M5",
                    "count": 2,
                },
            )
            assert api_response.status_code == 200
            api_timestamps = [
                datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00"))
                for item in api_response.json()["batch"]["candles"]
            ]
            assert [
                int(value.timestamp()) for value in api_timestamps
            ] == expected_utc_epochs
            assert all(value.utcoffset().total_seconds() == 0 for value in api_timestamps)
            client.close()

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
        monotonic_patcher.stop()
        adapter.shutdown()


def test_mt5_disconnect_reconnect_recalibrates_before_candle_fetch(
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

    assert verifier.verify().verified is True

    calibrator = BrokerCandleClockCalibrator(
        verifier,
        required_samples=3,
        minimum_span_seconds=3.0,
        maximum_spread_seconds=0.5,
        maximum_sample_age_seconds=30.0,
    )

    raw_closed_timestamps = []
    mt5 = make_mt5_module(raw_closed_timestamps)

    # The fake terminal becomes connected only when initialize()
    # is called, exactly as it should during monitor recovery.
    terminal = mt5.terminal_info.return_value

    def initialize_after_disconnect(**kwargs):
        terminal.connected = True
        return True

    mt5.initialize.side_effect = initialize_after_disconnect

    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    try:
        # 1. Establish the initial MT5 connection.
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
        registry.for_broker_times.side_effect = (
            lambda identity, timestamps: tuple(
                confirmation for _ in timestamps
            )
        )
        adapter.set_broker_offset_registry(registry)

        # 2. Obtain a verified initial calibration.
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

        # 3. Disconnect MT5 while the calibration is still fresh.
        terminal.connected = False

        disconnected = adapter.connection_status()
        assert disconnected.state == MT5ConnectionState.DISCONNECTED

        # 4. Run the real connection monitor.
        # It must invalidate calibration before reconnecting.
        monitor = MT5ConnectionMonitor(adapter)

        reconnected = monitor.check_once()

        assert reconnected.state == MT5ConnectionState.CONNECTED
        mt5.initialize.assert_called()

        invalidated_snapshot = calibrator.snapshot()

        assert invalidated_snapshot.status == "DEGRADED"
        assert invalidated_snapshot.sample_count == 0
        assert invalidated_snapshot.offset_seconds is None
        assert "connection was lost" in invalidated_snapshot.reason.lower()

        # 5. A connected terminal must still be blocked from
        # retrieving closed candles until fresh calibration completes.
        assert adapter.connection_status().state == (
            MT5ConnectionState.CONNECTED
        )

        closed_fetch_calls_before = [
            call
            for call in mt5.copy_rates_from_pos.call_args_list
            if call.args[2] == 1
        ]

        with pytest.raises(
            MT5AdapterError,
            match="Candle clock is not verified",
        ):
            adapter.get_closed_candles("XAUUSD", "M5", 2)

        closed_fetch_calls_after = [
            call
            for call in mt5.copy_rates_from_pos.call_args_list
            if call.args[2] == 1
        ]

        assert closed_fetch_calls_after == closed_fetch_calls_before

        # 6. Collect three entirely fresh samples after reconnection.
        with patch(
            "backend.market_data.broker_candle_clock.monotonic",
            side_effect=(110.0, 112.0, 114.0),
        ):
            for _ in range(3):
                sample = adapter.get_active_candle_clock_sample(
                    "XAUUSD"
                )
                snapshot = calibrator.observe(**sample)

        assert snapshot.status == "VERIFIED"
        assert snapshot.sample_count == 3
        assert snapshot.offset_seconds == BROKER_OFFSET_SECONDS

        # 7. Closed-candle retrieval is allowed only after
        # the new calibration and trusted evidence are available.
        with patch(
            "backend.market_data.broker_candle_clock.monotonic",
            return_value=114.0,
        ):
            batch = adapter.get_closed_candles(
                "XAUUSD",
                "M5",
                2,
            )

        assert len(batch.candles) == 2
        assert all(candle.is_closed for candle in batch.candles)
        assert batch.clock_evidence is not None
        assert batch.clock_evidence.verified is True

        assert len([
            call
            for call in mt5.copy_rates_from_pos.call_args_list
            if call.args[2] == 1
        ]) > len(closed_fetch_calls_before)

    finally:
        adapter.shutdown()


def test_main_candle_clock_worker_recalibrates_after_mt5_reconnect():
    import asyncio

    from backend.market_data.broker_candle_clock import monotonic

    verifier = NtpTimeVerifier(
        servers=("ntp-a", "ntp-b", "ntp-c"),
        query=lambda server: NtpSample(
            server=server,
            offset_seconds=0.0,
            round_trip_seconds=0.02,
            root_dispersion_seconds=0.01,
        ),
    )

    assert verifier.verify().verified is True

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

    connection = adapter.connect()
    assert connection.state == MT5ConnectionState.CONNECTED

    calibration_stop = asyncio.Event()
    collector_stop = asyncio.Event()
    collector_started = asyncio.Event()

    class FakeCollector:
        async def run(self, stop_event):
            collector_started.set()
            stop_event.set()

    collector = FakeCollector()

    class FakeClock:
        value = 100.0

    clock = FakeClock()

    class MainWorkerAdapter:
        def __init__(self):
            self.calls = 0
            self.loop = None

        def get_active_candle_clock_sample(self, symbol):
            self.calls += 1

            if self.calls <= 3:
                clock.value = 100.0 + (self.calls - 1) * 2.0

            elif self.calls == 4:
                initial_snapshot = calibrator.snapshot()

                assert initial_snapshot.status == "VERIFIED"
                assert initial_snapshot.sample_count == 3

                # Simulate MT5 disconnection and stale calibration.
                clock.value = 140.0
                mt5.terminal_info.return_value.connected = False

                stale_snapshot = calibrator.snapshot()

                assert stale_snapshot.status == "DEGRADED"
                assert stale_snapshot.sample_count == 0
                assert stale_snapshot.offset_seconds is None

                return adapter.get_active_candle_clock_sample(symbol)

            elif self.calls == 5:
                clock.value = 145.0
                mt5.terminal_info.return_value.connected = True

                reconnected = adapter.connect()
                assert reconnected.state == MT5ConnectionState.CONNECTED

            elif self.calls == 6:
                clock.value = 147.0

            elif self.calls == 7:
                clock.value = 149.0
                self.loop.call_soon_threadsafe(
                    calibration_stop.set
                )

            return adapter.get_active_candle_clock_sample(symbol)

    worker_adapter = MainWorkerAdapter()

    async def run_workers():
        worker_adapter.loop = asyncio.get_running_loop()

        calibration_task = asyncio.create_task(
            main._candle_clock_calibration_loop(
                worker_adapter,
                calibrator,
                "XAUUSD",
                calibration_stop,
            )
        )

        collector_task = asyncio.create_task(
            main._run_collector_after_clock_verification(
                collector,
                calibrator,
                collector_stop,
            )
        )

        await asyncio.sleep(0.1)

        assert not collector_started.is_set()

        await asyncio.wait_for(
            asyncio.gather(
                calibration_task,
                collector_task,
            ),
            timeout=15.0,
        )

    try:
        with patch(
            "backend.market_data.broker_candle_clock.monotonic",
            side_effect=lambda: clock.value,
        ):
            asyncio.run(run_workers())

            final_snapshot = calibrator.snapshot()

        assert worker_adapter.calls == 7

        assert final_snapshot.status == "VERIFIED"
        assert final_snapshot.sample_count == 3
        assert final_snapshot.offset_seconds == BROKER_OFFSET_SECONDS

        assert collector_started.is_set()
        assert collector_stop.is_set()

    finally:
        adapter.shutdown()
