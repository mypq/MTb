import asyncio
from types import SimpleNamespace

import backend.main as main


class FakeAdapter:
    def connection_status(self):
        return SimpleNamespace(
            model_dump=lambda mode: {
                "connected": False,
                "terminal_connected": False,
            }
        )


class FakeMonitor:
    @property
    def is_running(self):
        return False


class FakeChronyStatusService:
    def __init__(self, status):
        self.status = status

    def snapshot(self):
        return self.status


class FakeNtpVerificationMonitor:
    def snapshot(self):
        return {
            "provider": "external_ntp_udp_123",
            "trusted": False,
            "verification_interval_seconds": 3600.0,
            "last_attempt": None,
            "last_attempt_at_unix": None,
            "last_success": None,
            "last_error": None,
        }


class FakeBrokerTickWorker:
    def __init__(self, status):
        self._status = status
        self.status_calls = 0

    def status(self):
        self.status_calls += 1
        return self._status


class FakeBrokerCandleClockCalibrator:
    def __init__(self, snapshot):
        self._snapshot = snapshot
        self.snapshot_calls = 0

    def snapshot(self):
        self.snapshot_calls += 1
        return self._snapshot


def create_status_endpoint(
    monkeypatch,
    chrony_status,
    broker_tick_worker=None,
    broker_candle_clock_calibrator=None,
):
    settings = SimpleNamespace(
        APP_NAME="GOLD_BOT",
        APP_ENV="testing",
        BOT_MODE="ANALYSIS_ONLY",
        TRADING_ENABLED=False,
        LIVE_TRADING_AUTHORIZED=False,
        EMERGENCY_STOP=True,
        trading_must_be_blocked=True,
        live_trading_must_be_blocked=True,
        MT5_ENABLED=False,
        DEFAULT_TIMEFRAME="M5",
        MT5_SYMBOL="",
    )

    monkeypatch.setattr(main, "get_settings", lambda: settings)

    application = main.create_app()
    application.state.mt5_adapter = FakeAdapter()
    application.state.mt5_monitor = FakeMonitor()
    application.state.chrony_status_service = (
        FakeChronyStatusService(chrony_status)
    )
    application.state.ntp_verification_monitor = (
        FakeNtpVerificationMonitor()
    )
    application.state.broker_tick_worker = broker_tick_worker
    application.state.broker_candle_clock_calibrator = (
        broker_candle_clock_calibrator
    )

    route = next(
        route
        for route in application.routes
        if getattr(route, "path", None) == "/api/v1/system/status"
    )

    return route.endpoint


def make_chrony_status(
    *,
    available=True,
    synchronized=True,
    error=None,
):
    return SimpleNamespace(
        available=available,
        synchronized=synchronized,
        reference_id="A29FC87B" if available else None,
        stratum=4 if available else None,
        leap_status="Normal" if available else None,
        last_offset_seconds=0.0001 if available else None,
        root_dispersion_seconds=0.002 if available else None,
        selected_source=(
            "time.cloudflare.com" if available else None
        ),
        healthy_sources=3 if available else 0,
        nts_authenticated_sources=3 if available else 0,
        quorum_met=available,
        error=error,
    )


def test_system_status_includes_chrony_and_mt5_information(
    monkeypatch,
):
    chrony_status = make_chrony_status()

    endpoint = create_status_endpoint(monkeypatch, chrony_status)
    result = asyncio.run(endpoint())

    assert result["application"] == "GOLD_BOT"
    assert result["mode"] == "ANALYSIS_ONLY"
    assert result["trading_enabled"] is False
    assert result["trading_blocked"] is True

    assert result["mt5_enabled"] is False
    assert result["mt5_connection"] == {
        "connected": False,
        "terminal_connected": False,
    }
    assert result["mt5_monitor_running"] is False

    assert result["time_sync"] == {
        "provider": "chrony",
        "available": True,
        "synchronized": True,
        "reference_id": "A29FC87B",
        "stratum": 4,
        "leap_status": "Normal",
        "last_offset_seconds": 0.0001,
        "root_dispersion_seconds": 0.002,
        "selected_source": "time.cloudflare.com",
        "healthy_sources": 3,
        "nts_authenticated_sources": 3,
        "quorum_met": True,
        "error": None,
        "broker_time_verified": False,
    }


def test_system_status_reports_unavailable_chrony(
    monkeypatch,
):
    chrony_status = make_chrony_status(
        available=False,
        synchronized=False,
        error="Chrony command timed out.",
    )

    endpoint = create_status_endpoint(monkeypatch, chrony_status)
    result = asyncio.run(endpoint())

    assert result["time_sync"]["available"] is False
    assert result["time_sync"]["synchronized"] is False
    assert result["time_sync"]["selected_source"] is None
    assert result["time_sync"]["healthy_sources"] == 0
    assert result["time_sync"]["nts_authenticated_sources"] == 0
    assert result["time_sync"]["quorum_met"] is False
    assert result["time_sync"]["error"] == "Chrony command timed out."
    assert result["time_sync"]["broker_time_verified"] is False


def test_system_status_preserves_all_trading_safety_flags(
    monkeypatch,
):
    chrony_status = make_chrony_status()

    endpoint = create_status_endpoint(monkeypatch, chrony_status)
    result = asyncio.run(endpoint())

    assert result["mode"] == "ANALYSIS_ONLY"
    assert result["trading_enabled"] is False
    assert result["live_trading_authorized"] is False
    assert result["emergency_stop"] is True
    assert result["trading_blocked"] is True
    assert result["live_trading_blocked"] is True
    assert result["mt5_enabled"] is False


def test_system_status_never_claims_broker_time_verified(
    monkeypatch,
):
    chrony_status = make_chrony_status()

    endpoint = create_status_endpoint(monkeypatch, chrony_status)
    result = asyncio.run(endpoint())

    assert result["time_sync"]["synchronized"] is True
    assert result["time_sync"]["broker_time_verified"] is False


def test_system_status_reports_mt5_monitor_state(monkeypatch):
    chrony_status = make_chrony_status(
        available=False,
        synchronized=False,
        error="Unavailable",
    )

    endpoint = create_status_endpoint(monkeypatch, chrony_status)
    result = asyncio.run(endpoint())

    assert result["mt5_monitor_running"] is False
    assert result["mt5_connection"]["connected"] is False
    assert result["mt5_enabled"] is False


def test_system_status_reports_default_broker_tick_state(
    monkeypatch,
):
    chrony_status = make_chrony_status()

    endpoint = create_status_endpoint(monkeypatch, chrony_status)
    result = asyncio.run(endpoint())

    assert result["broker_tick_monitor"] == {
        "running": False,
        "state": "NO_DATA",
        "clock_verification": "UNVERIFIED",
        "symbol": None,
        "account_login": None,
        "broker_server": None,
        "last_broker_time_msc": None,
        "seconds_since_progress": None,
        "samples_received": 0,
        "timestamp_advances": 0,
        "repeated_timestamps": 0,
        "timestamp_regressions": 0,
        "identity_changes": 0,
        "last_error": None,
    }


def test_system_status_returns_live_broker_tick_worker_status(
    monkeypatch,
):
    chrony_status = make_chrony_status()

    expected_status = {
        "running": True,
        "state": "HEALTHY",
        "clock_verification": "UNVERIFIED",
        "symbol": "XAUUSD",
        "account_login": 123456,
        "broker_server": "TestServer",
        "last_broker_time_msc": 1794708000000,
        "seconds_since_progress": 0.5,
        "samples_received": 10,
        "timestamp_advances": 9,
        "repeated_timestamps": 1,
        "timestamp_regressions": 0,
        "identity_changes": 0,
        "last_error": None,
    }

    worker = FakeBrokerTickWorker(expected_status)

    endpoint = create_status_endpoint(
        monkeypatch,
        chrony_status,
        broker_tick_worker=worker,
    )

    result = asyncio.run(endpoint())

    assert result["broker_tick_monitor"] == expected_status
    assert worker.status_calls == 1
    assert result["time_sync"]["broker_time_verified"] is False


def test_system_status_preserves_broker_tick_worker_errors(
    monkeypatch,
):
    chrony_status = make_chrony_status()

    expected_status = {
        "running": True,
        "state": "NO_DATA",
        "clock_verification": "UNVERIFIED",
        "symbol": "XAUUSD",
        "account_login": None,
        "broker_server": None,
        "last_broker_time_msc": None,
        "seconds_since_progress": None,
        "samples_received": 0,
        "timestamp_advances": 0,
        "repeated_timestamps": 0,
        "timestamp_regressions": 0,
        "identity_changes": 0,
        "last_error": "MT5AdapterError",
    }

    worker = FakeBrokerTickWorker(expected_status)

    endpoint = create_status_endpoint(
        monkeypatch,
        chrony_status,
        broker_tick_worker=worker,
    )

    result = asyncio.run(endpoint())

    assert result["broker_tick_monitor"]["last_error"] == (
        "MT5AdapterError"
    )
    assert result["broker_tick_monitor"]["state"] == "NO_DATA"
    assert result["broker_tick_monitor"]["clock_verification"] == (
        "UNVERIFIED"
    )
    assert worker.status_calls == 1


def test_system_status_reports_candle_clock_calibration(
    monkeypatch,
):
    chrony_status = make_chrony_status()

    expected_snapshot = SimpleNamespace(
        status="COLLECTING",
        symbol="GOLD",
        account_login=337260380,
        broker_server="XMGlobal-MT5 9",
        offset_seconds=None,
        uncertainty_seconds=None,
        sample_count=3,
        reason="Collecting stable M1/M5 boundary samples.",
    )

    calibrator = FakeBrokerCandleClockCalibrator(
        expected_snapshot
    )

    endpoint = create_status_endpoint(
        monkeypatch,
        chrony_status,
        broker_candle_clock_calibrator=calibrator,
    )

    result = asyncio.run(endpoint())

    assert result["broker_candle_clock_calibration"] == {
        "status": "COLLECTING",
        "symbol": "GOLD",
        "account_login": 337260380,
        "broker_server": "XMGlobal-MT5 9",
        "offset_seconds": None,
        "uncertainty_seconds": None,
        "sample_count": 3,
        "reason": "Collecting stable M1/M5 boundary samples.",
    }

    assert calibrator.snapshot_calls == 1

    assert (
        result["broker_tick_monitor"]["clock_verification"]
        == "UNVERIFIED"
    )

    assert result["time_sync"]["broker_time_verified"] is False


def test_system_status_reports_unavailable_candle_calibrator(
    monkeypatch,
):
    endpoint = create_status_endpoint(
        monkeypatch,
        make_chrony_status(),
    )

    result = asyncio.run(endpoint())

    assert result["broker_candle_clock_calibration"] == {
        "status": "UNVERIFIED",
        "symbol": None,
        "account_login": None,
        "broker_server": None,
        "offset_seconds": None,
        "uncertainty_seconds": None,
        "sample_count": 0,
        "reason": "Candle clock calibrator is unavailable.",
    }
