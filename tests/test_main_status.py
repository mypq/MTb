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


def create_status_endpoint(monkeypatch, chrony_status):
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

    route = next(
        route
        for route in application.routes
        if getattr(route, "path", None) == "/api/v1/system/status"
    )

    return route.endpoint


def test_system_status_includes_chrony_and_mt5_information(
    monkeypatch,
):
    chrony_status = SimpleNamespace(
        available=True,
        synchronized=True,
        reference_id="A29FC87B",
        stratum=4,
        leap_status="Normal",
        last_offset_seconds=0.0001,
        root_dispersion_seconds=0.002,
        selected_source="time.cloudflare.com",
        error=None,
    )

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
        "error": None,
        "broker_time_verified": False,
    }


def test_system_status_reports_unavailable_chrony(
    monkeypatch,
):
    chrony_status = SimpleNamespace(
        available=False,
        synchronized=False,
        reference_id=None,
        stratum=None,
        leap_status="Not synchronised",
        last_offset_seconds=None,
        root_dispersion_seconds=None,
        selected_source=None,
        error="Chrony command timed out.",
    )

    endpoint = create_status_endpoint(monkeypatch, chrony_status)
    result = asyncio.run(endpoint())

    assert result["time_sync"]["available"] is False
    assert result["time_sync"]["synchronized"] is False
    assert result["time_sync"]["selected_source"] is None
    assert result["time_sync"]["error"] == "Chrony command timed out."
    assert result["time_sync"]["broker_time_verified"] is False
