from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from backend.config import Settings
from backend.mt5 import (
    MT5Adapter,
    MT5AdapterError,
    MT5ConnectionState,
)


def make_settings(**overrides):
    values = {
        "MT5_ENABLED": True,
        "MT5_TERMINAL_PATH": "",
        "MT5_LOGIN": None,
        "MT5_PASSWORD": "",
        "MT5_SERVER": "",
        "MT5_TIMEOUT_MS": 10000,
    }
    values.update(overrides)
    return Settings(**values)


def make_mt5_module(
    initialized=True,
    connected=True,
):
    terminal = SimpleNamespace(
        connected=connected,
        trade_allowed=True,
    )

    account = SimpleNamespace(
        login=123456,
        server="Demo-Server",
        currency="USD",
        balance=1000.0,
        equity=1010.0,
        margin=20.0,
        margin_free=990.0,
        leverage=100,
        trade_allowed=True,
        trade_expert=True,
    )

    symbol = SimpleNamespace(
        name="XAUUSD",
        description="Gold",
        digits=2,
        point=0.01,
        trade_contract_size=100.0,
        trade_tick_size=0.01,
        trade_tick_value=1.0,
        volume_min=0.01,
        volume_max=100.0,
        volume_step=0.01,
        spread=25,
        visible=True,
    )

    mt5 = Mock()
    mt5.initialize.return_value = initialized
    mt5.terminal_info.return_value = terminal
    mt5.account_info.return_value = account
    mt5.symbol_info.return_value = symbol
    mt5.last_error.return_value = (1, "Test error")

    return mt5


def test_disabled_mt5_does_not_initialize():
    mt5 = make_mt5_module()
    settings = make_settings(MT5_ENABLED=False)

    adapter = MT5Adapter(settings=settings, mt5_module=mt5)

    status = adapter.connect()

    assert status.state == MT5ConnectionState.DISABLED
    mt5.initialize.assert_not_called()


def test_successful_connection():
    mt5 = make_mt5_module()
    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    status = adapter.connect()

    assert status.state == MT5ConnectionState.CONNECTED
    assert status.terminal_connected is True
    mt5.initialize.assert_called_once()


def test_terminal_disconnected_is_not_reported_as_connected():
    mt5 = make_mt5_module(connected=False)
    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    status = adapter.connect()

    assert status.state == MT5ConnectionState.DISCONNECTED
    assert status.terminal_connected is False


def test_initialization_failure():
    mt5 = make_mt5_module(initialized=False)
    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    status = adapter.connect()

    assert status.state == MT5ConnectionState.DISCONNECTED
    assert "Test error" in status.message


def test_account_information_mapping():
    mt5 = make_mt5_module()
    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    adapter.connect()
    account = adapter.get_account_info()

    assert account.login == 123456
    assert account.currency == "USD"
    assert account.balance == 1000.0
    assert account.free_margin == 990.0


def test_symbol_information_mapping():
    mt5 = make_mt5_module()
    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    adapter.connect()
    symbol = adapter.get_symbol_info("XAUUSD")

    assert symbol.name == "XAUUSD"
    assert symbol.digits == 2
    assert symbol.point == 0.01
    assert symbol.volume_step == 0.01


def test_empty_symbol_is_rejected():
    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=make_mt5_module(),
    )

    with pytest.raises(ValueError):
        adapter.get_symbol_info("   ")


def test_symbol_unavailable():
    mt5 = make_mt5_module()
    mt5.symbol_info.return_value = None

    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    adapter.connect()

    with pytest.raises(MT5AdapterError):
        adapter.get_symbol_info("UNKNOWN")


def test_shutdown_only_after_adapter_initialization():
    mt5 = make_mt5_module()
    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    adapter.shutdown()
    mt5.shutdown.assert_not_called()

    adapter.connect()
    adapter.shutdown()

    mt5.shutdown.assert_called_once()


def test_shutdown_resets_connection_state():
    mt5 = make_mt5_module()
    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    adapter.connect()
    adapter.shutdown()

    status = adapter.connection_status()

    assert status.state == MT5ConnectionState.DISCONNECTED


def test_repeated_connect_does_not_initialize_twice():
    mt5 = make_mt5_module()
    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    adapter.connect()
    adapter.connect()

    mt5.initialize.assert_called_once()
