from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from backend.config import Settings
from backend.market_data import VolumeSource
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
    mt5.TIMEFRAME_M5 = 5

    return mt5


_DEFAULT_TIMESTAMP = object()


def make_rate(
    timestamp=_DEFAULT_TIMESTAMP,
    open_price=2000.0,
    high=2005.0,
    low=1995.0,
    close=2002.0,
    tick_volume=120,
    real_volume=0,
):
    if timestamp is _DEFAULT_TIMESTAMP:
        timestamp = int(
            (datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp()
        )

    return {
        "time": timestamp,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "tick_volume": tick_volume,
        "spread": 25,
        "real_volume": real_volume,
    }


def make_connected_adapter(mt5=None):
    module = mt5 or make_mt5_module()

    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=module,
    )

    status = adapter.connect()
    assert status.state == MT5ConnectionState.CONNECTED

    return adapter, module


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


def test_closed_candles_exclude_current_forming_candle():
    adapter, mt5 = make_connected_adapter()

    first_time = int(
        (datetime.now(timezone.utc) - timedelta(minutes=20)).timestamp()
    )
    second_time = int(
        (datetime.now(timezone.utc) - timedelta(minutes=15)).timestamp()
    )

    mt5.copy_rates_from_pos.return_value = [
        make_rate(timestamp=first_time),
        make_rate(timestamp=second_time),
    ]

    batch = adapter.get_closed_candles("XAUUSD", "M5", 2)

    mt5.copy_rates_from_pos.assert_called_once_with(
        "XAUUSD",
        5,
        1,
        2,
    )

    assert len(batch.candles) == 2
    assert all(candle.is_closed for candle in batch.candles)


def test_candle_timestamps_are_converted_to_utc():
    adapter, mt5 = make_connected_adapter()

    timestamp = int(
        (datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp()
    )
    mt5.copy_rates_from_pos.return_value = [
        make_rate(timestamp=timestamp),
    ]

    batch = adapter.get_closed_candles("XAUUSD", "M5", 1)
    candle = batch.candles[0]

    assert candle.timestamp.tzinfo is not None
    assert candle.timestamp.utcoffset() == timedelta(0)
    assert candle.timestamp == datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc,
    )


def test_real_volume_and_tick_volume_remain_separate():
    adapter, mt5 = make_connected_adapter()

    timestamp = int(
        (datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp()
    )
    mt5.copy_rates_from_pos.return_value = [
        make_rate(
            timestamp=timestamp,
            tick_volume=150,
            real_volume=75,
        ),
    ]

    batch = adapter.get_closed_candles("XAUUSD", "M5", 1)
    candle = batch.candles[0]

    assert candle.tick_volume == 150
    assert candle.real_volume == 75
    assert candle.volume_source == VolumeSource.REAL


def test_zero_real_volume_is_not_misrepresented_as_real_volume():
    adapter, mt5 = make_connected_adapter()

    timestamp = int(
        (datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp()
    )
    mt5.copy_rates_from_pos.return_value = [
        make_rate(
            timestamp=timestamp,
            tick_volume=150,
            real_volume=0,
        ),
    ]

    batch = adapter.get_closed_candles("XAUUSD", "M5", 1)
    candle = batch.candles[0]

    assert candle.tick_volume == 150
    assert candle.real_volume is None
    assert candle.volume_source == VolumeSource.TICK


def test_candles_are_sorted_oldest_to_newest():
    adapter, mt5 = make_connected_adapter()

    now = datetime.now(timezone.utc)
    older = int((now - timedelta(minutes=20)).timestamp())
    newer = int((now - timedelta(minutes=10)).timestamp())

    mt5.copy_rates_from_pos.return_value = [
        make_rate(timestamp=newer),
        make_rate(timestamp=older),
    ]

    batch = adapter.get_closed_candles("XAUUSD", "M5", 2)

    assert batch.candles[0].timestamp < batch.candles[1].timestamp


def test_duplicate_candle_timestamps_are_rejected():
    adapter, mt5 = make_connected_adapter()

    timestamp = int(
        (datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp()
    )
    mt5.copy_rates_from_pos.return_value = [
        make_rate(timestamp=timestamp),
        make_rate(timestamp=timestamp),
    ]

    with pytest.raises(MT5AdapterError):
        adapter.get_closed_candles("XAUUSD", "M5", 2)


def test_future_candle_timestamp_is_rejected():
    adapter, mt5 = make_connected_adapter()

    future_timestamp = int(
        (datetime.now(timezone.utc) + timedelta(hours=1)).timestamp()
    )
    mt5.copy_rates_from_pos.return_value = [
        make_rate(timestamp=future_timestamp),
    ]

    with pytest.raises(MT5AdapterError, match="future timestamp"):
        adapter.get_closed_candles("XAUUSD", "M5", 1)


def test_empty_candle_history_is_rejected():
    adapter, mt5 = make_connected_adapter()
    mt5.copy_rates_from_pos.return_value = []

    with pytest.raises(MT5AdapterError, match="empty candle history"):
        adapter.get_closed_candles("XAUUSD", "M5", 10)


def test_none_candle_history_is_rejected():
    adapter, mt5 = make_connected_adapter()
    mt5.copy_rates_from_pos.return_value = None

    with pytest.raises(MT5AdapterError, match="no candle data"):
        adapter.get_closed_candles("XAUUSD", "M5", 10)


def test_invalid_ohlc_data_is_rejected():
    adapter, mt5 = make_connected_adapter()

    timestamp = int(
        (datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp()
    )
    mt5.copy_rates_from_pos.return_value = [
        make_rate(
            timestamp=timestamp,
            open_price=2010.0,
            high=2005.0,
            low=1995.0,
            close=2002.0,
        ),
    ]

    with pytest.raises(MT5AdapterError):
        adapter.get_closed_candles("XAUUSD", "M5", 1)


def test_negative_volume_is_rejected():
    adapter, mt5 = make_connected_adapter()

    timestamp = int(
        (datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp()
    )
    mt5.copy_rates_from_pos.return_value = [
        make_rate(
            timestamp=timestamp,
            tick_volume=-1,
        ),
    ]

    with pytest.raises(MT5AdapterError, match="negative volume"):
        adapter.get_closed_candles("XAUUSD", "M5", 1)


@pytest.mark.parametrize("count", [0, -1, 10001, True, 1.5])
def test_invalid_candle_count_is_rejected(count):
    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=make_mt5_module(),
    )

    with pytest.raises((ValueError, TypeError)):
        adapter.get_closed_candles("XAUUSD", "M5", count)


def test_unsupported_timeframe_is_rejected():
    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=make_mt5_module(),
    )

    with pytest.raises(ValueError):
        adapter.get_closed_candles("XAUUSD", "M2", 10)

def test_initialization_exception_returns_error_status():
    mt5 = make_mt5_module()
    mt5.initialize.side_effect = RuntimeError("Initialization crashed")

    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    status = adapter.connect()

    assert status.state == MT5ConnectionState.ERROR
    assert "RuntimeError" in status.message
    mt5.initialize.assert_called_once()


def test_candle_retrieval_exception_is_wrapped_as_adapter_error():
    adapter, mt5 = make_connected_adapter()

    mt5.copy_rates_from_pos.side_effect = RuntimeError(
        "Terminal communication failed"
    )

    with pytest.raises(
        MT5AdapterError,
        match="candle retrieval raised RuntimeError",
    ) as error:
        adapter.get_closed_candles("XAUUSD", "M5", 10)

    assert isinstance(error.value.__cause__, RuntimeError)
    mt5.copy_rates_from_pos.assert_called_once_with(
        "XAUUSD",
        5,
        1,
        10,
    )
def test_account_information_unavailable_raises_adapter_error():
    adapter, mt5 = make_connected_adapter()
    mt5.account_info.return_value = None

    with pytest.raises(MT5AdapterError, match="Test error"):
        adapter.get_account_info()


@pytest.mark.parametrize(
    "missing_info",
    ["terminal", "account"],
)
def test_connection_status_handles_missing_terminal_or_account_info(
    missing_info,
):
    adapter, mt5 = make_connected_adapter()

    if missing_info == "terminal":
        mt5.terminal_info.return_value = None
    else:
        mt5.account_info.return_value = None

    status = adapter.connection_status()

    assert status.state == MT5ConnectionState.DISCONNECTED
    assert status.terminal_connected is False


def test_connection_status_handles_terminal_exception():
    adapter, mt5 = make_connected_adapter()
    mt5.terminal_info.side_effect = RuntimeError(
        "Terminal status unavailable"
    )

    status = adapter.connection_status()

    assert status.state == MT5ConnectionState.ERROR
    assert "RuntimeError" in status.message
def test_monitor_recovers_real_adapter_after_initial_connection_failure():
    from backend.mt5.monitor import MT5ConnectionMonitor

    mt5 = make_mt5_module()
    mt5.initialize.side_effect = [False, True]

    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    initial_status = adapter.connect()

    assert initial_status.state == MT5ConnectionState.DISCONNECTED

    monitor = MT5ConnectionMonitor(adapter)

    recovered_status = monitor.check_once()

    assert recovered_status.state == MT5ConnectionState.CONNECTED
    assert adapter.connection_status().state == MT5ConnectionState.CONNECTED
    assert mt5.initialize.call_count == 2

    adapter.shutdown()
    mt5.shutdown.assert_called_once()
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("open", float("nan")),
        ("open", float("inf")),
        ("high", float("-inf")),
        ("low", float("nan")),
        ("close", float("inf")),
    ],
)
def test_non_finite_candle_prices_are_rejected(field, value):
    adapter, mt5 = make_connected_adapter()

    timestamp = int(
        (datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp()
    )
    rate = make_rate(timestamp=timestamp)
    rate[field] = value
    mt5.copy_rates_from_pos.return_value = [rate]

    with pytest.raises(MT5AdapterError, match="non-finite"):
        adapter.get_closed_candles("XAUUSD", "M5", 1)


def test_negative_real_volume_is_rejected():
    adapter, mt5 = make_connected_adapter()

    timestamp = int(
        (datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp()
    )
    mt5.copy_rates_from_pos.return_value = [
        make_rate(timestamp=timestamp, real_volume=-1),
    ]

    with pytest.raises(MT5AdapterError, match="negative volume"):
        adapter.get_closed_candles("XAUUSD", "M5", 1)


@pytest.mark.parametrize("timestamp", [0, -1])
def test_non_positive_candle_timestamp_is_rejected(timestamp):
    adapter, mt5 = make_connected_adapter()
    mt5.copy_rates_from_pos.return_value = [
        make_rate(timestamp=timestamp),
    ]

    with pytest.raises(MT5AdapterError, match="invalid candle timestamp"):
        adapter.get_closed_candles("XAUUSD", "M5", 1)


@pytest.mark.parametrize(
    "missing_field",
    [
        "time",
        "open",
        "high",
        "low",
        "close",
        "tick_volume",
        "real_volume",
    ],
)
def test_missing_candle_field_is_rejected(missing_field):
    adapter, mt5 = make_connected_adapter()

    timestamp = int(
        (datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp()
    )
    rate = make_rate(timestamp=timestamp)
    del rate[missing_field]
    mt5.copy_rates_from_pos.return_value = [rate]

    with pytest.raises(MT5AdapterError, match="failed validation"):
        adapter.get_closed_candles("XAUUSD", "M5", 1)
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("digits", -1),
        ("digits", 13),
        ("point", 0),
        ("point", -0.01),
        ("trade_contract_size", 0),
        ("trade_tick_size", 0),
        ("trade_tick_value", -1),
        ("volume_min", 0),
        ("volume_max", 0),
        ("volume_step", 0),
        ("spread", -1),
    ],
)
def test_invalid_symbol_information_is_rejected(field, value):
    adapter, mt5 = make_connected_adapter()

    symbol_info = mt5.symbol_info.return_value
    setattr(symbol_info, field, value)

    with pytest.raises(Exception):
        adapter.get_symbol_info("XAUUSD")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("point", float("nan")),
        ("point", float("inf")),
        ("trade_contract_size", float("nan")),
        ("trade_tick_size", float("inf")),
        ("trade_tick_value", float("nan")),
        ("volume_min", float("nan")),
        ("volume_max", float("inf")),
        ("volume_step", float("nan")),
    ],
)
def test_non_finite_symbol_information_is_rejected(field, value):
    adapter, mt5 = make_connected_adapter()

    symbol_info = mt5.symbol_info.return_value
    setattr(symbol_info, field, value)

    with pytest.raises(Exception):
        adapter.get_symbol_info("XAUUSD")


def test_symbol_information_is_immutable():
    adapter, mt5 = make_connected_adapter()

    symbol = adapter.get_symbol_info("XAUUSD")

    with pytest.raises(Exception):
        symbol.digits = 3


def test_symbol_name_is_normalized_before_mt5_lookup():
    adapter, mt5 = make_connected_adapter()

    adapter.get_symbol_info(" XAUUSD ")

    mt5.symbol_info.assert_called_once_with("XAUUSD")


def test_empty_symbol_information_name_is_rejected():
    adapter, mt5 = make_connected_adapter()

    mt5.symbol_info.return_value.name = ""

    with pytest.raises(Exception):
        adapter.get_symbol_info("XAUUSD")


@pytest.mark.parametrize(
    ("digits", "point", "tick_size"),
    [
        (0, 1.0, 1.0),
        (2, 0.01, 0.01),
        (3, 0.001, 0.001),
        (4, 0.0001, 0.0005),
        (5, 0.00001, 0.0001),
        (2, 0.01, 0.005),
        (12, 0.000000000001, 0.000000000001),
    ],
)
def test_symbol_price_properties_are_preserved(
    digits,
    point,
    tick_size,
):
    adapter, mt5 = make_connected_adapter()

    info = mt5.symbol_info.return_value
    info.digits = digits
    info.point = point
    info.trade_tick_size = tick_size

    symbol = adapter.get_symbol_info("XAUUSD")

    assert symbol.digits == digits
    assert symbol.point == point
    assert symbol.trade_tick_size == tick_size


@pytest.mark.parametrize(
    "digits",
    [-1, 13, True, 2.5, "2", None],
)
def test_invalid_symbol_digits_are_rejected(digits):
    adapter, mt5 = make_connected_adapter()
    mt5.symbol_info.return_value.digits = digits

    with pytest.raises(MT5AdapterError, match="invalid symbol digits"):
        adapter.get_symbol_info("XAUUSD")


def test_point_and_tick_size_can_have_different_values():
    adapter, mt5 = make_connected_adapter()

    info = mt5.symbol_info.return_value
    info.digits = 3
    info.point = 0.001
    info.trade_tick_size = 0.005

    symbol = adapter.get_symbol_info("XAUUSD")

    assert symbol.point == 0.001
    assert symbol.trade_tick_size == 0.005
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("point", True),
        ("trade_contract_size", False),
        ("trade_tick_size", "invalid"),
        ("trade_tick_value", None),
        ("volume_min", "invalid"),
        ("volume_max", None),
        ("volume_step", True),
    ],
)
def test_malformed_symbol_numeric_fields_are_rejected(
    field,
    value,
):
    adapter, mt5 = make_connected_adapter()
    setattr(mt5.symbol_info.return_value, field, value)

    with pytest.raises(MT5AdapterError):
        adapter.get_symbol_info("XAUUSD")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("spread", -1),
        ("spread", 1.5),
        ("spread", True),
        ("spread", "invalid"),
    ],
)
def test_invalid_symbol_spread_is_rejected(field, value):
    adapter, mt5 = make_connected_adapter()
    setattr(mt5.symbol_info.return_value, field, value)

    with pytest.raises(Exception):
        adapter.get_symbol_info("XAUUSD")

@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("balance", float("nan")),
        ("balance", float("inf")),
        ("equity", float("-inf")),
        ("margin", float("nan")),
        ("margin_free", float("inf")),
    ],
)
def test_non_finite_account_financial_values_are_rejected(field, value):
    adapter, mt5 = make_connected_adapter()
    setattr(mt5.account_info.return_value, field, value)

    with pytest.raises(MT5AdapterError, match="non-finite"):
        adapter.get_account_info()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("balance", True),
        ("equity", "invalid"),
        ("margin", None),
        ("margin_free", False),
    ],
)
def test_malformed_account_financial_values_are_rejected(field, value):
    adapter, mt5 = make_connected_adapter()
    setattr(mt5.account_info.return_value, field, value)

    with pytest.raises(MT5AdapterError, match="invalid account field"):
        adapter.get_account_info()


def test_negative_account_margin_is_rejected():
    adapter, mt5 = make_connected_adapter()
    mt5.account_info.return_value.margin = -0.01

    with pytest.raises(MT5AdapterError, match="negative account margin"):
        adapter.get_account_info()


def test_negative_balance_equity_and_free_margin_are_preserved():
    adapter, mt5 = make_connected_adapter()
    mt5.account_info.return_value.balance = -10.0
    mt5.account_info.return_value.equity = -15.0
    mt5.account_info.return_value.margin_free = -20.0

    account = adapter.get_account_info()

    assert account.balance == -10.0
    assert account.equity == -15.0
    assert account.free_margin == -20.0

@pytest.mark.parametrize(
    "timestamp",
    [
        123.9,
        123.0,
        "123",
        True,
        False,
        None,
    ],
)
def test_malformed_candle_timestamps_are_rejected(timestamp):
    adapter, mt5 = make_connected_adapter()

    mt5.copy_rates_from_pos.return_value = [
        make_rate(timestamp=timestamp),
    ]

    with pytest.raises(
        MT5AdapterError,
        match="invalid candle timestamp",
    ):
        adapter.get_closed_candles("XAUUSD", "M5", 1)


def test_integer_candle_timestamp_is_accepted():
    adapter, mt5 = make_connected_adapter()

    timestamp = int(
        (datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp()
    )
    mt5.copy_rates_from_pos.return_value = [
        make_rate(timestamp=timestamp),
    ]

    batch = adapter.get_closed_candles("XAUUSD", "M5", 1)

    assert len(batch.candles) == 1
    assert batch.candles[0].timestamp == datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc,
    )
def test_monitor_recovers_after_active_mt5_connection_is_lost():
    from backend.mt5.monitor import MT5ConnectionMonitor

    mt5 = make_mt5_module()

    adapter = MT5Adapter(
        settings=make_settings(),
        mt5_module=mt5,
    )

    initial_status = adapter.connect()

    assert initial_status.state == MT5ConnectionState.CONNECTED
    assert mt5.initialize.call_count == 1

    # Simulate losing the broker connection while the terminal
    # was previously connected.
    mt5.terminal_info.return_value.connected = False

    def restore_terminal_connection(**kwargs):
        mt5.terminal_info.return_value.connected = True
        return True

    mt5.initialize.side_effect = restore_terminal_connection

    monitor = MT5ConnectionMonitor(adapter)

    recovered_status = monitor.check_once()

    assert recovered_status.state == MT5ConnectionState.CONNECTED
    assert mt5.shutdown.call_count == 1
    assert mt5.initialize.call_count == 2

    timestamp = int(
        (datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp()
    )
    mt5.copy_rates_from_pos.return_value = [
        make_rate(timestamp=timestamp),
    ]

    batch = adapter.get_closed_candles("XAUUSD", "M5", 1)

    assert len(batch.candles) == 1
    assert adapter.connection_status().state == MT5ConnectionState.CONNECTED

    adapter.shutdown()
    assert mt5.shutdown.call_count == 2
