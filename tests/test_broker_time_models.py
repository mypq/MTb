
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from backend.mt5.time_models import BrokerTimeSample


def make_sample(**overrides):
    values = {
        "symbol": "XAUUSD",
        "broker_time_msc": 1794729600123,
        "received_at_utc": datetime(
            2026, 11, 15, 12, 0, 1,
            tzinfo=timezone.utc,
        ),
        "received_monotonic_ns": 123456789000,
        "bid": 4100.10,
        "ask": 4100.20,
        "account_login": 123456,
        "broker_server": "Demo-Server",
    }
    values.update(overrides)
    return BrokerTimeSample(**values)


def test_valid_broker_time_sample():
    sample = make_sample()

    assert sample.symbol == "XAUUSD"
    assert sample.broker_time_msc == 1794729600123
    assert sample.source == "symbol_info_tick"


def test_raw_broker_timestamp_is_preserved():
    timestamp = 1794729600123

    sample = make_sample(broker_time_msc=timestamp)

    assert sample.broker_time_msc == timestamp


def test_received_timestamp_must_be_utc():
    with pytest.raises(ValidationError, match="already UTC"):
        make_sample(
            received_at_utc=datetime(
                2026, 11, 15, 15, 0,
                tzinfo=timezone(timedelta(hours=3)),
            )
        )


def test_naive_received_timestamp_is_rejected():
    with pytest.raises(ValidationError, match="timezone-aware"):
        make_sample(
            received_at_utc=datetime(2026, 11, 15, 12, 0)
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("symbol", ""),
        ("symbol", "   "),
        ("broker_server", ""),
        ("broker_server", "   "),
        ("broker_time_msc", 0),
        ("broker_time_msc", -1),
        ("received_monotonic_ns", -1),
        ("account_login", 0),
    ],
)
def test_invalid_required_fields_are_rejected(field, value):
    with pytest.raises(ValidationError):
        make_sample(**{field: value})


@pytest.mark.parametrize(
    "field",
    [
        "broker_time_msc",
        "received_monotonic_ns",
        "account_login",
    ],
)
def test_boolean_integer_fields_are_rejected(field):
    with pytest.raises(ValidationError):
        make_sample(**{field: True})


@pytest.mark.parametrize(
    "bid,ask",
    [
        (0, 1),
        (-1, 1),
        (1, 0),
        (1, -1),
        (2, 1),
    ],
)
def test_invalid_bid_ask_prices_are_rejected(bid, ask):
    with pytest.raises(ValidationError):
        make_sample(bid=bid, ask=ask)


def test_boolean_prices_are_rejected():
    with pytest.raises(ValidationError):
        make_sample(bid=True)

    with pytest.raises(ValidationError):
        make_sample(ask=False)


def test_sample_is_immutable():
    sample = make_sample()

    with pytest.raises(ValidationError):
        sample.symbol = "EURUSD"


def test_text_fields_are_normalized():
    sample = make_sample(
        symbol=" XAUUSD ",
        broker_server=" Demo-Server ",
    )

    assert sample.symbol == "XAUUSD"
    assert sample.broker_server == "Demo-Server"


def test_unsupported_source_is_rejected():
    with pytest.raises(ValidationError):
        make_sample(source="TimeTradeServer")
