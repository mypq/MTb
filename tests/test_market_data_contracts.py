from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from backend.market_data.contracts import (
    MarketCandleDataContract,
    MarketDataBatchContract,
)

UTC = timezone.utc
NOW = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)


def make_candle_contract(**overrides):
    values = {
        "candle_id": 1,
        "batch_id": 2,
        "instrument": "GOLD",
        "timeframe": "M5",
        "event_time": NOW,
        "available_at": NOW,
        "recorded_at": NOW,
        "open": 2000.0,
        "high": 2005.0,
        "low": 1995.0,
        "close": 2002.0,
        "tick_volume": 100,
        "real_volume": 0,
        "volume_source": "TICK",
        "candle_quality": "VERIFIED",
        "is_closed": True,
        "source": "MT5",
        "source_account_login": 123456,
        "source_broker_server": "Broker",
        "clock_verified": True,
        "analysis_eligible": True,
    }
    values.update(overrides)
    return MarketCandleDataContract(**values)


def make_batch_contract(**overrides):
    values = {
        "batch_id": 2,
        "instrument": "GOLD",
        "timeframe": "M5",
        "source": "MT5",
        "available_at": NOW,
        "recorded_at": NOW,
        "candle_count": 2,
        "candle_ids": (1, 2),
        "source_account_login": 123456,
        "source_broker_server": "Broker",
        "clock_verified": True,
    }
    values.update(overrides)
    return MarketDataBatchContract(**values)


@pytest.mark.parametrize(
    "field",
    [
        "candle_id",
        "batch_id",
        "tick_volume",
        "real_volume",
        "source_account_login",
    ],
)
@pytest.mark.parametrize("value", [True, False])
def test_candle_contract_rejects_boolean_integer_fields(field, value):
    with pytest.raises(ValidationError):
        make_candle_contract(**{field: value})


@pytest.mark.parametrize("field", ["open", "high", "low", "close"])
@pytest.mark.parametrize("value", [True, False])
def test_candle_contract_rejects_boolean_price_fields(field, value):
    with pytest.raises(ValidationError):
        make_candle_contract(**{field: value})


@pytest.mark.parametrize(
    "field",
    ["batch_id", "candle_count", "source_account_login"],
)
@pytest.mark.parametrize("value", [True, False])
def test_batch_contract_rejects_boolean_integer_fields(field, value):
    with pytest.raises(ValidationError):
        make_batch_contract(**{field: value})


@pytest.mark.parametrize("candle_ids", [(1, True), (False, 2), [1, False]])
def test_batch_contract_rejects_boolean_candle_ids(candle_ids):
    with pytest.raises(ValidationError):
        make_batch_contract(candle_ids=candle_ids)


def test_contracts_accept_valid_integer_and_decimal_values():
    candle = make_candle_contract()
    batch = make_batch_contract()

    assert candle.candle_id == 1
    assert candle.open == 2000.0
    assert batch.candle_ids == (1, 2)
