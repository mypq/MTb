from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from backend.market_data.capabilities import (
    CapabilityRegistry,
    CapabilityStatus,
    MarketDataCapability,
)


NOW = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)


def make_capability(**overrides):
    values = {
        "capability": "tick_history",
        "status": CapabilityStatus.UNKNOWN,
        "source": "MT5",
        "checked_at": NOW,
    }
    values.update(overrides)
    return MarketDataCapability(**values)


def test_unknown_is_the_safe_default():
    capability = make_capability(status=CapabilityStatus.UNKNOWN)
    assert capability.status is CapabilityStatus.UNKNOWN


def test_capability_requires_timezone_aware_utc_check_time():
    with pytest.raises(ValidationError):
        make_capability(checked_at=datetime(2026, 11, 21, 12, 0))

    with pytest.raises(ValidationError):
        make_capability(
            checked_at=datetime(
                2026, 11, 21, 15, 0,
                tzinfo=timezone(timedelta(hours=3)),
            )
        )


def test_capability_normalizes_identity_fields():
    capability = make_capability(
        capability="  tick_history ",
        source=" MT5 ",
        symbol=" XAUUSD ",
        timeframe=" m5 ",
        broker_server=" Demo-Server ",
        account_login=123456,
    )
    assert capability.capability == "tick_history"
    assert capability.source == "MT5"
    assert capability.symbol == "XAUUSD"
    assert capability.timeframe == "M5"
    assert capability.broker_server == "Demo-Server"


def test_capability_rejects_boolean_account_login():
    with pytest.raises(ValidationError):
        make_capability(account_login=True)


def test_capability_is_immutable():
    capability = make_capability()
    with pytest.raises(ValidationError):
        capability.status = CapabilityStatus.AVAILABLE


def test_registry_records_and_retrieves_by_scope():
    registry = CapabilityRegistry()
    capability = make_capability(
        status=CapabilityStatus.PARTIAL,
        symbol="XAUUSD",
        timeframe="M5",
        evidence=("limited historical response observed",),
    )

    registry.record(capability)

    assert registry.get(
        "TICK_HISTORY",
        symbol="xauusd",
        timeframe="m5",
    ) == capability
    assert registry.get(
        "tick_history",
        symbol="EURUSD",
        timeframe="M5",
    ) is None


def test_registry_replaces_only_the_same_scope():
    registry = CapabilityRegistry()
    first = make_capability(symbol="XAUUSD", timeframe="M5")
    second = make_capability(
        symbol="XAUUSD",
        timeframe="M5",
        status=CapabilityStatus.AVAILABLE,
        evidence=("verified historical response",),
    )

    registry.record(first)
    registry.record(second)

    assert registry.snapshot() == (second,)


def test_registry_snapshot_is_stable_and_sorted():
    registry = CapabilityRegistry()
    second = make_capability(capability="tick_history")
    first = make_capability(capability="candle_history")

    registry.record(second)
    registry.record(first)

    snapshot = registry.snapshot()
    assert tuple(item.capability for item in snapshot) == (
        "candle_history",
        "tick_history",
    )
    assert isinstance(snapshot, tuple)


def test_registry_clear_removes_observations():
    registry = CapabilityRegistry()
    registry.record(make_capability())

    registry.clear()

    assert registry.snapshot() == ()


def test_registry_rejects_untyped_values():
    registry = CapabilityRegistry()
    with pytest.raises(TypeError):
        registry.record({"capability": "tick_history"})


def test_observed_status_requires_evidence():
    with pytest.raises(ValidationError):
        make_capability(status=CapabilityStatus.AVAILABLE)


def test_account_identity_must_be_complete():
    with pytest.raises(ValidationError):
        make_capability(account_login=123456)
