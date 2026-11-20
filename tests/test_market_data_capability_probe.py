from types import SimpleNamespace

from backend.market_data.capabilities import CapabilityRegistry, CapabilityStatus
from backend.market_data.mt5_capability_probe import MT5CapabilityProbe


class FakeMT5Source:
    def __init__(self):
        self.calls = []

    def get_account_info(self):
        self.calls.append("account")
        return SimpleNamespace(login=123456, server="Demo-Server")

    def get_symbol_info(self, symbol):
        self.calls.append(("symbol", symbol))
        return SimpleNamespace(name=symbol, digits=2)

    def get_broker_time_sample(self, symbol):
        self.calls.append(("quote", symbol))
        return SimpleNamespace(
            account_login=123456,
            broker_server="Demo-Server",
            broker_time_msc=1790000000000,
        )

    def get_closed_candles(self, *args, **kwargs):
        raise AssertionError("Capability probe must not request candles.")


def test_probe_records_only_observed_live_capabilities():
    source = FakeMT5Source()
    registry = CapabilityRegistry()

    observations = MT5CapabilityProbe(source, registry).probe(" XAUUSD ", "m5")

    assert len(observations) == 6
    assert registry.get("account_identity").status is CapabilityStatus.AVAILABLE
    assert registry.get("symbol_metadata", symbol="XAUUSD").status is CapabilityStatus.AVAILABLE
    quote = registry.get("live_quote_sample", symbol="XAUUSD")
    assert quote.status is CapabilityStatus.AVAILABLE
    assert any("not asserted to be UTC" in item for item in quote.evidence)

    candle = registry.get("candle_history", symbol="XAUUSD", timeframe="M5")
    assert candle.status is CapabilityStatus.UNKNOWN
    assert registry.get("tick_history_archive", symbol="XAUUSD").status is CapabilityStatus.UNAVAILABLE
    assert registry.get("quote_archive", symbol="XAUUSD").status is CapabilityStatus.UNAVAILABLE
    assert source.calls == [
        "account",
        ("symbol", "XAUUSD"),
        ("quote", "XAUUSD"),
    ]


def test_probe_records_transient_failures_without_claiming_permanent_broker_limits():
    class DisconnectedSource:
        def get_account_info(self):
            raise RuntimeError("offline")

        def get_symbol_info(self, symbol):
            raise RuntimeError("offline")

        def get_broker_time_sample(self, symbol):
            raise RuntimeError("offline")

    registry = CapabilityRegistry()
    MT5CapabilityProbe(DisconnectedSource(), registry).probe("XAUUSD", "M5")

    assert registry.get("account_identity").status is CapabilityStatus.UNAVAILABLE
    quote = registry.get("live_quote_sample", symbol="XAUUSD")
    assert quote.status is CapabilityStatus.UNAVAILABLE
    assert "retry" in " ".join(quote.limitations).lower()
    assert registry.get("candle_history", symbol="XAUUSD", timeframe="M5").status is CapabilityStatus.UNKNOWN


def test_probe_rejects_empty_scope_before_calling_source():
    source = FakeMT5Source()
    probe = MT5CapabilityProbe(source, CapabilityRegistry())

    import pytest
    with pytest.raises(ValueError):
        probe.probe(" ", "M5")
    assert source.calls == []
