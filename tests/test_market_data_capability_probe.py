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

    def probe_tick_history(
        self,
        symbol,
        *,
        lookback_days=1,
        sample_count=1000,
    ):
        self.calls.append(("tick_history", symbol, lookback_days, sample_count))
        return SimpleNamespace(
            lookback_days=lookback_days,
            requested_count_per_query=sample_count,
            all_ticks_count=1000,
            quote_ticks_count=700,
            all_ticks_first_time_msc=1794000000000,
            all_ticks_last_time_msc=1794000001000,
            quote_ticks_first_time_msc=1794000000100,
            quote_ticks_last_time_msc=1794000000900,
        )

    def get_closed_candles(self, *args, **kwargs):
        raise AssertionError("Capability probe must not request candles.")


def test_probe_records_only_observed_live_capabilities():
    source = FakeMT5Source()
    registry = CapabilityRegistry()

    observations = MT5CapabilityProbe(source, registry).probe(" XAUUSD ", "m5")

    assert len(observations) == 8
    assert registry.get("account_identity").status is CapabilityStatus.AVAILABLE
    assert registry.get("symbol_metadata", symbol="XAUUSD").status is CapabilityStatus.AVAILABLE
    quote = registry.get("live_quote_sample", symbol="XAUUSD")
    assert quote.status is CapabilityStatus.AVAILABLE
    assert any("not asserted to be UTC" in item for item in quote.evidence)

    candle = registry.get("candle_history", symbol="XAUUSD", timeframe="M5")
    assert candle.status is CapabilityStatus.UNKNOWN
    assert registry.get("tick_history_source", symbol="XAUUSD").status is CapabilityStatus.AVAILABLE
    assert registry.get("quote_history_source", symbol="XAUUSD").status is CapabilityStatus.AVAILABLE
    assert registry.get("tick_history_archive", symbol="XAUUSD").status is CapabilityStatus.UNAVAILABLE
    assert registry.get("quote_archive", symbol="XAUUSD").status is CapabilityStatus.UNAVAILABLE
    assert source.calls == [
        "account",
        ("symbol", "XAUUSD"),
        ("quote", "XAUUSD"),
        ("tick_history", "XAUUSD", 1, 1000),
    ]


def test_probe_records_transient_failures_without_claiming_permanent_broker_limits():
    class DisconnectedSource:
        def get_account_info(self):
            raise RuntimeError("offline")

        def get_symbol_info(self, symbol):
            raise RuntimeError("offline")

        def get_broker_time_sample(self, symbol):
            raise RuntimeError("offline")

        def probe_tick_history(self, symbol, *, lookback_days=1, sample_count=1000):
            raise RuntimeError("offline")

    registry = CapabilityRegistry()
    MT5CapabilityProbe(DisconnectedSource(), registry).probe("XAUUSD", "M5")

    assert registry.get("account_identity").status is CapabilityStatus.UNAVAILABLE
    quote = registry.get("live_quote_sample", symbol="XAUUSD")
    assert quote.status is CapabilityStatus.UNAVAILABLE
    assert "retry" in " ".join(quote.limitations).lower()
    assert registry.get("candle_history", symbol="XAUUSD", timeframe="M5").status is CapabilityStatus.UNKNOWN
    assert registry.get("tick_history_source", symbol="XAUUSD").status is CapabilityStatus.PARTIAL
    assert registry.get("quote_history_source", symbol="XAUUSD").status is CapabilityStatus.PARTIAL


def test_probe_rejects_empty_scope_before_calling_source():
    source = FakeMT5Source()
    probe = MT5CapabilityProbe(source, CapabilityRegistry())

    import pytest
    with pytest.raises(ValueError):
        probe.probe(" ", "M5")
    assert source.calls == []



def test_probe_initializes_candle_capability_for_every_configured_timeframe():
    source = FakeMT5Source()
    registry = CapabilityRegistry()
    frames = ("M1", "M5", "M15", "M30", "H1", "H4", "D1")

    observations = MT5CapabilityProbe(source, registry).probe(
        "XAUUSD",
        "M5",
        include_live_quote=False,
        timeframes=frames,
    )

    candle_observations = [
        item for item in observations
        if item.capability == "candle_history"
    ]
    assert [item.timeframe for item in candle_observations] == list(frames)
    assert all(item.status is CapabilityStatus.UNKNOWN for item in candle_observations)
    assert source.calls == [
        "account",
        ("symbol", "XAUUSD"),
        ("tick_history", "XAUUSD", 30, 1000),
    ]


def test_probe_rejects_duplicate_or_incomplete_timeframe_scope():
    import pytest

    probe = MT5CapabilityProbe(FakeMT5Source(), CapabilityRegistry())
    with pytest.raises(ValueError, match="duplicates"):
        probe.probe(
            "XAUUSD",
            "M5",
            timeframes=("M5", "m5"),
        )
    with pytest.raises(ValueError, match="primary timeframe"):
        probe.probe(
            "XAUUSD",
            "M5",
            timeframes=("M1", "M15"),
        )



def test_probe_keeps_empty_history_partial_not_permanently_unavailable():
    class EmptyHistorySource(FakeMT5Source):
        def probe_tick_history(
            self,
            symbol,
            *,
            lookback_days=30,
            sample_count=1000,
        ):
            self.calls.append(("tick_history", symbol, lookback_days, sample_count))
            return SimpleNamespace(
                lookback_days=lookback_days,
                requested_count_per_query=sample_count,
                all_ticks_count=0,
                quote_ticks_count=0,
                all_ticks_first_time_msc=None,
                all_ticks_last_time_msc=None,
                quote_ticks_first_time_msc=None,
                quote_ticks_last_time_msc=None,
            )

    registry = CapabilityRegistry()
    MT5CapabilityProbe(EmptyHistorySource(), registry).probe(
        "XAUUSD",
        "M5",
        include_live_quote=False,
    )

    tick_source = registry.get("tick_history_source", symbol="XAUUSD")
    quote_source = registry.get("quote_history_source", symbol="XAUUSD")
    assert tick_source.status is CapabilityStatus.PARTIAL
    assert quote_source.status is CapabilityStatus.PARTIAL
    assert "does not prove" in " ".join(tick_source.limitations)
    assert registry.get("tick_history_archive", symbol="XAUUSD").status is CapabilityStatus.UNAVAILABLE
