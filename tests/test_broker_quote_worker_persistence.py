from datetime import datetime, timezone

from backend.mt5.broker_tick_monitor import BrokerTickMonitor
from backend.mt5.broker_tick_worker import BrokerTickMonitorWorker
from backend.mt5.time_models import BrokerTimeSample


class RecordingQuoteRepository:
    def __init__(self):
        self.samples = []

    def save_sample(self, sample):
        self.samples.append(sample)
        return type("SaveResult", (), {"inserted": True, "duplicate": False})()


class SingleSampleAdapter:
    def __init__(self, sample):
        self.sample = sample

    def get_broker_time_sample(self, symbol):
        assert symbol == self.sample.symbol
        return self.sample


def test_worker_persists_each_validated_quote_sample():
    sample = BrokerTimeSample(
        symbol="GOLD",
        broker_time_msc=1795406052640,
        received_at_utc=datetime.now(timezone.utc),
        received_monotonic_ns=123456789,
        bid=3603.30,
        ask=3603.87,
        account_login=12345678,
        broker_server="XMGlobal-MT5 9",
    )
    repository = RecordingQuoteRepository()
    worker = BrokerTickMonitorWorker(
        SingleSampleAdapter(sample),
        BrokerTickMonitor(),
        symbol="GOLD",
        quote_repository=repository,
    )
    worker.check_once()
    status = worker.status()
    assert repository.samples == [sample]
    assert status["persistence"]["enabled"] is True
    assert status["persistence"]["inserted"] == 1
    assert status["persistence"]["duplicates"] == 0
    assert status["persistence"]["last_error"] is None
