
from datetime import datetime, timezone

from backend.market_data.broker_clock import (
    BrokerClockCalibrator,
    BrokerClockStatus,
)
from backend.market_data.ntp_time import (
    NtpSample,
    NtpTimeVerifier,
)
from backend.mt5.broker_tick_monitor import BrokerTickMonitor
from backend.mt5.broker_tick_worker import BrokerTickMonitorWorker
from backend.mt5.time_models import BrokerTimeSample


class FakeAdapter:
    def __init__(self, sample):
        self.sample = sample

    def get_broker_time_sample(self, symbol):
        return self.sample


def make_ntp_verifier():
    return NtpTimeVerifier(
        servers=("ntp-a", "ntp-b", "ntp-c"),
        wall_clock=lambda: 1000.0,
        monotonic_clock=lambda: 100.0,
        query=lambda server: NtpSample(
            server=server,
            offset_seconds=0.0,
            round_trip_seconds=0.02,
            root_dispersion_seconds=0.01,
        ),
    )


def make_sample(seconds):
    receipt_epoch = 1000.0 + seconds

    return BrokerTimeSample(
        symbol="GOLD",
        broker_time_msc=int(
            (receipt_epoch + 7200.0) * 1000
        ),
        received_at_utc=datetime.fromtimestamp(
            receipt_epoch,
            tz=timezone.utc,
        ),
        received_monotonic_ns=int(
            seconds * 1_000_000_000
        ),
        bid=4000.0,
        ask=4000.1,
        account_login=123456,
        broker_server="XMGlobal-MT5 9",
    )


def test_worker_produces_estimate_without_claiming_broker_utc_verification():
    ntp = make_ntp_verifier()
    assert ntp.verify().verified is True

    calibrator = BrokerClockCalibrator(
        ntp,
        required_samples=5,
        max_samples=9,
        minimum_span_seconds=3.0,
    )

    monitor = BrokerTickMonitor()
    adapter = FakeAdapter(make_sample(0))

    worker = BrokerTickMonitorWorker(
        adapter,
        monitor,
        symbol="GOLD",
        clock_calibrator=calibrator,
    )

    for second in (0, 1, 2, 3, 4):
        adapter.sample = make_sample(second)
        worker.check_once()

    result = calibrator.snapshot()

    assert result.status == BrokerClockStatus.ESTIMATED
    assert result.offset_seconds == 7200.0
    assert result.sample_count == 5
    assert result.reason is not None
    assert "not verified broker UTC" in result.reason

    status = worker.status()

    # Tick progression monitoring does not verify UTC alignment.
    assert status["clock_verification"] == "UNVERIFIED"

    # Calibration is exposed only as an estimate.
    assert status["clock_calibration"]["status"] == "ESTIMATED"
    assert status["clock_calibration"]["offset_seconds"] == 7200.0
