
from datetime import datetime, timedelta, timezone

import pytest

from backend.market_data.time_sync import (
    TimeSyncStatus,
    TimeSynchronizationService,
)

UTC = timezone.utc
INITIAL_TIME = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


class FakeClock:
    def __init__(self):
        self.wall_time = INITIAL_TIME
        self.monotonic_ns = 1_000_000_000

    def wall(self):
        return self.wall_time

    def monotonic(self):
        return self.monotonic_ns

    def advance(
        self,
        milliseconds: float,
        *,
        wall_adjustment_ms: float = 0,
    ):
        self.monotonic_ns += int(milliseconds * 1_000_000)
        self.wall_time += timedelta(
            milliseconds=milliseconds + wall_adjustment_ms
        )


def make_service(
    clock: FakeClock,
    **overrides,
) -> TimeSynchronizationService:
    options = {
        "wall_clock": clock.wall,
        "monotonic_clock_ns": clock.monotonic,
    }
    options.update(overrides)
    return TimeSynchronizationService(**options)


def add_sample(
    service: TimeSynchronizationService,
    clock: FakeClock,
    *,
    elapsed_ms: float = 100,
    offset_ms: float = 100,
):
    request_start = service.begin_sample()
    clock.advance(elapsed_ms)

    server_time = request_start.local_sent_at + timedelta(
        milliseconds=elapsed_ms / 2 + offset_ms
    )

    return service.complete_sample(
        request_start,
        server_time,
    )


def add_three_samples(
    service: TimeSynchronizationService,
    clock: FakeClock,
):
    for _ in range(3):
        add_sample(service, clock)


def test_service_starts_unsynchronized():
    clock = FakeClock()
    service = make_service(clock)

    snapshot = service.snapshot()

    assert snapshot.status == TimeSyncStatus.UNSYNCHRONIZED
    assert snapshot.sample_count == 0
    assert snapshot.offset_ms is None


def test_begin_sample_captures_utc_and_monotonic_clocks():
    clock = FakeClock()
    service = make_service(clock)

    request_start = service.begin_sample()

    assert request_start.local_sent_at == INITIAL_TIME
    assert request_start.monotonic_sent_ns == clock.monotonic_ns


def test_valid_sample_uses_monotonic_round_trip():
    clock = FakeClock()
    service = make_service(clock)

    sample = add_sample(
        service,
        clock,
        elapsed_ms=120,
        offset_ms=80,
    )

    assert sample is not None
    assert sample.round_trip_ms == 120
    assert sample.estimated_offset_ms == pytest.approx(80)


def test_three_valid_samples_produce_synchronized_status():
    clock = FakeClock()
    service = make_service(clock)

    add_three_samples(service, clock)

    snapshot = service.snapshot()

    assert snapshot.status == TimeSyncStatus.SYNCHRONIZED
    assert snapshot.sample_count == 3
    assert snapshot.offset_ms == pytest.approx(100)
    assert snapshot.median_round_trip_ms == pytest.approx(100)
    assert snapshot.uncertainty_ms == pytest.approx(50)


def test_one_sample_is_not_enough_for_synchronization():
    clock = FakeClock()
    service = make_service(clock)

    add_sample(service, clock)

    snapshot = service.snapshot()

    assert snapshot.status == TimeSyncStatus.DEGRADED
    assert snapshot.sample_count == 1


def test_two_samples_are_not_enough_for_synchronization():
    clock = FakeClock()
    service = make_service(clock)

    add_sample(service, clock)
    add_sample(service, clock)

    snapshot = service.snapshot()

    assert snapshot.status == TimeSyncStatus.DEGRADED
    assert snapshot.sample_count == 2


def test_wall_clock_jump_during_request_invalidates_sample():
    clock = FakeClock()
    service = make_service(clock)

    request_start = service.begin_sample()
    clock.advance(100, wall_adjustment_ms=1000)

    result = service.complete_sample(
        request_start,
        INITIAL_TIME + timedelta(milliseconds=150),
    )

    assert result is None

    snapshot = service.snapshot()

    assert snapshot.status == TimeSyncStatus.DEGRADED
    assert snapshot.sample_count == 0


def test_wall_clock_jump_after_samples_degrades_service():
    clock = FakeClock()
    service = make_service(clock)

    add_three_samples(service, clock)

    clock.advance(100, wall_adjustment_ms=1000)

    snapshot = service.snapshot()

    assert snapshot.status == TimeSyncStatus.DEGRADED
    assert snapshot.sample_count == 0
    assert snapshot.offset_ms is None


def test_backward_wall_clock_jump_degrades_service():
    clock = FakeClock()
    service = make_service(clock)

    add_three_samples(service, clock)

    clock.advance(100, wall_adjustment_ms=-1000)

    snapshot = service.snapshot()

    assert snapshot.status == TimeSyncStatus.DEGRADED
    assert snapshot.sample_count == 0


def test_expired_samples_are_not_used():
    clock = FakeClock()
    service = make_service(
        clock,
        max_sample_age_seconds=1,
    )

    add_three_samples(service, clock)

    clock.advance(1100)

    snapshot = service.snapshot()

    assert snapshot.status == TimeSyncStatus.DEGRADED
    assert snapshot.sample_count == 0
    assert snapshot.offset_ms is None


def test_high_round_trip_sample_is_excluded():
    clock = FakeClock()
    service = make_service(
        clock,
        max_round_trip_ms=200,
    )

    add_sample(
        service,
        clock,
        elapsed_ms=300,
    )

    snapshot = service.snapshot()

    assert snapshot.status == TimeSyncStatus.DEGRADED
    assert snapshot.sample_count == 0


def test_offset_above_limit_is_not_synchronized():
    clock = FakeClock()
    service = make_service(
        clock,
        max_offset_ms=500,
    )

    for _ in range(3):
        add_sample(
            service,
            clock,
            offset_ms=1000,
        )

    snapshot = service.snapshot()

    assert snapshot.status == TimeSyncStatus.DEGRADED
    assert snapshot.offset_ms == pytest.approx(1000)


def test_estimated_server_time_requires_synchronized_status():
    clock = FakeClock()
    service = make_service(clock)

    assert service.estimate_server_time() is None


def test_estimated_server_time_is_available_after_three_samples():
    clock = FakeClock()
    service = make_service(clock)

    add_three_samples(service, clock)

    estimated = service.estimate_server_time()

    assert estimated is not None
    assert estimated.tzinfo is not None
    assert estimated.utcoffset() == timedelta(0)


def test_monotonic_clock_moving_backwards_invalidates_samples():
    clock = FakeClock()
    service = make_service(clock)

    request_start = service.begin_sample()
    clock.monotonic_ns -= 1

    with pytest.raises(ValueError, match="moved backwards"):
        service.complete_sample(
            request_start,
            INITIAL_TIME,
        )

    snapshot = service.snapshot()

    assert snapshot.status == TimeSyncStatus.DEGRADED
    assert snapshot.sample_count == 0


def test_non_utc_server_time_is_rejected():
    clock = FakeClock()
    service = make_service(clock)

    request_start = service.begin_sample()

    non_utc_time = datetime(
        2026,
        1,
        1,
        15,
        0,
        tzinfo=timezone(timedelta(hours=3)),
    )

    with pytest.raises(ValueError, match="UTC-aware"):
        service.complete_sample(
            request_start,
            non_utc_time,
        )


def test_naive_server_time_is_rejected():
    clock = FakeClock()
    service = make_service(clock)

    request_start = service.begin_sample()

    with pytest.raises(ValueError, match="UTC-aware"):
        service.complete_sample(
            request_start,
            datetime(2026, 1, 1, 12, 0),
        )


@pytest.mark.parametrize(
    "monotonic_value",
    [True, 1.5, "100"],
)
def test_invalid_monotonic_reading_is_rejected(monotonic_value):
    clock = FakeClock()
    service = make_service(
        clock,
        monotonic_clock_ns=lambda: monotonic_value,
    )

    with pytest.raises(TypeError, match="integer"):
        service.begin_sample()


@pytest.mark.parametrize(
    "max_samples",
    [True, 2.5, "9"],
)
def test_invalid_max_samples_is_rejected(max_samples):
    with pytest.raises(TypeError, match="integer"):
        TimeSynchronizationService(max_samples=max_samples)


def test_reset_clears_samples_and_status():
    clock = FakeClock()
    service = make_service(clock)

    add_three_samples(service, clock)

    assert (
        service.snapshot().status
        == TimeSyncStatus.SYNCHRONIZED
    )

    service.reset()

    snapshot = service.snapshot()

    assert snapshot.status == TimeSyncStatus.UNSYNCHRONIZED
    assert snapshot.sample_count == 0
    assert snapshot.offset_ms is None
