from __future__ import annotations

from dataclasses import replace

import pytest

from backend.market_data.broker_offset_detector import (
    BrokerOffsetDetector,
    BrokerOffsetStatus,
)
from backend.market_data.clock_history import MT5ClockHistorySample
from backend.market_data.ntp_time import (
    NtpSample,
    NtpTimeVerifier,
    NtpVerification,
)


BASE_EPOCH = 1_794_952_458


def make_verifier(
    *,
    verify: bool = True,
    ntp_offset_seconds: float = 0.0,
) -> NtpTimeVerifier:
    verifier = NtpTimeVerifier(
        servers=("ntp-a", "ntp-b", "ntp-c"),
        wall_clock=lambda: float(BASE_EPOCH),
        monotonic_clock=lambda: 100.0,
        query=lambda server: NtpSample(
            server=server,
            offset_seconds=ntp_offset_seconds,
            round_trip_seconds=0.02,
            root_dispersion_seconds=0.01,
        ),
    )

    if verify:
        result = verifier.verify()
        assert result.verified

    return verifier


def make_sample(
    *,
    run_id: str = "run-001",
    sequence: int = 1,
    monotonic_ms: int = 1000,
    offset: int = 7200,
    account_login: int = 337260380,
    broker_server: str = "XMGlobal-MT5 9",
    symbol: str = "GOLD",
    terminal_build: int = 6316,
    connected: bool = True,
) -> MT5ClockHistorySample:
    gmt = BASE_EPOCH + (sequence - 1) * 60

    return MT5ClockHistorySample(
        schema_version=1,
        run_id=run_id,
        sample_sequence=sequence,
        monotonic_ms=monotonic_ms,
        time_gmt_epoch=gmt,
        time_trade_server_epoch=gmt + offset,
        time_current_quote_epoch=gmt + offset,
        time_local_epoch=gmt,
        server_utc_offset_seconds=offset,
        account_login=account_login,
        broker_server=broker_server,
        symbol=symbol,
        terminal_build=terminal_build,
        terminal_connected=connected,
    )


def test_candidate_requires_three_consecutive_samples() -> None:
    detector = BrokerOffsetDetector(make_verifier())

    first = detector.observe(make_sample(sequence=1))
    second = detector.observe(
        make_sample(sequence=2, monotonic_ms=61000)
    )
    third = detector.observe(
        make_sample(sequence=3, monotonic_ms=121000)
    )

    assert first.status == BrokerOffsetStatus.COLLECTING
    assert first.sample_count == 1

    assert second.status == BrokerOffsetStatus.COLLECTING
    assert second.sample_count == 2

    assert third.status == BrokerOffsetStatus.CANDIDATE_READY
    assert third.candidate_ready is True
    assert third.offset_seconds == 7200
    assert third.sample_count == 3
    assert third.run_id == "run-001"
    assert third.identity == (
        337260380,
        "XMGlobal-MT5 9",
        "GOLD",
        6316,
    )


def test_candidate_is_not_ready_after_two_samples() -> None:
    detector = BrokerOffsetDetector(make_verifier())

    detector.observe(make_sample(sequence=1))
    result = detector.observe(
        make_sample(sequence=2, monotonic_ms=61000)
    )

    assert result.candidate_ready is False
    assert result.offset_seconds is None
    assert result.sample_count == 2


def test_run_id_change_restarts_consecutive_sequence() -> None:
    detector = BrokerOffsetDetector(make_verifier())

    detector.observe(make_sample(sequence=1))

    second = detector.observe(
        make_sample(
            run_id="run-002",
            sequence=1,
            monotonic_ms=1000,
        )
    )

    assert second.status == BrokerOffsetStatus.COLLECTING
    assert second.sample_count == 1
    assert second.run_id == "run-002"

    detector.observe(
        make_sample(
            run_id="run-002",
            sequence=2,
            monotonic_ms=61000,
        )
    )

    result = detector.observe(
        make_sample(
            run_id="run-002",
            sequence=3,
            monotonic_ms=121000,
        )
    )

    assert result.candidate_ready is True
    assert result.offset_seconds == 7200


def test_identity_change_restarts_sequence() -> None:
    detector = BrokerOffsetDetector(make_verifier())

    detector.observe(make_sample(sequence=1))

    changed = detector.observe(
        make_sample(
            sequence=2,
            monotonic_ms=61000,
            broker_server="Different-Broker",
        )
    )

    assert changed.status == BrokerOffsetStatus.COLLECTING
    assert changed.sample_count == 1

    detector.observe(
        make_sample(
            sequence=3,
            monotonic_ms=121000,
            broker_server="Different-Broker",
        )
    )

    result = detector.observe(
        make_sample(
            sequence=4,
            monotonic_ms=181000,
            broker_server="Different-Broker",
        )
    )

    assert result.candidate_ready is True
    assert result.identity[1] == "Different-Broker"


def test_sequence_gap_restarts_collection() -> None:
    detector = BrokerOffsetDetector(make_verifier())

    detector.observe(make_sample(sequence=1))

    result = detector.observe(
        make_sample(sequence=3, monotonic_ms=61000)
    )

    assert result.status == BrokerOffsetStatus.COLLECTING
    assert result.sample_count == 1


def test_offset_change_restarts_collection() -> None:
    detector = BrokerOffsetDetector(make_verifier())

    detector.observe(make_sample(sequence=1, offset=7200))

    changed = detector.observe(
        make_sample(
            sequence=2,
            monotonic_ms=61000,
            offset=10800,
        )
    )

    assert changed.status == BrokerOffsetStatus.COLLECTING
    assert changed.sample_count == 1

    detector.observe(
        make_sample(
            sequence=3,
            monotonic_ms=121000,
            offset=10800,
        )
    )

    result = detector.observe(
        make_sample(
            sequence=4,
            monotonic_ms=181000,
            offset=10800,
        )
    )

    assert result.candidate_ready is True
    assert result.offset_seconds == 10800


def test_untrusted_ntp_prevents_candidate() -> None:
    detector = BrokerOffsetDetector(make_verifier(verify=False))

    result = detector.observe(make_sample())

    assert result.status == BrokerOffsetStatus.WAITING_FOR_NTP
    assert result.candidate_ready is False
    assert result.offset_seconds is None
    assert result.sample_count == 0


def test_disconnected_terminal_is_rejected() -> None:
    detector = BrokerOffsetDetector(make_verifier())

    result = detector.observe(
        make_sample(connected=False)
    )

    assert result.status == BrokerOffsetStatus.DEGRADED
    assert result.candidate_ready is False
    assert result.sample_count == 0


def test_reset_discards_collected_samples() -> None:
    detector = BrokerOffsetDetector(make_verifier())

    detector.observe(make_sample(sequence=1))
    detector.observe(
        make_sample(sequence=2, monotonic_ms=61000)
    )

    detector.reset()

    result = detector.observe(
        make_sample(sequence=3, monotonic_ms=121000)
    )

    assert result.status == BrokerOffsetStatus.COLLECTING
    assert result.sample_count == 1
    assert result.candidate_ready is False


def test_invalid_required_sample_count_is_rejected() -> None:
    verifier = make_verifier()

    with pytest.raises(ValueError):
        BrokerOffsetDetector(verifier, required_samples=4)


def test_invalid_ntp_verifier_is_rejected() -> None:
    with pytest.raises(TypeError):
        BrokerOffsetDetector(None)  # type: ignore[arg-type]


def test_sample_older_than_ntp_freshness_window_is_rejected() -> None:
    detector = BrokerOffsetDetector(make_verifier())

    sample = make_sample(sequence=1)
    stale_sample = replace(
        sample,
        time_gmt_epoch=BASE_EPOCH - (90 * 60) - 1,
        time_trade_server_epoch=(
            BASE_EPOCH - (90 * 60) - 1 + 7200
        ),
        time_current_quote_epoch=(
            BASE_EPOCH - (90 * 60) - 1 + 7200
        ),
    )

    result = detector.observe(stale_sample)

    assert result.status == BrokerOffsetStatus.WAITING_FOR_NTP
    assert result.candidate_ready is False
    assert result.sample_count == 0


def test_sample_at_ntp_freshness_boundary_is_accepted() -> None:
    detector = BrokerOffsetDetector(make_verifier())

    sample = make_sample(sequence=1)
    boundary_sample = replace(
        sample,
        time_gmt_epoch=BASE_EPOCH - (90 * 60),
        time_trade_server_epoch=(
            BASE_EPOCH - (90 * 60) + 7200
        ),
        time_current_quote_epoch=(
            BASE_EPOCH - (90 * 60) + 7200
        ),
    )

    result = detector.observe(boundary_sample)

    assert result.status == BrokerOffsetStatus.COLLECTING
    assert result.sample_count == 1


def test_samples_too_close_together_restart_collection() -> None:
    detector = BrokerOffsetDetector(make_verifier())

    first = detector.observe(
        make_sample(sequence=1, monotonic_ms=1000)
    )

    second = detector.observe(
        make_sample(sequence=2, monotonic_ms=3000)
    )

    assert first.sample_count == 1
    assert second.status == BrokerOffsetStatus.COLLECTING
    assert second.sample_count == 1
    assert second.candidate_ready is False


def test_samples_at_minimum_interval_can_form_candidate() -> None:
    detector = BrokerOffsetDetector(make_verifier())

    detector.observe(
        make_sample(sequence=1, monotonic_ms=1000)
    )

    detector.observe(
        make_sample(sequence=2, monotonic_ms=4000)
    )

    result = detector.observe(
        make_sample(sequence=3, monotonic_ms=7000)
    )

    assert result.status == BrokerOffsetStatus.CANDIDATE_READY
    assert result.candidate_ready is True
    assert result.offset_seconds == 7200
    assert result.sample_count == 3


def test_non_finite_ntp_verification_time_is_rejected() -> None:
    verifier = make_verifier()
    verification = verifier.last_verification()

    assert verification is not None

    verifier._last_verification = replace(
        verification,
        checked_at_unix=float("nan"),
    )

    detector = BrokerOffsetDetector(verifier)
    result = detector.observe(make_sample())

    assert result.status == BrokerOffsetStatus.WAITING_FOR_NTP
    assert result.candidate_ready is False
    assert result.sample_count == 0


def test_invalid_minimum_sample_interval_is_rejected() -> None:
    verifier = make_verifier()

    with pytest.raises(ValueError):
        BrokerOffsetDetector(
            verifier,
            minimum_sample_interval_seconds=0,
        )


@pytest.mark.parametrize(
    ("ntp_offset_seconds", "raw_offset_seconds"),
    [
        (-0.75, 7199),
        (0.75, 7201),
    ],
)
def test_candidate_corrects_raw_clock_difference_with_ntp(
    ntp_offset_seconds: float,
    raw_offset_seconds: int,
) -> None:
    detector = BrokerOffsetDetector(
        make_verifier(
            ntp_offset_seconds=ntp_offset_seconds
        )
    )

    detector.observe(
        make_sample(sequence=1, offset=raw_offset_seconds)
    )
    detector.observe(
        make_sample(
            sequence=2,
            monotonic_ms=61000,
            offset=raw_offset_seconds,
        )
    )

    result = detector.observe(
        make_sample(
            sequence=3,
            monotonic_ms=121000,
            offset=raw_offset_seconds,
        )
    )

    assert result.status == BrokerOffsetStatus.CANDIDATE_READY
    assert result.offset_seconds == 7200

    assert all(
        sample.calculated_offset_seconds
        == pytest.approx(raw_offset_seconds - ntp_offset_seconds)
        for sample in result.evidence_samples
    )

    assert all(
        sample.ntp_offset_seconds == ntp_offset_seconds
        for sample in result.evidence_samples
    )


@pytest.mark.parametrize(
    "ntp_offset_seconds",
    [-120.0, 120.0],
)
def test_large_positive_and_negative_ntp_offsets_block_candidate(
    ntp_offset_seconds: float,
) -> None:
    verifier = make_verifier(
        verify=False,
        ntp_offset_seconds=ntp_offset_seconds,
    )

    verification = verifier.verify()
    assert verification.verified is False

    detector = BrokerOffsetDetector(verifier)

    result = detector.observe(make_sample())

    assert result.status == BrokerOffsetStatus.WAITING_FOR_NTP
    assert result.candidate_ready is False
    assert result.offset_seconds is None
    assert result.sample_count == 0


def test_candidate_uses_ntp_snapshot_captured_with_final_sample(
    monkeypatch,
) -> None:
    verifier = make_verifier()
    original = verifier.last_verification()

    assert original is not None
    assert isinstance(original, NtpVerification)

    final_snapshot = replace(
        original,
        checked_at_unix=float(BASE_EPOCH + 120),
        offset_seconds=0.1,
        uncertainty_seconds=0.2,
    )

    newer_snapshot = replace(
        original,
        checked_at_unix=float(BASE_EPOCH + 120),
        offset_seconds=0.3,
        uncertainty_seconds=0.4,
    )

    snapshots = iter(
        (
            original,
            original,
            final_snapshot,
            newer_snapshot,
        )
    )

    calls = 0

    def changing_verification():
        nonlocal calls
        calls += 1
        return next(snapshots)

    monkeypatch.setattr(
        verifier,
        "last_verification",
        changing_verification,
    )

    detector = BrokerOffsetDetector(verifier)

    detector.observe(make_sample(sequence=1))
    detector.observe(
        make_sample(sequence=2, monotonic_ms=61000)
    )

    result = detector.observe(
        make_sample(sequence=3, monotonic_ms=121000)
    )

    assert result.status == BrokerOffsetStatus.CANDIDATE_READY
    assert result.candidate_ready is True

    assert calls == 3

    assert result.ntp_checked_at_unix == (
        final_snapshot.checked_at_unix
    )
    assert result.ntp_offset_seconds == (
        final_snapshot.offset_seconds
    )
    assert result.ntp_uncertainty_seconds == (
        final_snapshot.uncertainty_seconds
    )

    final_sample = result.evidence_samples[-1]

    assert final_sample.ntp_checked_at_unix == (
        final_snapshot.checked_at_unix
    )
    assert final_sample.ntp_offset_seconds == (
        final_snapshot.offset_seconds
    )
    assert final_sample.ntp_uncertainty_seconds == (
        final_snapshot.uncertainty_seconds
    )
