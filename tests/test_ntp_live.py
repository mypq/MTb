
from __future__ import annotations

import os

import pytest

from backend.market_data.ntp_time import NtpTimeVerifier


def test_live_ntp_udp_123_verification() -> None:
    """
    Exercise real UDP/123 access only in the explicitly dispatched live job.

    The normal unit-test suite must remain deterministic and offline.
    """
    if os.getenv("GOLD_BOT_RUN_LIVE_NTP") != "1":
        pytest.skip(
            "Live NTP check is restricted to the manual GitHub Actions job."
        )

    verifier = NtpTimeVerifier(
        timeout_seconds=3.0,
        max_round_trip_seconds=2.5,
        max_root_dispersion_seconds=1.0,
        max_offset_seconds=1.0,
        max_uncertainty_seconds=1.0,
    )

    result = verifier.verify()

    print(
        "LIVE NTP RESULT: "
        f"verified={result.verified}, "
        f"sample_count={result.sample_count}, "
        f"offset_seconds={result.offset_seconds}, "
        f"uncertainty_seconds={result.uncertainty_seconds}, "
        f"servers={result.servers}, "
        f"error={result.error}"
    )

    assert result.verified is True, (
        "Live NTP verification failed. "
        f"sample_count={result.sample_count}; error={result.error}"
    )
    assert result.sample_count >= 3
    assert result.offset_seconds is not None
    assert abs(result.offset_seconds) < 1.0
    assert result.uncertainty_seconds is not None
    assert result.uncertainty_seconds <= 1.0
    assert verifier.is_trusted() is True
    assert verifier.last_verification() == result
