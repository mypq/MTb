from __future__ import annotations

import pytest

from backend.market_data.clock_history_lock import (
    ClockHistoryFileLock,
    ClockHistoryLockError,
)


def test_lock_creates_file_without_truncating_existing_content(tmp_path):
    path = tmp_path / "clock_history.writer.lock"
    path.write_bytes(b"preserve-this")

    with ClockHistoryFileLock(path):
        assert path.exists()

    assert path.read_bytes() == b"preserve-this"


def test_lock_prevents_second_lock_until_release(tmp_path):
    path = tmp_path / "clock_history.writer.lock"

    first = ClockHistoryFileLock(
        path,
        timeout_seconds=0,
    )
    second = ClockHistoryFileLock(
        path,
        timeout_seconds=0,
    )

    first.acquire()

    try:
        with pytest.raises(ClockHistoryLockError):
            second.acquire()
    finally:
        first.release()

    with second:
        assert path.exists()


def test_lock_can_be_reacquired_after_release(tmp_path):
    path = tmp_path / "clock_history.writer.lock"

    with ClockHistoryFileLock(path):
        pass

    with ClockHistoryFileLock(path):
        assert path.exists()


def test_lock_timeout_does_not_leave_lock_held(tmp_path):
    path = tmp_path / "clock_history.writer.lock"

    first = ClockHistoryFileLock(path)
    second = ClockHistoryFileLock(
        path,
        timeout_seconds=0,
    )

    first.acquire()

    try:
        with pytest.raises(ClockHistoryLockError):
            second.acquire()
    finally:
        first.release()

    with ClockHistoryFileLock(path):
        assert path.exists()


def test_invalid_retry_interval_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        ClockHistoryFileLock(
            tmp_path / "lock",
            retry_interval_seconds=0,
        )



@pytest.mark.parametrize(
    "timeout",
    [-1, float("nan"), float("inf"), float("-inf"), True, "1"],
)
def test_invalid_timeout_values_are_rejected(tmp_path, timeout):
    with pytest.raises(ValueError, match="finite non-negative number"):
        ClockHistoryFileLock(
            tmp_path / "lock",
            timeout_seconds=timeout,
        )


@pytest.mark.parametrize(
    "retry_interval",
    [0, -0.1, float("nan"), float("inf"), float("-inf"), False, "0.1"],
)
def test_invalid_retry_interval_values_are_rejected(
    tmp_path,
    retry_interval,
):
    with pytest.raises(ValueError, match="finite positive number"):
        ClockHistoryFileLock(
            tmp_path / "lock",
            retry_interval_seconds=retry_interval,
        )


def test_zero_timeout_is_allowed(tmp_path):
    lock = ClockHistoryFileLock(
        tmp_path / "lock",
        timeout_seconds=0,
    )
    assert lock.timeout_seconds == 0.0
