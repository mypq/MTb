"""Cross-process exclusive lock for MT5 clock-history access."""

from __future__ import annotations

import os
import time
from pathlib import Path


class ClockHistoryLockError(RuntimeError):
    """The clock-history lock could not be acquired safely."""


class ClockHistoryLockBusyError(ClockHistoryLockError):
    """The writer currently holds the clock-history lock."""


class ClockHistoryFileLock:
    """Exclusive lock compatible with MT5 on Windows."""

    def __init__(
        self,
        path: str | Path,
        *,
        timeout_seconds: float = 1.0,
        retry_interval_seconds: float = 0.05,
    ) -> None:
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds cannot be negative.")
        if retry_interval_seconds <= 0:
            raise ValueError("retry_interval_seconds must be positive.")

        self.path = Path(path)
        self.timeout_seconds = timeout_seconds
        self.retry_interval_seconds = retry_interval_seconds
        self._handle = None

    def __enter__(self) -> "ClockHistoryFileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.release()

    def acquire(self) -> None:
        if self._handle is not None:
            raise ClockHistoryLockError("Lock is already held.")

        self.path.parent.mkdir(parents=True, exist_ok=True)

        deadline = time.monotonic() + self.timeout_seconds

        while True:
            try:
                if os.name == "nt":
                    self._acquire_windows()
                else:
                    self._acquire_posix()
                return
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise ClockHistoryLockBusyError(
                        "Timed out waiting for clock-history lock."
                    ) from None
                time.sleep(self.retry_interval_seconds)

    def _acquire_windows(self) -> None:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

        create_file = kernel32.CreateFileW
        create_file.argtypes = (
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.HANDLE,
        )
        create_file.restype = wintypes.HANDLE

        handle = create_file(
            str(self.path),
            0x80000000 | 0x40000000,  # GENERIC_READ | GENERIC_WRITE
            0,                       # No sharing: exclusive access
            None,
            4,                       # OPEN_ALWAYS; never truncate
            0x00000080,               # FILE_ATTRIBUTE_NORMAL
            None,
        )

        invalid_handle = ctypes.c_void_p(-1).value

        if handle == invalid_handle:
            error = ctypes.get_last_error()

            if error in (32, 33):
                raise BlockingIOError(error, "Lock is busy.")

            raise ClockHistoryLockError(
                f"Windows lock acquisition failed. error={error}"
            )

        self._handle = ("windows", kernel32, handle)

    def _acquire_posix(self) -> None:
        import fcntl

        descriptor = os.open(
            self.path,
            os.O_CREAT | os.O_RDWR,
            0o600,
        )

        try:
            fcntl.flock(
                descriptor,
                fcntl.LOCK_EX | fcntl.LOCK_NB,
            )
        except BlockingIOError:
            os.close(descriptor)
            raise

        self._handle = ("posix", descriptor)

    def release(self) -> None:
        if self._handle is None:
            return

        kind, *values = self._handle
        self._handle = None

        if kind == "windows":
            _, kernel32, handle = ("windows", *values)
            if not kernel32.CloseHandle(handle):
                error = __import__("ctypes").get_last_error()
                raise ClockHistoryLockError(
                    f"Windows lock release failed. error={error}"
                )
            return

        import fcntl

        descriptor = values[0]
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)
