"""Keep the local WSL Chrony service available while GOLD-BOT is running."""

from __future__ import annotations

import logging
import platform
import shutil
import subprocess
import threading
import time

logger = logging.getLogger(__name__)


class WslChronyKeepalive:
    """Hold one WSL command session open without terminating the distro."""

    def __init__(
        self,
        *,
        startup_timeout_seconds: float = 30.0,
        poll_interval_seconds: float = 0.25,
    ) -> None:
        if startup_timeout_seconds <= 0:
            raise ValueError("startup_timeout_seconds must be positive.")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive.")

        self._startup_timeout_seconds = float(startup_timeout_seconds)
        self._poll_interval_seconds = float(poll_interval_seconds)
        self._process: subprocess.Popen | None = None
        self._lock = threading.Lock()

    @property
    def is_running(self) -> bool:
        process = self._process
        return process is not None and process.poll() is None

    def start(self) -> None:
        """Start WSL and wait until its Chrony service reports active."""
        if platform.system() != "Windows":
            return

        with self._lock:
            if self.is_running:
                process = self._process
            else:
                executable = shutil.which("wsl.exe") or shutil.which("wsl")
                if executable is None:
                    raise RuntimeError("wsl.exe was not found on PATH.")

                creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
                process = subprocess.Popen(
                    [executable, "--exec", "sleep", "infinity"],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    creationflags=creation_flags,
                )
                self._process = process

        deadline = time.monotonic() + self._startup_timeout_seconds
        executable = shutil.which("wsl.exe") or shutil.which("wsl") or "wsl.exe"

        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(
                    "The WSL keepalive process exited before Chrony became ready."
                )

            remaining = max(0.1, deadline - time.monotonic())
            try:
                result = subprocess.run(
                    [
                        executable,
                        "--exec",
                        "systemctl",
                        "is-active",
                        "chrony",
                    ],
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    check=False,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=min(3.0, remaining),
                    shell=False,
                )
            except (OSError, subprocess.TimeoutExpired):
                result = None

            if (
                result is not None
                and result.returncode == 0
                and result.stdout.strip() == "active"
            ):
                logger.info(
                    "WSL keepalive is running and Chrony is active."
                )
                return

            time.sleep(self._poll_interval_seconds)

        raise TimeoutError(
            "WSL started, but Chrony did not become active within "
            f"{self._startup_timeout_seconds:.1f} seconds. "
            "The keepalive remains running so the NTP monitor can retry."
        )

    def stop(self) -> None:
        """Release only the WSL command started by this manager."""
        with self._lock:
            process = self._process
            self._process = None

        if process is None or process.poll() is not None:
            return

        process.terminate()
        try:
            process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5.0)

        logger.info("WSL Chrony keepalive process stopped.")
