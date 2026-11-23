"""Keep the local WSL Chrony service available while GOLD-BOT is running."""

from __future__ import annotations

import logging
import platform
import shutil
import subprocess
import threading
import time
import uuid

logger = logging.getLogger(__name__)


class WslChronyKeepalive:
    """Manage one uniquely identified Linux keepalive process inside WSL."""

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
        self._lock = threading.Lock()
        self._executable: str | None = None
        self._pid: int | None = None
        self._marker: str | None = None

    @property
    def is_running(self) -> bool:
        """Return whether this manager currently owns a keepalive PID."""
        return self._pid is not None

    @staticmethod
    def _bash_command(executable: str, script: str, *args: str, timeout: float):
        return subprocess.run(
            [executable, "--exec", "/bin/bash", "-c", script, "_", *args],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            shell=False,
        )

    def start(self) -> None:
        """Start a uniquely marked WSL process and wait for Chrony readiness."""
        if platform.system() != "Windows":
            return

        with self._lock:
            if self._pid is None:
                executable = shutil.which("wsl.exe") or shutil.which("wsl")
                if executable is None:
                    raise RuntimeError("wsl.exe was not found on PATH.")

                marker = f"giant_goldbot_wsl_keepalive_{uuid.uuid4().hex}"
                launch_script = (
                    "nohup /bin/bash -c 'exec -a \"$1\" sleep infinity' "
                    "_ \"$1\" >/dev/null 2>&1 </dev/null & echo $!"
                )
                launched = self._bash_command(
                    executable,
                    launch_script,
                    marker,
                    timeout=30.0,
                )
                if launched.returncode != 0:
                    raise RuntimeError(
                        "Could not launch the managed WSL keepalive process: "
                        f"{launched.stderr.strip()}"
                    )
                try:
                    pid = int(launched.stdout.strip().splitlines()[-1])
                except (ValueError, IndexError) as exc:
                    raise RuntimeError(
                        "WSL did not return a valid keepalive process ID."
                    ) from exc

                self._executable = executable
                self._pid = pid
                self._marker = marker

            executable = self._executable
            pid = self._pid
            marker = self._marker

        assert executable is not None and pid is not None and marker is not None
        deadline = time.monotonic() + self._startup_timeout_seconds
        readiness_script = (
            'pid="$1"; marker="$2"; '
            '[ -r "/proc/$pid/cmdline" ] || exit 1; '
            'actual=$(tr "\\000" " " < "/proc/$pid/cmdline") || exit 1; '
            'case "$actual" in *"$marker"*) exit 0 ;; *) exit 1 ;; esac'
        )

        while time.monotonic() < deadline:
            remaining = max(0.1, deadline - time.monotonic())
            try:
                owned = self._bash_command(
                    executable,
                    readiness_script,
                    str(pid),
                    marker,
                    timeout=min(3.0, remaining),
                )
                if owned.returncode != 0:
                    raise RuntimeError(
                        "The managed WSL keepalive process exited unexpectedly."
                    )

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
                time.sleep(self._poll_interval_seconds)
                continue

            if result.returncode == 0 and result.stdout.strip() == "active":
                logger.info(
                    "Managed WSL keepalive process %s is running and Chrony is active.",
                    pid,
                )
                return

            time.sleep(self._poll_interval_seconds)

        raise TimeoutError(
            "WSL started, but Chrony did not become active within "
            f"{self._startup_timeout_seconds:.1f} seconds. "
            "The managed keepalive remains running so NTP verification can retry."
        )

    def stop(self) -> None:
        """Stop only this manager's marked Linux process; never terminate WSL."""
        with self._lock:
            executable = self._executable
            pid = self._pid
            marker = self._marker
            self._executable = None
            self._pid = None
            self._marker = None

        if platform.system() != "Windows" or executable is None or pid is None or marker is None:
            return

        stop_script = (
            'pid="$1"; marker="$2"; '
            '[ -r "/proc/$pid/cmdline" ] || exit 0; '
            'actual=$(tr "\\000" " " < "/proc/$pid/cmdline") || exit 0; '
            'case "$actual" in *"$marker"*) ;; *) exit 0 ;; esac; '
            'kill -TERM "$pid" 2>/dev/null || exit 0; '
            'i=0; while [ "$i" -lt 50 ]; do '
            '[ -r "/proc/$pid/cmdline" ] || exit 0; '
            'actual=$(tr "\\000" " " < "/proc/$pid/cmdline") || exit 0; '
            'case "$actual" in *"$marker"*) ;; *) exit 0 ;; esac; '
            'sleep 0.1; i=$((i + 1)); done; exit 2'
        )
        try:
            result = self._bash_command(
                executable,
                stop_script,
                str(pid),
                marker,
                timeout=8.0,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            logger.exception("Could not confirm WSL keepalive process shutdown.")
            raise RuntimeError(
                "Could not confirm that the managed WSL keepalive stopped."
            ) from exc

        if result.returncode == 2:
            raise RuntimeError(
                "The managed WSL keepalive did not stop within five seconds."
            )
        if result.returncode != 0:
            raise RuntimeError(
                "Could not safely stop the managed WSL keepalive: "
                f"{result.stderr.strip()}"
            )

        logger.info(
            "Managed WSL keepalive process %s stopped; WSL and Chrony were not terminated.",
            pid,
        )
