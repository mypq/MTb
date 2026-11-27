"""Keep the local WSL Chrony service available while GOLD-BOT is running."""

from __future__ import annotations

import asyncio
import logging
import math
import platform
import shutil
import subprocess
import threading
import time
import uuid

logger = logging.getLogger(__name__)


class WslChronyKeepalive:
    """Keep one uniquely identified Linux process attached to a managed wsl.exe."""

    def __init__(
        self,
        *,
        startup_timeout_seconds: float = 30.0,
        poll_interval_seconds: float = 0.25,
    ) -> None:
        self._startup_timeout_seconds = self._positive_interval(
            startup_timeout_seconds,
            "startup_timeout_seconds",
        )
        self._poll_interval_seconds = self._positive_interval(
            poll_interval_seconds,
            "poll_interval_seconds",
        )
        self._lock = threading.Lock()
        self._executable: str | None = None
        self._process: subprocess.Popen | None = None
        self._pid: int | None = None
        self._marker: str | None = None
        self._ready = False

    @staticmethod
    def _positive_interval(value: float, name: str) -> float:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"{name} must be a finite positive number.")
        return float(value)

    @property
    def is_running(self) -> bool:
        """Return whether the managed WSL process is still alive."""
        process = self._process
        return process is not None and process.poll() is None

    @property
    def is_ready(self) -> bool:
        """Return whether the managed process passed the Chrony readiness check."""
        process = self._process
        return (
            self._ready
            and process is not None
            and process.poll() is None
        )

    async def run(
        self,
        stop_event: asyncio.Event,
        *,
        retry_interval_seconds: float = 5.0,
        health_check_interval_seconds: float = 10.0,
    ) -> None:
        """Maintain WSL/Chrony for the application lifetime and recover exits."""
        if not isinstance(stop_event, asyncio.Event):
            raise TypeError("stop_event must be an asyncio.Event.")

        retry_interval = self._positive_interval(
            retry_interval_seconds,
            "retry_interval_seconds",
        )
        health_interval = self._positive_interval(
            health_check_interval_seconds,
            "health_check_interval_seconds",
        )

        if platform.system() != "Windows":
            await stop_event.wait()
            return

        logger.info(
            "WSL Chrony keepalive supervisor started: retry_seconds=%s "
            "health_check_seconds=%s",
            retry_interval,
            health_interval,
        )
        try:
            while not stop_event.is_set():
                try:
                    await asyncio.to_thread(self.start)
                except Exception:
                    logger.exception(
                        "WSL Chrony keepalive is not ready; automatic recovery "
                        "will retry in %.1f seconds.",
                        retry_interval,
                    )

                delay = health_interval if self.is_ready else retry_interval
                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=delay,
                    )
                except asyncio.TimeoutError:
                    continue
        finally:
            try:
                await asyncio.to_thread(self.stop)
            except Exception:
                logger.exception(
                    "WSL Chrony keepalive supervisor could not confirm shutdown."
                )
            logger.info("WSL Chrony keepalive supervisor stopped.")

    @staticmethod
    def _run_wsl_command(
        executable: str,
        script: str,
        *args: str,
        timeout: float,
    ) -> subprocess.CompletedProcess:
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
        """Start WSL as a managed child and wait for its marked process and Chrony."""
        if platform.system() != "Windows":
            return

        with self._lock:
            if self._process is not None and self._process.poll() is not None:
                logger.warning(
                    "Managed WSL process exited; clearing stale state before recovery."
                )
                self._executable = None
                self._process = None
                self._pid = None
                self._marker = None
                self._ready = False

            if self._process is None:
                executable = shutil.which("wsl.exe") or shutil.which("wsl")
                if executable is None:
                    raise RuntimeError("wsl.exe was not found on PATH.")

                marker = f"giant_goldbot_wsl_keepalive_{uuid.uuid4().hex}"
                launch_script = (
                    'echo "$$" > "/tmp/$1.pid"; '
                    'exec -a "$1" sleep infinity'
                )
                try:
                    process = subprocess.Popen(
                        [
                            executable,
                            "--exec",
                            "/bin/bash",
                            "-c",
                            launch_script,
                            "_",
                            marker,
                        ],
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        shell=False,
                    )
                except OSError as exc:
                    raise RuntimeError(
                        f"Could not start managed WSL process: {exc}"
                    ) from exc

                self._executable = executable
                self._process = process
                self._pid = None
                self._marker = marker

            executable = self._executable
            process = self._process
            marker = self._marker
            self._ready = False

        assert executable is not None and process is not None and marker is not None
        deadline = time.monotonic() + self._startup_timeout_seconds
        readiness_script = (
            'pid_file="/tmp/$1.pid"; '
            '[ -r "$pid_file" ] || exit 1; '
            'pid=$(cat "$pid_file") || exit 1; '
            'case "$pid" in ""|*[!0-9]*) exit 2 ;; esac; '
            '[ -r "/proc/$pid/cmdline" ] || exit 1; '
            'actual=$(tr "\\000" " " < "/proc/$pid/cmdline") || exit 1; '
            'case "$actual" in *"$1"*) ;; *) exit 3 ;; esac; '
            'chrony_state=$(systemctl is-active chrony 2>/dev/null) || '
            '{ printf "CHRONY_STATE=%s\\n" "$chrony_state" >&2; exit 4; }; '
            '[ "$chrony_state" = "active" ] || '
            '{ printf "CHRONY_STATE=%s\\n" "$chrony_state" >&2; exit 4; }; '
            'printf "%s\\n" "$pid"; exit 0'
        )
        last_readiness_detail = "No readiness response was received."

        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(
                    "The managed WSL process exited before keepalive readiness."
                )

            remaining = max(0.1, deadline - time.monotonic())
            try:
                result = self._run_wsl_command(
                    executable,
                    readiness_script,
                    marker,
                    # WSL cold-starts can take longer than three seconds.
                    # Repeated short timeouts kill the client command while
                    # WSL is still initializing and can queue competing requests.
                    timeout=remaining,
                )
                if result.returncode == 0:
                    try:
                        pid = int(result.stdout.strip().splitlines()[-1])
                    except (ValueError, IndexError) as exc:
                        raise RuntimeError(
                            "WSL did not return a valid managed process ID."
                        ) from exc

                    with self._lock:
                        self._pid = pid
                        self._ready = True

                    logger.info(
                        "Managed WSL process %s is running and Chrony is active.",
                        pid,
                    )
                    return

                if result.returncode in (2, 3):
                    raise RuntimeError(
                        "The WSL keepalive PID file or process identity is invalid."
                    )

                last_readiness_detail = (
                    result.stderr.strip()
                    or result.stdout.strip()
                    or f"Readiness command exited with code {result.returncode}."
                )
            except subprocess.TimeoutExpired:
                last_readiness_detail = (
                    "The combined WSL process-identity and Chrony readiness "
                    "check exceeded its command timeout."
                )
            except OSError as exc:
                last_readiness_detail = f"WSL readiness command failed: {exc}"

            logger.debug(
                "WSL Chrony keepalive is not ready yet: %s",
                last_readiness_detail,
            )
            time.sleep(self._poll_interval_seconds)

        raise TimeoutError(
            "WSL started, but the managed process and active Chrony service "
            f"were not ready within {self._startup_timeout_seconds:.1f} seconds. "
            f"Last readiness result: {last_readiness_detail} "
            "The managed WSL process remains attached so readiness can be retried."
        )

    def stop(self) -> None:
        """Stop only the marked Linux process, then wait for its WSL parent to exit."""
        with self._lock:
            executable = self._executable
            process = self._process
            pid = self._pid
            marker = self._marker

        if (
            platform.system() != "Windows"
            or executable is None
            or process is None
            or marker is None
        ):
            return

        if process.poll() is not None:
            with self._lock:
                self._executable = None
                self._process = None
                self._pid = None
                self._marker = None
                self._ready = False
            return

        stop_script = (
            'pid_file="/tmp/$1.pid"; '
            '[ -r "$pid_file" ] || exit 3; '
            'pid=$(cat "$pid_file") || exit 3; '
            'case "$pid" in ""|*[!0-9]*) exit 3 ;; esac; '
            '[ -r "/proc/$pid/cmdline" ] || { rm -f "$pid_file"; exit 0; }; '
            'actual=$(tr "\\000" " " < "/proc/$pid/cmdline") || exit 0; '
            'case "$actual" in *"$2"*) ;; *) rm -f "$pid_file"; exit 0 ;; esac; '
            'kill -TERM "$pid" 2>/dev/null || exit 0; '
            'i=0; while [ "$i" -lt 50 ]; do '
            '[ -r "/proc/$pid/cmdline" ] || { rm -f "$pid_file"; exit 0; }; '
            'actual=$(tr "\\000" " " < "/proc/$pid/cmdline") || '
            '{ rm -f "$pid_file"; exit 0; }; '
            'case "$actual" in *"$2"*) ;; *) rm -f "$pid_file"; exit 0 ;; esac; '
            'sleep 0.1; i=$((i + 1)); done; exit 2'
        )

        try:
            result = self._run_wsl_command(
                executable,
                stop_script,
                marker,
                marker,
                timeout=max(20.0, self._startup_timeout_seconds),
            )
            if result.returncode == 2:
                raise RuntimeError(
                    "The managed Linux keepalive did not stop within five seconds."
                )
            if result.returncode != 0:
                raise RuntimeError(
                    "Could not safely stop the managed Linux keepalive "
                    f"(exit={result.returncode}): "
                    f"{result.stderr.strip() or result.stdout.strip() or 'no command output'}"
                )
            process.wait(timeout=8.0)
        except (OSError, subprocess.TimeoutExpired) as exc:
            logger.exception("Could not confirm managed WSL keepalive shutdown.")
            raise RuntimeError(
                "Could not confirm that the managed WSL keepalive stopped."
            ) from exc

        with self._lock:
            if self._process is process:
                self._executable = None
                self._process = None
                self._pid = None
                self._marker = None
                self._ready = False

        logger.info(
            "Managed WSL keepalive process %s stopped; WSL may shut down when no other Linux processes remain.",
            pid,
        )
