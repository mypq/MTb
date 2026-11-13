
"""Read-only health checks for the local Chrony time service."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True, slots=True)
class ChronyStatus:
    """A read-only snapshot of Chrony's synchronization state."""

    available: bool
    synchronized: bool
    reference_id: str | None = None
    stratum: int | None = None
    leap_status: str | None = None
    last_offset_seconds: float | None = None
    root_dispersion_seconds: float | None = None
    selected_source: str | None = None
    error: str | None = None


class ChronyStatusError(RuntimeError):
    """Raised when Chrony status cannot be queried or parsed."""


class ChronyStatusService:
    """Query Chrony without changing the operating-system clock."""

    def __init__(
        self,
        command: Sequence[str] | None = None,
        *,
        timeout_seconds: float = 3.0,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero.")

        if command is None:
            command = self._default_command()

        if not command or any(not part for part in command):
            raise ValueError("Chrony command must contain non-empty arguments.")

        self._command = tuple(command)
        self._timeout_seconds = timeout_seconds

    @staticmethod
    def _default_command() -> tuple[str, ...]:
        if os.name == "nt":
            wsl = shutil.which("wsl.exe") or shutil.which("wsl")
            if wsl is None:
                return ("wsl.exe", "--exec", "chronyc")
            return (wsl, "--exec", "chronyc")

        chronyc = shutil.which("chronyc")
        return (chronyc or "chronyc",)

    def _run(self, *arguments: str) -> str:
        try:
            result = subprocess.run(
                [*self._command, *arguments],
                capture_output=True,
                check=False,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self._timeout_seconds,
                shell=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ChronyStatusError(
                f"Unable to execute Chrony query: {type(exc).__name__}."
            ) from exc

        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            raise ChronyStatusError(
                detail or f"Chrony query exited with code {result.returncode}."
            )

        return result.stdout

    @staticmethod
    def _parse_tracking(output: str) -> dict[str, str]:
        parsed: dict[str, str] = {}

        for line in output.splitlines():
            if ":" not in line:
                continue

            key, value = line.split(":", 1)
            parsed[key.strip()] = value.strip()

        required = {
            "Reference ID",
            "Stratum",
            "Last offset",
            "Root dispersion",
            "Leap status",
        }

        missing = required.difference(parsed)

        if missing:
            raise ChronyStatusError(
                "Chrony tracking output is missing required fields: "
                + ", ".join(sorted(missing))
            )

        return parsed

    @staticmethod
    def _parse_seconds(value: str) -> float:
        match = re.search(
            r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)",
            value,
        )

        if match is None:
            raise ChronyStatusError("Chrony returned an invalid time value.")

        return float(match.group())

    @staticmethod
    def _parse_selected_source(output: str) -> str | None:
        for line in output.splitlines():
            fields = line.split()

            if len(fields) >= 2 and fields[0] == "^*":
                return fields[1]

        return None

    def snapshot(self) -> ChronyStatus:
        """Return current Chrony status; failures are reported as unavailable."""
        try:
            tracking = self._parse_tracking(
                self._run("-n", "tracking")
            )

            sources = self._run("-n", "sources", "-v")

            reference_id = tracking["Reference ID"]
            stratum = int(tracking["Stratum"].split()[0])
            leap_status = tracking["Leap status"]
            selected_source = self._parse_selected_source(sources)

            synchronized = (
                stratum > 0
                and leap_status.lower() == "normal"
                and reference_id not in {"00000000", "00000000 ()"}
                and selected_source is not None
            )

            return ChronyStatus(
                available=True,
                synchronized=synchronized,
                reference_id=reference_id,
                stratum=stratum,
                leap_status=leap_status,
                last_offset_seconds=self._parse_seconds(
                    tracking["Last offset"]
                ),
                root_dispersion_seconds=self._parse_seconds(
                    tracking["Root dispersion"]
                ),
                selected_source=selected_source,
            )

        except (ChronyStatusError, ValueError, IndexError) as exc:
            return ChronyStatus(
                available=False,
                synchronized=False,
                error=str(exc),
            )
