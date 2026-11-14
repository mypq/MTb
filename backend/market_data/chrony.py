
"""Read-only health checks for the local Chrony time service."""

from __future__ import annotations

import math
import os
import shutil
import subprocess
from dataclasses import dataclass
from typing import Sequence


REQUIRED_NTS_SOURCES = 3

REQUIRED_NTS_SOURCE_NAMES = frozenset({
    "time.cloudflare.com",
    "gbg1-ts.nts.netnod.se",
    "ntppool1.time.nl",
})


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
    healthy_sources: int = 0
    nts_authenticated_sources: int = 0
    quorum_met: bool = False
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
        if isinstance(timeout_seconds, bool) or not isinstance(
            timeout_seconds, (int, float)
        ):
            raise ValueError("timeout_seconds must be a finite positive number.")

        try:
            timeout_value = float(timeout_seconds)
        except (OverflowError, TypeError, ValueError) as exc:
            raise ValueError(
                "timeout_seconds must be a finite positive number."
            ) from exc

        if not math.isfinite(timeout_value) or timeout_value <= 0:
            raise ValueError("timeout_seconds must be a finite positive number.")

        if command is None:
            command = self._default_command()

        if (
            not command
            or any(
                not isinstance(part, str) or not part.strip()
                for part in command
            )
        ):
            raise ValueError("Chrony command must contain non-empty arguments.")

        self._command = tuple(command)
        self._timeout_seconds = timeout_value

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

    def _run_sources(self) -> str:
        """Read Chrony sources with the required WSL privilege."""
        if os.name == "nt":
            try:
                result = subprocess.run(
                    [
                        "wsl.exe",
                        "--exec",
                        "sudo",
                        "-n",
                        "/usr/bin/chronyc",
                        "-n",
                        "sources",
                        "-v",
                    ],
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
                    f"Unable to execute Chrony sources query: "
                    f"{type(exc).__name__}."
                ) from exc

            if result.returncode != 0:
                detail = result.stderr.strip() or result.stdout.strip()
                raise ChronyStatusError(
                    detail or (
                        "Chrony sources query exited with code "
                        f"{result.returncode}."
                    )
                )

            return result.stdout

        return self._run("-n", "sources", "-v")

    def _run_authdata(self) -> str:
        """Read NTS authentication records with the required WSL privilege."""
        if os.name == "nt":
            try:
                result = subprocess.run(
                    [
                        "wsl.exe",
                        "--exec",
                        "sudo",
                        "-n",
                        "/usr/bin/chronyc",
                        "authdata",
                    ],
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
                    f"Unable to execute Chrony authdata query: "
                    f"{type(exc).__name__}."
                ) from exc

            if result.returncode != 0:
                detail = result.stderr.strip() or result.stdout.strip()
                raise ChronyStatusError(
                    detail or (
                        "Chrony authdata query exited with code "
                        f"{result.returncode}."
                    )
                )

            return result.stdout

        return self._run("authdata")

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
        """Parse one complete numeric token and reject non-finite values."""
        parts = value.strip().split()

        if not parts:
            raise ChronyStatusError("Chrony returned an invalid time value.")

        try:
            parsed = float(parts[0])
        except (ValueError, OverflowError) as exc:
            raise ChronyStatusError(
                "Chrony returned an invalid time value."
            ) from exc

        if not math.isfinite(parsed):
            raise ChronyStatusError(
                "Chrony returned a non-finite time value."
            )

        return parsed

    @staticmethod
    def _parse_sources(output: str) -> tuple[str | None, int]:
        selected_source: str | None = None
        healthy_sources = 0

        for line in output.splitlines():
            fields = line.split()

            if len(fields) < 6:
                continue

            mode_state = fields[0]

            if (
                len(mode_state) != 2
                or mode_state[0] != "^"
                or mode_state[1] not in {"*", "+"}
            ):
                continue

            source = fields[1]

            try:
                stratum = int(fields[2])
                reach = int(fields[4], 8)
            except ValueError as exc:
                raise ChronyStatusError(
                    "Chrony returned invalid source statistics."
                ) from exc

            if stratum <= 0 or reach <= 0:
                continue

            healthy_sources += 1

            if mode_state[1] == "*":
                if selected_source is not None:
                    raise ChronyStatusError(
                        "Chrony returned multiple selected sources."
                    )

                selected_source = source

        return selected_source, healthy_sources

    @staticmethod
    def _parse_authdata(output: str) -> int:
        """Count healthy NTS authentication records.

        Only the three explicitly approved NTS sources are accepted.
        An empty table provides zero authenticated sources.
        """
        authenticated_sources = 0
        seen_sources: set[str] = set()
        header_found = False

        for line in output.splitlines():
            fields = line.split()

            if not fields:
                continue

            if fields[0] == "Name/IP":
                header_found = True
                continue

            if fields[0].startswith("="):
                continue

            if not header_found:
                continue

            if len(fields) != 10:
                raise ChronyStatusError(
                    "Chrony returned an invalid authdata row."
                )

            (
                source,
                mode,
                _key_id,
                _auth_type,
                _key_length,
                _last_attempt,
                attempts,
                nak,
                cookies,
                _cookie_length,
            ) = fields

            source_key = source.casefold()

            if source_key in seen_sources:
                raise ChronyStatusError(
                    "Chrony authdata contains duplicate sources."
                )

            seen_sources.add(source_key)

            if mode != "NTS":
                continue

            if source_key not in REQUIRED_NTS_SOURCE_NAMES:
                raise ChronyStatusError(
                    f"Unexpected NTS source: {source}."
                )

            try:
                attempts_count = int(attempts)
                nak_count = int(nak)
                cookie_count = int(cookies)
            except ValueError as exc:
                raise ChronyStatusError(
                    "Chrony returned invalid NTS authentication statistics."
                ) from exc

            if (
                attempts_count < 0
                or nak_count != 0
                or cookie_count <= 0
            ):
                continue

            authenticated_sources += 1

        if not header_found:
            raise ChronyStatusError(
                "Chrony authdata output is missing its expected header."
            )

        return authenticated_sources

    def snapshot(self) -> ChronyStatus:
        """Return current Chrony status; query failures are unavailable."""
        try:
            tracking = self._parse_tracking(
                self._run("-n", "tracking")
            )

            sources_output = self._run_sources()
            authdata_output = self._run_authdata()

            selected_source, healthy_sources = self._parse_sources(
                sources_output
            )

            authenticated_sources = self._parse_authdata(
                authdata_output
            )

            reference_id = tracking["Reference ID"]
            stratum = int(tracking["Stratum"].split()[0])
            leap_status = tracking["Leap status"]

            if not 0 <= stratum <= 16:
                raise ChronyStatusError(
                    "Chrony returned an invalid stratum value."
                )

            last_offset = self._parse_seconds(
                tracking["Last offset"]
            )
            root_dispersion = self._parse_seconds(
                tracking["Root dispersion"]
            )

            if root_dispersion < 0:
                raise ChronyStatusError(
                    "Chrony returned a negative root dispersion value."
                )

            quorum_met = (
                healthy_sources == REQUIRED_NTS_SOURCES
                and authenticated_sources == REQUIRED_NTS_SOURCES
            )

            synchronized = (
                stratum > 0
                and leap_status.lower() == "normal"
                and reference_id not in {"00000000", "00000000 ()"}
                and selected_source is not None
                and quorum_met
            )

            return ChronyStatus(
                available=True,
                synchronized=synchronized,
                reference_id=reference_id,
                stratum=stratum,
                leap_status=leap_status,
                last_offset_seconds=last_offset,
                root_dispersion_seconds=root_dispersion,
                selected_source=selected_source,
                healthy_sources=healthy_sources,
                nts_authenticated_sources=authenticated_sources,
                quorum_met=quorum_met,
                error=(
                    None
                    if quorum_met
                    else (
                        "NTS quorum not met: "
                        f"{healthy_sources} healthy Chrony sources "
                        f"(required exactly {REQUIRED_NTS_SOURCES}); "
                        f"{authenticated_sources} authenticated NTS sources "
                        f"(required exactly {REQUIRED_NTS_SOURCES})."
                    )
                ),
            )

        except (ChronyStatusError, ValueError, IndexError) as exc:
            return ChronyStatus(
                available=False,
                synchronized=False,
                error=str(exc),
            )
