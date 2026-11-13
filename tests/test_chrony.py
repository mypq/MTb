from subprocess import CompletedProcess, TimeoutExpired
from unittest.mock import patch

import pytest

from backend.market_data.chrony import ChronyStatusService

TRACKING_OUTPUT = """
Reference ID    : A29FC87B (time.cloudflare.com)
Stratum         : 4
Ref time (UTC)  : Fri Nov 13 04:07:58 2026
Last offset     : -0.000281089 seconds
Root dispersion : 0.002205403 seconds
Leap status     : Normal
"""

SOURCES_OUTPUT = """
MS Name/IP address         Stratum Poll Reach LastRx Last sample
===============================================================================
^* time.cloudflare.com           3   6   377    12   -281us[ -300us] +/-  2205us
^+ nts.netnod.se                 1   6   377    10   +120us[ +100us] +/-  3000us
"""


def successful_command(*args, **kwargs):
    cmd = args[0] if args else []
    cmd_str = " ".join(cmd) if isinstance(cmd, list) else str(cmd)

    if "tracking" in cmd_str:
        return CompletedProcess(args, 0, TRACKING_OUTPUT, "")

    if "sources" in cmd_str:
        return CompletedProcess(args, 0, SOURCES_OUTPUT, "")

    return CompletedProcess(args, 1, "", "Unexpected command")


def test_chrony_reports_synchronized_with_selected_source():
    service = ChronyStatusService(command=["chronyc"])

    with patch(
        "backend.market_data.chrony.subprocess.run",
        side_effect=successful_command,
    ):
        status = service.snapshot()

    assert status.available is True
    assert status.synchronized is True
    assert status.stratum == 4
    assert status.leap_status == "Normal"
    assert status.selected_source == "time.cloudflare.com"
    assert status.last_offset_seconds == -0.000281089
    assert status.root_dispersion_seconds == 0.002205403


def test_chrony_does_not_treat_candidate_as_selected_source():
    service = ChronyStatusService(command=["chronyc"])
    candidate_only = SOURCES_OUTPUT.replace("^*", "^ ")

    def command_without_selected(*args, **kwargs):
        cmd = args[0] if args else []
        cmd_str = " ".join(cmd) if isinstance(cmd, list) else str(cmd)

        if "tracking" in cmd_str:
            return CompletedProcess(args, 0, TRACKING_OUTPUT, "")

        return CompletedProcess(args, 0, candidate_only, "")

    with patch(
        "backend.market_data.chrony.subprocess.run",
        side_effect=command_without_selected,
    ):
        status = service.snapshot()

    assert status.available is True
    assert status.synchronized is False
    assert status.selected_source is None


def test_chrony_reports_unavailable_when_command_fails():
    service = ChronyStatusService(command=["chronyc"])

    with patch(
        "backend.market_data.chrony.subprocess.run",
        side_effect=FileNotFoundError,
    ):
        status = service.snapshot()

    assert status.available is False
    assert status.synchronized is False
    assert "Unable to execute" in status.error


def test_chrony_reports_timeout():
    service = ChronyStatusService(command=["chronyc"])

    with patch(
        "backend.market_data.chrony.subprocess.run",
        side_effect=TimeoutExpired("chronyc", 3),
    ):
        status = service.snapshot()

    assert status.available is False
    assert status.synchronized is False
    assert "TimeoutExpired" in status.error


def test_chrony_rejects_incomplete_tracking_output():
    service = ChronyStatusService(command=["chronyc"])

    def incomplete_command(*args, **kwargs):
        return CompletedProcess(args, 0, "Stratum : 0\n", "")

    with patch(
        "backend.market_data.chrony.subprocess.run",
        side_effect=incomplete_command,
    ):
        status = service.snapshot()

    assert status.available is False
    assert status.synchronized is False
    assert "missing required fields" in status.error


def test_chrony_rejects_non_positive_timeout():
    with pytest.raises(ValueError):
        ChronyStatusService(command=["chronyc"], timeout_seconds=0)
