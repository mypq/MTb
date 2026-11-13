
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
^* 162.159.200.123               3   7   377   227  +1095us[+1133us] +/- 46ms
^+ 194.58.203.196                1   7   377    32  -1710us[-1710us] +/- 45ms
^+ 94.198.159.15                 1   7   377   228    +72us[ +110us] +/- 45ms
"""

AUTHDATA_OUTPUT = """
Name/IP address             Mode KeyID Type KLen Last Atmp  NAK Cook CLen
=========================================================================
time.cloudflare.com          NTS     1   30  128  23m    0    0    8   64
gbg1-ts.nts.netnod.se        NTS     1   15  256  23m    0    0    8  100
ntppool1.time.nl             NTS     1   30  128  23m    0    0    8   64
"""


def successful_command(*args, **kwargs):
    cmd = args[0] if args else []
    cmd_str = " ".join(cmd) if isinstance(cmd, list) else str(cmd)

    if "tracking" in cmd_str:
        return CompletedProcess(args, 0, TRACKING_OUTPUT, "")

    if "sources" in cmd_str:
        return CompletedProcess(args, 0, SOURCES_OUTPUT, "")

    if "authdata" in cmd_str:
        return CompletedProcess(args, 0, AUTHDATA_OUTPUT, "")

    return CompletedProcess(args, 1, "", "Unexpected command")


def run_with_outputs(
    *,
    tracking=TRACKING_OUTPUT,
    sources=SOURCES_OUTPUT,
    authdata=AUTHDATA_OUTPUT,
):
    def command(*args, **kwargs):
        cmd = args[0] if args else []
        cmd_str = " ".join(cmd) if isinstance(cmd, list) else str(cmd)

        if "tracking" in cmd_str:
            return CompletedProcess(args, 0, tracking, "")

        if "sources" in cmd_str:
            return CompletedProcess(args, 0, sources, "")

        if "authdata" in cmd_str:
            return CompletedProcess(args, 0, authdata, "")

        return CompletedProcess(args, 1, "", "Unexpected command")

    return command


def test_chrony_reports_synchronized_with_three_nts_sources():
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
    assert status.selected_source == "162.159.200.123"
    assert status.healthy_sources == 3
    assert status.nts_authenticated_sources == 3
    assert status.quorum_met is True
    assert status.last_offset_seconds == -0.000281089
    assert status.root_dispersion_seconds == 0.002205403


def test_chrony_does_not_synchronize_without_selected_source():
    service = ChronyStatusService(command=["chronyc"])
    candidate_only = SOURCES_OUTPUT.replace("^*", "^ ")

    with patch(
        "backend.market_data.chrony.subprocess.run",
        side_effect=run_with_outputs(sources=candidate_only),
    ):
        status = service.snapshot()

    assert status.available is True
    assert status.synchronized is False
    assert status.selected_source is None
    assert status.quorum_met is True


def test_chrony_does_not_synchronize_with_fewer_than_three_sources():
    service = ChronyStatusService(command=["chronyc"])
    two_sources = "\n".join(SOURCES_OUTPUT.splitlines()[:4])

    with patch(
        "backend.market_data.chrony.subprocess.run",
        side_effect=run_with_outputs(sources=two_sources),
    ):
        status = service.snapshot()

    assert status.available is True
    assert status.synchronized is False
    assert status.healthy_sources == 2
    assert status.quorum_met is False


def test_chrony_rejects_nts_authentication_with_nonzero_nak():
    service = ChronyStatusService(command=["chronyc"])
    invalid_authdata = AUTHDATA_OUTPUT.replace(
        "23m    0    0    8",
        "23m    0    1    8",
        1,
    )

    with patch(
        "backend.market_data.chrony.subprocess.run",
        side_effect=run_with_outputs(authdata=invalid_authdata),
    ):
        status = service.snapshot()

    assert status.available is False
    assert status.synchronized is False
    assert "authentication health check failed" in status.error


def test_chrony_rejects_source_without_nts_mode():
    service = ChronyStatusService(command=["chronyc"])
    invalid_authdata = AUTHDATA_OUTPUT.replace("NTS", "NTP", 1)

    with patch(
        "backend.market_data.chrony.subprocess.run",
        side_effect=run_with_outputs(authdata=invalid_authdata),
    ):
        status = service.snapshot()

    assert status.available is False
    assert status.synchronized is False
    assert "NTS authentication is not active" in status.error


def test_chrony_rejects_missing_authenticated_source():
    service = ChronyStatusService(command=["chronyc"])
    incomplete_authdata = "\n".join(AUTHDATA_OUTPUT.splitlines()[:-1])

    with patch(
        "backend.market_data.chrony.subprocess.run",
        side_effect=run_with_outputs(authdata=incomplete_authdata),
    ):
        status = service.snapshot()

    assert status.available is False
    assert status.synchronized is False
    assert "Expected exactly three" in status.error


def test_chrony_rejects_source_without_cookies():
    service = ChronyStatusService(command=["chronyc"])
    invalid_authdata = AUTHDATA_OUTPUT.replace(
        "23m    0    0    8",
        "23m    0    0    0",
        1,
    )

    with patch(
        "backend.market_data.chrony.subprocess.run",
        side_effect=run_with_outputs(authdata=invalid_authdata),
    ):
        status = service.snapshot()

    assert status.available is False
    assert status.synchronized is False
    assert "authentication health check failed" in status.error


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
