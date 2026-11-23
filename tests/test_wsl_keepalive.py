from subprocess import CompletedProcess, TimeoutExpired
from unittest.mock import MagicMock, patch

import pytest

from backend.market_data.wsl_keepalive import WslChronyKeepalive


def test_wsl_keepalive_is_a_noop_outside_windows():
    manager = WslChronyKeepalive()

    with patch(
        "backend.market_data.wsl_keepalive.platform.system",
        return_value="Linux",
    ), patch(
        "backend.market_data.wsl_keepalive.subprocess.Popen",
    ) as popen:
        manager.start()
        manager.stop()

    popen.assert_not_called()


def test_wsl_keepalive_starts_session_and_waits_for_chrony():
    process = MagicMock()
    process.poll.return_value = None
    manager = WslChronyKeepalive()

    with patch(
        "backend.market_data.wsl_keepalive.platform.system",
        return_value="Windows",
    ), patch(
        "backend.market_data.wsl_keepalive.shutil.which",
        return_value="wsl.exe",
    ), patch(
        "backend.market_data.wsl_keepalive.subprocess.Popen",
        return_value=process,
    ) as popen, patch(
        "backend.market_data.wsl_keepalive.subprocess.run",
        return_value=CompletedProcess(
            args=["wsl.exe"],
            returncode=0,
            stdout="active\n",
            stderr="",
        ),
    ) as run:
        manager.start()

    assert manager.is_running
    popen.assert_called_once()
    assert popen.call_args.args[0] == [
        "wsl.exe",
        "--exec",
        "sleep",
        "infinity",
    ]
    assert run.call_args.args[0] == [
        "wsl.exe",
        "--exec",
        "systemctl",
        "is-active",
        "chrony",
    ]


def test_wsl_keepalive_stop_terminates_only_its_process():
    process = MagicMock()
    process.poll.return_value = None
    manager = WslChronyKeepalive()
    manager._process = process

    manager.stop()

    process.terminate.assert_called_once()
    process.wait.assert_called_once_with(timeout=5.0)
    process.kill.assert_not_called()
    assert manager.is_running is False


def test_wsl_keepalive_kills_process_if_graceful_stop_times_out():
    process = MagicMock()
    process.poll.return_value = None
    process.wait.side_effect = [TimeoutExpired("wsl.exe", 5.0), None]
    manager = WslChronyKeepalive()
    manager._process = process

    manager.stop()

    process.terminate.assert_called_once()
    process.kill.assert_called_once()
    assert process.wait.call_count == 2


def test_wsl_keepalive_reports_chrony_startup_timeout_but_keeps_session():
    process = MagicMock()
    process.poll.return_value = None
    manager = WslChronyKeepalive(
        startup_timeout_seconds=0.01,
        poll_interval_seconds=0.001,
    )

    with patch(
        "backend.market_data.wsl_keepalive.platform.system",
        return_value="Windows",
    ), patch(
        "backend.market_data.wsl_keepalive.shutil.which",
        return_value="wsl.exe",
    ), patch(
        "backend.market_data.wsl_keepalive.subprocess.Popen",
        return_value=process,
    ), patch(
        "backend.market_data.wsl_keepalive.subprocess.run",
        return_value=CompletedProcess(
            args=["wsl.exe"],
            returncode=1,
            stdout="inactive\n",
            stderr="",
        ),
    ), patch(
        "backend.market_data.wsl_keepalive.time.sleep",
    ):
        with pytest.raises(TimeoutError, match="Chrony did not become active"):
            manager.start()

    assert manager.is_running
    process.terminate.assert_not_called()
