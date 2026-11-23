from subprocess import CompletedProcess
from unittest.mock import patch

import pytest

from backend.market_data.wsl_keepalive import WslChronyKeepalive


def _completed(stdout="", returncode=0, stderr=""):
    return CompletedProcess(
        args=["wsl.exe"],
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


def test_wsl_keepalive_is_a_noop_outside_windows():
    manager = WslChronyKeepalive()

    with patch(
        "backend.market_data.wsl_keepalive.platform.system",
        return_value="Linux",
    ), patch(
        "backend.market_data.wsl_keepalive.subprocess.run",
    ) as run:
        manager.start()
        manager.stop()

    run.assert_not_called()
    assert manager.is_running is False


def test_wsl_keepalive_starts_unique_linux_process_and_waits_for_chrony():
    manager = WslChronyKeepalive()
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        if "systemctl" in args:
            return _completed("active\n")
        if "echo $!" in args[4]:
            return _completed("281\n")
        return _completed()

    with patch(
        "backend.market_data.wsl_keepalive.platform.system",
        return_value="Windows",
    ), patch(
        "backend.market_data.wsl_keepalive.shutil.which",
        return_value="wsl.exe",
    ), patch(
        "backend.market_data.wsl_keepalive.uuid.uuid4",
    ) as uuid4, patch(
        "backend.market_data.wsl_keepalive.subprocess.run",
        side_effect=run,
    ):
        uuid4.return_value.hex = "testmarker"
        manager.start()

    assert manager.is_running is True
    assert calls[0][:4] == [
        "wsl.exe",
        "--exec",
        "/bin/bash",
        "-c",
    ]
    assert "exec -a" in calls[0][4]
    assert "giant_goldbot_wsl_keepalive_testmarker" in calls[0]
    assert calls[1][2:5] == [
        "systemctl",
        "is-active",
        "chrony",
    ]


def test_wsl_keepalive_stop_signals_only_its_marked_linux_pid():
    manager = WslChronyKeepalive()
    manager._executable = "wsl.exe"
    manager._pid = 281
    manager._marker = "giant_goldbot_wsl_keepalive_testmarker"

    with patch(
        "backend.market_data.wsl_keepalive.platform.system",
        return_value="Windows",
    ), patch(
        "backend.market_data.wsl_keepalive.subprocess.run",
        return_value=_completed(),
    ) as run:
        manager.stop()

    command = run.call_args.args[0]
    assert command[:4] == ["wsl.exe", "--exec", "/bin/bash", "-c"]
    assert "kill -TERM" in command[4]
    assert "case \"$actual\"" in command[4]
    assert "giant_goldbot_wsl_keepalive_testmarker" in command
    assert manager.is_running is False


def test_wsl_keepalive_stop_does_not_signal_pid_with_different_marker():
    manager = WslChronyKeepalive()
    manager._executable = "wsl.exe"
    manager._pid = 281
    manager._marker = "giant_goldbot_wsl_keepalive_owned"

    with patch(
        "backend.market_data.wsl_keepalive.platform.system",
        return_value="Windows",
    ), patch(
        "backend.market_data.wsl_keepalive.subprocess.run",
        return_value=_completed(),
    ) as run:
        manager.stop()

    script = run.call_args.args[0][4]
    assert 'case "$actual"' in script
    assert "kill -TERM" in script
    assert "giant_goldbot_wsl_keepalive_owned" in run.call_args.args[0]


def test_wsl_keepalive_reports_chrony_startup_timeout_but_keeps_session():
    manager = WslChronyKeepalive(
        startup_timeout_seconds=0.01,
        poll_interval_seconds=0.001,
    )

    def run(args, **kwargs):
        if "echo $!" in args[4]:
            return _completed("281\n")
        if "systemctl" in args:
            return _completed("inactive\n", returncode=3)
        return _completed()

    with patch(
        "backend.market_data.wsl_keepalive.platform.system",
        return_value="Windows",
    ), patch(
        "backend.market_data.wsl_keepalive.shutil.which",
        return_value="wsl.exe",
    ), patch(
        "backend.market_data.wsl_keepalive.uuid.uuid4",
    ) as uuid4, patch(
        "backend.market_data.wsl_keepalive.subprocess.run",
        side_effect=run,
    ), patch(
        "backend.market_data.wsl_keepalive.time.sleep",
    ):
        uuid4.return_value.hex = "timeoutmarker"
        with pytest.raises(TimeoutError, match="Chrony did not become active"):
            manager.start()

    assert manager.is_running is True
