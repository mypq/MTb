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


class _FakeProcess:
    def __init__(self, *, initial_returncode=None):
        self.returncode = initial_returncode
        self.wait_calls = []

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.wait_calls.append(timeout)
        self.returncode = 15
        return self.returncode


def _windows_patches(*, run_side_effect=None, run_return=None, popen=None):
    return (
        patch(
            "backend.market_data.wsl_keepalive.platform.system",
            return_value="Windows",
        ),
        patch(
            "backend.market_data.wsl_keepalive.shutil.which",
            return_value="wsl.exe",
        ),
        patch(
            "backend.market_data.wsl_keepalive.uuid.uuid4",
        ),
        patch(
            "backend.market_data.wsl_keepalive.subprocess.Popen",
            return_value=popen,
        ),
        patch(
            "backend.market_data.wsl_keepalive.subprocess.run",
            side_effect=run_side_effect,
            return_value=run_return,
        ),
    )


def test_wsl_keepalive_is_a_noop_outside_windows():
    manager = WslChronyKeepalive()

    with patch(
        "backend.market_data.wsl_keepalive.platform.system",
        return_value="Linux",
    ), patch(
        "backend.market_data.wsl_keepalive.subprocess.run",
    ) as run, patch(
        "backend.market_data.wsl_keepalive.subprocess.Popen",
    ) as popen:
        manager.start()
        manager.stop()

    run.assert_not_called()
    popen.assert_not_called()
    assert manager.is_running is False


def test_wsl_keepalive_starts_attached_process_and_waits_for_chrony():
    manager = WslChronyKeepalive()
    process = _FakeProcess()
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        if "pid_file=" in args[4]:
            return _completed("281\n")
        return _completed()

    platform_patch, which_patch, uuid_patch, popen_patch, run_patch = (
        _windows_patches(run_side_effect=run, popen=process)
    )
    with platform_patch, which_patch, uuid_patch as uuid4, popen_patch as popen, run_patch:
        uuid4.return_value.hex = "testmarker"
        manager.start()

    assert manager.is_running is True
    popen.assert_called_once()
    args = popen.call_args.args[0]
    kwargs = popen.call_args.kwargs
    assert args[:4] == ["wsl.exe", "--exec", "/bin/bash", "-c"]
    assert 'echo "$$" > "/tmp/$1.pid"' in args[4]
    assert "exec -a" in args[4]
    assert "giant_goldbot_wsl_keepalive_testmarker" in args
    assert kwargs["stdin"] is not None
    assert kwargs["stdout"] is not None
    assert kwargs["stderr"] is not None
    assert len(calls) == 1
    assert calls[0][2:4] == ["/bin/bash", "-c"]
    assert "systemctl is-active chrony" in calls[0][4]


def test_wsl_keepalive_stop_signals_only_its_marked_linux_pid_and_waits():
    manager = WslChronyKeepalive()
    process = _FakeProcess()
    manager._executable = "wsl.exe"
    manager._process = process
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
    script = command[4]
    assert command[:4] == ["wsl.exe", "--exec", "/bin/bash", "-c"]
    assert 'case "$actual"' in script
    assert "kill -TERM" in script
    assert "giant_goldbot_wsl_keepalive_testmarker" in command
    assert process.wait_calls == [8.0]
    assert manager.is_running is False


def test_wsl_keepalive_stop_does_not_run_when_managed_wsl_process_already_exited():
    manager = WslChronyKeepalive()
    process = _FakeProcess(initial_returncode=15)
    manager._executable = "wsl.exe"
    manager._process = process
    manager._pid = 281
    manager._marker = "giant_goldbot_wsl_keepalive_owned"

    with patch(
        "backend.market_data.wsl_keepalive.platform.system",
        return_value="Windows",
    ), patch(
        "backend.market_data.wsl_keepalive.subprocess.run",
    ) as run:
        manager.stop()

    run.assert_not_called()
    assert manager.is_running is False


def test_wsl_keepalive_reports_chrony_startup_timeout_but_keeps_process():
    manager = WslChronyKeepalive(
        startup_timeout_seconds=0.01,
        poll_interval_seconds=0.001,
    )
    process = _FakeProcess()

    def run(args, **kwargs):
        return _completed(
            returncode=4,
            stderr="CHRONY_STATE=inactive",
        )

    platform_patch, which_patch, uuid_patch, popen_patch, run_patch = (
        _windows_patches(run_side_effect=run, popen=process)
    )
    with platform_patch, which_patch, uuid_patch as uuid4, popen_patch, run_patch, patch(
        "backend.market_data.wsl_keepalive.time.sleep",
    ):
        uuid4.return_value.hex = "timeoutmarker"
        with pytest.raises(TimeoutError, match="active Chrony service"):
            manager.start()

    assert manager.is_running is True
    assert manager._pid is None
    assert process.poll() is None




def test_wsl_keepalive_retries_readiness_on_same_managed_process():
    import time

    manager = WslChronyKeepalive(startup_timeout_seconds=0.01)
    process = _FakeProcess()
    attempts = []

    def run(args, **kwargs):
        attempts.append(args)
        if len(attempts) == 1:
            time.sleep(0.02)
            return _completed(
                returncode=4,
                stderr="CHRONY_STATE=inactive",
            )
        return _completed("281\n")

    platform_patch, which_patch, uuid_patch, popen_patch, run_patch = (
        _windows_patches(run_side_effect=run, popen=process)
    )
    with platform_patch, which_patch, uuid_patch as uuid4, popen_patch as popen, run_patch:
        uuid4.return_value.hex = "retrymarker"
        with pytest.raises(TimeoutError, match="CHRONY_STATE=inactive"):
            manager.start()

        assert manager.is_running is True
        assert manager._pid is None

        manager.start()

    assert popen.call_count == 1
    assert len(attempts) == 2
    assert manager.is_running is True
    assert manager._pid == 281

def test_wsl_keepalive_detects_managed_wsl_process_exiting_during_start():
    manager = WslChronyKeepalive()
    process = _FakeProcess(initial_returncode=15)

    platform_patch, which_patch, uuid_patch, popen_patch, run_patch = (
        _windows_patches(popen=process)
    )
    with platform_patch, which_patch, uuid_patch as uuid4, popen_patch, run_patch as run:
        uuid4.return_value.hex = "exitmarker"
        with pytest.raises(RuntimeError, match="exited before keepalive readiness"):
            manager.start()

    run.assert_not_called()
    assert manager.is_running is False



@pytest.mark.parametrize(
    ("startup_timeout", "poll_interval"),
    [
        (0, 0.1),
        (-1, 0.1),
        (float("nan"), 0.1),
        (float("inf"), 0.1),
        (True, 0.1),
        (1.0, 0),
        (1.0, -0.1),
        (1.0, float("nan")),
        (1.0, float("inf")),
        (1.0, False),
        ("1", 0.1),
    ],
)
def test_wsl_keepalive_rejects_non_finite_or_invalid_timing_configuration(
    startup_timeout,
    poll_interval,
):
    with pytest.raises(ValueError, match="finite positive number"):
        WslChronyKeepalive(
            startup_timeout_seconds=startup_timeout,
            poll_interval_seconds=poll_interval,
        )


def test_wsl_keepalive_reports_missing_wsl_executable():
    manager = WslChronyKeepalive()

    with patch(
        "backend.market_data.wsl_keepalive.platform.system",
        return_value="Windows",
    ), patch(
        "backend.market_data.wsl_keepalive.shutil.which",
        return_value=None,
    ):
        with pytest.raises(RuntimeError, match="wsl.exe was not found"):
            manager.start()

    assert manager.is_running is False


def test_wsl_keepalive_wraps_process_launch_os_error():
    manager = WslChronyKeepalive()

    with patch(
        "backend.market_data.wsl_keepalive.platform.system",
        return_value="Windows",
    ), patch(
        "backend.market_data.wsl_keepalive.shutil.which",
        return_value="wsl.exe",
    ), patch(
        "backend.market_data.wsl_keepalive.uuid.uuid4",
    ) as uuid4, patch(
        "backend.market_data.wsl_keepalive.subprocess.Popen",
        side_effect=OSError("process creation denied"),
    ):
        uuid4.return_value.hex = "launchfailure"
        with pytest.raises(
            RuntimeError,
            match="Could not start managed WSL process",
        ):
            manager.start()

    assert manager.is_running is False
