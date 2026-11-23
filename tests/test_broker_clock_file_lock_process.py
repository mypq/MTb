from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.test_broker_clock_history import history_module


def test_os_lock_blocks_a_separate_process_and_releases_after_exit(
    tmp_path, monkeypatch
):
    lock_path = tmp_path / "history.csv.lock"
    ready_path = tmp_path / "child-ready.txt"
    release_path = tmp_path / "child-release.txt"
    script_path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "diagnostics"
        / "append_broker_clock_history.py"
    )

    child_code = """
import runpy
import sys
import time
from pathlib import Path

namespace = runpy.run_path(sys.argv[1])
lock = namespace["exclusive_file_lock"]
lock_path = sys.argv[2]
ready_path = Path(sys.argv[3])
release_path = Path(sys.argv[4])

with lock(lock_path):
    ready_path.write_text("ready", encoding="ascii")
    deadline = time.monotonic() + 15.0
    while not release_path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    if not release_path.exists():
        raise SystemExit("parent did not release child lock")
"""

    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            child_code,
            str(script_path),
            str(lock_path),
            str(ready_path),
            str(release_path),
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        deadline = time.monotonic() + 10.0
        while not ready_path.exists() and time.monotonic() < deadline:
            if child.poll() is not None:
                stdout, stderr = child.communicate()
                pytest.fail(
                    "child process exited before acquiring lock: "
                    f"stdout={stdout!r}, stderr={stderr!r}"
                )
            time.sleep(0.01)

        assert ready_path.exists(), "child process did not acquire the lock"

        monkeypatch.setattr(
            history_module, "LOCK_TIMEOUT_SECONDS", 0.2
        )
        monkeypatch.setattr(
            history_module, "LOCK_RETRY_SECONDS", 0.01
        )

        with pytest.raises(TimeoutError):
            with history_module.exclusive_file_lock(lock_path):
                pytest.fail("parent acquired a lock held by child process")

        release_path.write_text("release", encoding="ascii")
        stdout, stderr = child.communicate(timeout=10.0)
        assert child.returncode == 0, (
            f"child process failed: stdout={stdout!r}, stderr={stderr!r}"
        )

        # The child has exited its lock context. A new process owner can
        # acquire the same persistent lock file without stale metadata blocking.
        with history_module.exclusive_file_lock(lock_path):
            pass
    finally:
        if child.poll() is None:
            release_path.write_text("release", encoding="ascii")
            child.terminate()
            try:
                child.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5.0)
