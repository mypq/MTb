
from __future__ import annotations

import json

import pytest

import backend.market_data.clock_history_checkpoint as checkpoint_module
from backend.market_data.clock_history_checkpoint import (
    CHECKPOINT_VERSION,
    ClockHistoryCheckpoint,
    ClockHistoryCheckpointError,
    ClockHistoryCheckpointStore,
)


def make_checkpoint(
    *,
    offset: int = 256,
    file_size: int = 512,
    samples_processed: int = 3,
) -> ClockHistoryCheckpoint:
    return ClockHistoryCheckpoint(
        file_device=10,
        file_inode=20,
        offset=offset,
        file_size=file_size,
        file_mtime_ns=123456789,
        samples_processed=samples_processed,
        last_by_run={
            "run-a": {
                "sample_sequence": 3,
                "monotonic_ms": 181000,
                "identity": [
                    337260380,
                    "XMGlobal-MT5 9",
                    "GOLD",
                    6316,
                ],
            }
        },
    )


def test_missing_checkpoint_returns_none(tmp_path):
    store = ClockHistoryCheckpointStore(
        tmp_path / "state" / "checkpoint.json"
    )

    assert store.load() is None


def test_checkpoint_round_trip_preserves_reader_state(tmp_path):
    store = ClockHistoryCheckpointStore(
        tmp_path / "state" / "checkpoint.json"
    )
    expected = make_checkpoint()

    store.save(expected)

    assert store.load() == expected


def test_checkpoint_file_contains_versioned_json(tmp_path):
    path = tmp_path / "checkpoint.json"
    store = ClockHistoryCheckpointStore(path)

    store.save(make_checkpoint())

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["version"] == CHECKPOINT_VERSION
    assert payload["checkpoint"]["offset"] == 256
    assert payload["checkpoint"]["samples_processed"] == 3


def test_corrupt_checkpoint_is_rejected(tmp_path):
    path = tmp_path / "checkpoint.json"
    path.write_text("{not-json", encoding="utf-8")

    with pytest.raises(ClockHistoryCheckpointError):
        ClockHistoryCheckpointStore(path).load()


def test_unsupported_checkpoint_version_is_rejected(tmp_path):
    path = tmp_path / "checkpoint.json"
    path.write_text(
        json.dumps({
            "version": CHECKPOINT_VERSION + 1,
            "checkpoint": {},
        }),
        encoding="utf-8",
    )

    with pytest.raises(ClockHistoryCheckpointError, match="version"):
        ClockHistoryCheckpointStore(path).load()


@pytest.mark.parametrize(
    ("offset", "file_size"),
    [
        (-1, 10),
        (11, 10),
    ],
)
def test_invalid_checkpoint_offsets_are_rejected(
    offset,
    file_size,
):
    with pytest.raises(ClockHistoryCheckpointError):
        make_checkpoint(offset=offset, file_size=file_size)


def test_failed_atomic_replace_preserves_previous_checkpoint(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "checkpoint.json"
    store = ClockHistoryCheckpointStore(path)
    original = make_checkpoint()

    store.save(original)
    original_bytes = path.read_bytes()

    def fail_replace(source, destination):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(
        checkpoint_module.os,
        "replace",
        fail_replace,
    )

    with pytest.raises(
        ClockHistoryCheckpointError,
        match="persist",
    ):
        store.save(
            make_checkpoint(offset=300, file_size=600)
        )

    assert path.read_bytes() == original_bytes
    assert store.load() == original
    assert not list(
        tmp_path.glob(".checkpoint.json.*.tmp")
    )



@pytest.mark.parametrize(
    "field",
    [
        "file_device",
        "file_inode",
        "offset",
        "file_size",
        "file_mtime_ns",
        "samples_processed",
    ],
)
@pytest.mark.parametrize("invalid", [True, 1.5, "1"])
def test_checkpoint_numeric_fields_must_be_integers(field, invalid):
    values = {
        "file_device": 10,
        "file_inode": 20,
        "offset": 256,
        "file_size": 512,
        "file_mtime_ns": 123456789,
        "samples_processed": 3,
        "last_by_run": {},
    }
    values[field] = invalid

    with pytest.raises(
        ClockHistoryCheckpointError,
        match="must be integers",
    ):
        ClockHistoryCheckpoint(**values)


@pytest.mark.parametrize(
    "digest",
    [
        "not-a-hash",
        "a" * 63,
        "g" * 64,
        "a" * 65,
    ],
)
def test_invalid_checkpoint_prefix_digest_is_rejected(digest):
    with pytest.raises(
        ClockHistoryCheckpointError,
        match="64 hexadecimal",
    ):
        ClockHistoryCheckpoint(
            file_device=10,
            file_inode=20,
            offset=256,
            file_size=512,
            file_mtime_ns=123456789,
            samples_processed=3,
            last_by_run={},
            prefix_sha256=digest,
        )


def test_empty_checkpoint_prefix_digest_remains_supported():
    checkpoint = make_checkpoint()
    assert checkpoint.prefix_sha256 == ""


def test_boolean_checkpoint_version_is_rejected(tmp_path):
    path = tmp_path / "checkpoint.json"
    path.write_text(
        json.dumps({
            "version": True,
            "checkpoint": {},
        }),
        encoding="utf-8",
    )

    with pytest.raises(ClockHistoryCheckpointError, match="version"):
        ClockHistoryCheckpointStore(path).load()


@pytest.mark.parametrize(
    "last_by_run",
    [
        {1: {}},
        {"run-a": []},
    ],
)
def test_invalid_last_by_run_entries_are_rejected(last_by_run):
    with pytest.raises(
        ClockHistoryCheckpointError,
        match="keyed by run ID",
    ):
        ClockHistoryCheckpoint(
            file_device=10,
            file_inode=20,
            offset=256,
            file_size=512,
            file_mtime_ns=123456789,
            samples_processed=3,
            last_by_run=last_by_run,
        )
