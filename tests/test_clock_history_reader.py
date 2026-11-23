
from __future__ import annotations

import csv

import pytest

from backend.market_data.clock_history import CLOCK_HISTORY_COLUMNS
from backend.market_data.clock_history_reader import (
    ClockHistoryFileChangedError,
    ClockHistoryIncrementalReader,
    ClockHistoryReaderError,
)


def make_row(sequence: int = 1, **overrides):
    base = {
        "schema_version": "1",
        "run_id": "reader-test-run",
        "sample_sequence": str(sequence),
        "monotonic_ms": str(sequence * 60000),
        "time_gmt_epoch": str(1794952458 + sequence * 60),
        "time_trade_server_epoch": str(1794959658 + sequence * 60),
        "time_current_quote_epoch": str(1794959658 + sequence * 60),
        "time_local_epoch": str(1794963258 + sequence * 60),
        "server_utc_offset_seconds": "7200",
        "clock_values_valid": "true",
        "account_login": "337260380",
        "broker_server": "XMGlobal-MT5 9",
        "symbol": "GOLD",
        "terminal_build": "6316",
        "terminal_connected": "true",
    }
    base.update(overrides)
    return base


def header_bytes() -> bytes:
    return (";".join(CLOCK_HISTORY_COLUMNS) + "\r\n").encode("utf-8")


def row_bytes(row: dict[str, str]) -> bytes:
    import io

    output = io.StringIO(newline="")
    writer = csv.DictWriter(
        output,
        fieldnames=CLOCK_HISTORY_COLUMNS,
        delimiter=";",
        lineterminator="\r\n",
    )
    writer.writerow(row)
    return output.getvalue().encode("utf-8")


def write_history(path, rows):
    path.write_bytes(
        header_bytes()
        + b"".join(row_bytes(row) for row in rows)
    )


def make_reader(tmp_path, **kwargs):
    history = tmp_path / "clock_history.csv"
    checkpoint = tmp_path / "clock_history.checkpoint.json"
    reader = ClockHistoryIncrementalReader(
        history,
        checkpoint,
        **kwargs,
    )
    return history, checkpoint, reader


def test_initial_read_returns_latest_three_samples(tmp_path):
    history, _, reader = make_reader(tmp_path)

    write_history(
        history,
        [make_row(sequence) for sequence in range(1, 6)],
    )

    batch = reader.read_new()

    assert [sample.sample_sequence for sample in batch.samples] == [
        3, 4, 5
    ]
    assert batch.checkpoint.samples_processed == 5
    assert batch.checkpoint.last_by_run[
        "reader-test-run"
    ]["sample_sequence"] == 5


def test_initial_read_empty_file_header_can_be_checkpointed(tmp_path):
    history, _, reader = make_reader(tmp_path)
    history.write_bytes(header_bytes())

    batch = reader.read_new()

    assert batch.samples == ()
    assert batch.checkpoint.samples_processed == 0

    reader.commit(batch)


def test_subsequent_read_returns_only_appended_samples(tmp_path):
    history, _, reader = make_reader(tmp_path)

    write_history(history, [make_row(1), make_row(2)])
    first = reader.read_new()
    assert [sample.sample_sequence for sample in first.samples] == [1, 2]
    reader.commit(first)

    with history.open("ab") as handle:
        handle.write(row_bytes(make_row(3)))

    second = reader.read_new()

    assert [sample.sample_sequence for sample in second.samples] == [3]
    assert second.checkpoint.samples_processed == 3


def test_incomplete_trailing_line_is_not_consumed(tmp_path):
    history, _, reader = make_reader(tmp_path)

    first_row = row_bytes(make_row(1))
    second_row = row_bytes(make_row(2))

    history.write_bytes(
        header_bytes()
        + first_row
        + second_row[:len(second_row) // 2]
    )

    first = reader.read_new()

    assert [sample.sample_sequence for sample in first.samples] == [1]
    first_offset = first.checkpoint.offset
    reader.commit(first)

    second = reader.read_new()

    assert second.samples == ()
    assert second.checkpoint.offset == first_offset
    reader.commit(second)

    with history.open("ab") as handle:
        handle.write(second_row[len(second_row) // 2:])

    third = reader.read_new()

    assert [sample.sample_sequence for sample in third.samples] == [2]


def test_restart_resumes_from_durable_checkpoint_without_duplicates(
    tmp_path,
):
    history, checkpoint, reader = make_reader(tmp_path)

    write_history(history, [make_row(1), make_row(2)])
    first = reader.read_new()
    reader.commit(first)

    restarted = ClockHistoryIncrementalReader(history, checkpoint)

    empty = restarted.read_new()
    assert empty.samples == ()
    restarted.commit(empty)

    with history.open("ab") as handle:
        handle.write(row_bytes(make_row(3)))

    next_batch = restarted.read_new()

    assert [sample.sample_sequence for sample in next_batch.samples] == [3]


def test_modified_processed_prefix_is_rejected(tmp_path):
    history, checkpoint, reader = make_reader(tmp_path)

    write_history(history, [make_row(1), make_row(2)])
    batch = reader.read_new()
    reader.commit(batch)

    content = history.read_bytes()
    original = b"XMGlobal-MT5 9"
    replacement = b"XMGlobal-MT5 8"

    assert original in content
    history.write_bytes(content.replace(original, replacement, 1))

    restarted = ClockHistoryIncrementalReader(history, checkpoint)

    with pytest.raises(
        ClockHistoryFileChangedError,
        match="modified",
    ):
        restarted.read_new()


def test_truncated_history_file_is_rejected(tmp_path):
    history, checkpoint, reader = make_reader(tmp_path)

    write_history(history, [make_row(1), make_row(2)])
    batch = reader.read_new()
    reader.commit(batch)

    original = history.read_bytes()
    history.write_bytes(original[:batch.checkpoint.offset - 10])

    restarted = ClockHistoryIncrementalReader(history, checkpoint)

    with pytest.raises(ClockHistoryFileChangedError):
        restarted.read_new()


def test_replaced_history_file_is_rejected(tmp_path):
    history, checkpoint, reader = make_reader(tmp_path)

    write_history(history, [make_row(1)])
    batch = reader.read_new()
    reader.commit(batch)

    replacement = tmp_path / "replacement.csv"
    write_history(replacement, [make_row(1), make_row(2)])
    replacement.replace(history)

    restarted = ClockHistoryIncrementalReader(history, checkpoint)

    with pytest.raises(ClockHistoryFileChangedError):
        restarted.read_new()


def test_sequence_gap_in_appended_rows_is_rejected(tmp_path):
    history, _, reader = make_reader(tmp_path)

    write_history(history, [make_row(1)])
    first = reader.read_new()
    reader.commit(first)

    with history.open("ab") as handle:
        handle.write(row_bytes(make_row(3)))

    with pytest.raises(Exception, match="Sequence discontinuity"):
        reader.read_new()


def test_identity_change_inside_run_is_rejected(tmp_path):
    history, _, reader = make_reader(tmp_path)

    write_history(history, [make_row(1)])
    first = reader.read_new()
    reader.commit(first)

    with history.open("ab") as handle:
        handle.write(
            row_bytes(
                make_row(
                    2,
                    account_login="123456789",
                )
            )
        )

    with pytest.raises(Exception, match="Identity changed"):
        reader.read_new()


def test_batch_must_be_committed_before_next_read(tmp_path):
    history, _, reader = make_reader(tmp_path)

    write_history(history, [make_row(1)])
    batch = reader.read_new()

    with pytest.raises(ClockHistoryReaderError, match="committed"):
        reader.read_new()

    reader.commit(batch)


def test_uncommitted_batch_is_replayed_after_restart(tmp_path):
    history, checkpoint, reader = make_reader(tmp_path)

    write_history(history, [make_row(1), make_row(2)])
    first = reader.read_new()

    # Simulate a crash before commit: no checkpoint is persisted.
    restarted = ClockHistoryIncrementalReader(history, checkpoint)
    replayed = restarted.read_new()

    assert [
        sample.sample_sequence for sample in replayed.samples
    ] == [
        sample.sample_sequence for sample in first.samples
    ]


def test_foreign_batch_cannot_be_committed(tmp_path):
    history, _, reader = make_reader(tmp_path)

    write_history(history, [make_row(1)])
    batch = reader.read_new()

    other_history, _, other_reader = make_reader(
        tmp_path / "other"
    )
    other_history.parent.mkdir(parents=True, exist_ok=True)
    write_history(other_history, [make_row(1)])
    other_batch = other_reader.read_new()

    with pytest.raises(ClockHistoryReaderError, match="pending batch"):
        reader.commit(other_batch)

    reader.commit(batch)


def test_max_samples_per_read_limits_appended_batch(tmp_path):
    history, _, reader = make_reader(
        tmp_path,
        max_samples_per_read=1,
    )

    write_history(history, [make_row(1)])
    first = reader.read_new()
    reader.commit(first)

    with history.open("ab") as handle:
        handle.write(row_bytes(make_row(2)))
        handle.write(row_bytes(make_row(3)))

    second = reader.read_new()
    assert [sample.sample_sequence for sample in second.samples] == [2]
    reader.commit(second)

    third = reader.read_new()
    assert [sample.sample_sequence for sample in third.samples] == [3]


def test_verified_checkpoint_revalidates_prefix_when_file_changes(
    tmp_path,
    monkeypatch,
):
    import backend.market_data.clock_history_reader as reader_module

    history, _, reader = make_reader(tmp_path)
    rows = [make_row(1), make_row(2)]
    write_history(history, rows)

    first = reader.read_new()
    reader.commit(first)

    original_next_hash = reader_module._next_hash
    hashed_lines = []

    def count_hash(previous, line):
        hashed_lines.append(line)
        return original_next_hash(previous, line)

    monkeypatch.setattr(reader_module, "_next_hash", count_hash)

    appended_row = row_bytes(make_row(3))
    with history.open("ab") as handle:
        handle.write(appended_row)

    second = reader.read_new()

    assert [sample.sample_sequence for sample in second.samples] == [3]
    assert hashed_lines == [
        header_bytes(),
        row_bytes(rows[0]),
        row_bytes(rows[1]),
        appended_row,
    ]


def test_verified_checkpoint_rejects_modified_prefix_with_appended_rows(
    tmp_path,
):
    history, _, reader = make_reader(tmp_path)
    write_history(history, [make_row(1), make_row(2)])

    first = reader.read_new()
    reader.commit(first)

    content = history.read_bytes()
    original = b"XMGlobal-MT5 9"
    replacement = b"XMGlobal-MT5 8"
    assert original in content

    # Change an already-processed row and append a valid new row before
    # the next read, reproducing the modification-plus-append failure.
    history.write_bytes(
        content.replace(original, replacement, 1)
        + row_bytes(make_row(3))
    )

    with pytest.raises(
        ClockHistoryFileChangedError,
        match="Previously processed clock history was modified",
    ):
        reader.read_new()


def test_verified_checkpoint_rejects_same_size_prefix_rewrite(tmp_path):
    history, _, reader = make_reader(tmp_path)
    write_history(history, [make_row(1), make_row(2)])

    batch = reader.read_new()
    reader.commit(batch)

    content = history.read_bytes()
    original = b"XMGlobal-MT5 9"
    replacement = b"XMGlobal-MT5 8"
    assert original in content
    history.write_bytes(content.replace(original, replacement, 1))

    with pytest.raises(
        ClockHistoryFileChangedError,
        match="modified",
    ):
        reader.read_new()



@pytest.mark.parametrize(
    "value",
    [0, -1, True, 1.5, "2", None],
)
def test_max_samples_per_read_rejects_invalid_values(tmp_path, value):
    with pytest.raises(ValueError, match="positive"):
        make_reader(tmp_path, max_samples_per_read=value)


@pytest.mark.parametrize(
    "bad_line",
    [
        b"too;few;columns\\r\\n",
        b"\\xff\\xfe\\r\\n",
    ],
)
def test_malformed_or_non_utf8_appended_row_is_rejected(
    tmp_path,
    bad_line,
):
    history, _, reader = make_reader(tmp_path)
    history.write_bytes(header_bytes() + bad_line)

    with pytest.raises(Exception):
        reader.read_new()


def test_incomplete_header_is_rejected(tmp_path):
    history, _, reader = make_reader(tmp_path)
    history.write_bytes(b"schema_version;run_id")

    with pytest.raises(ClockHistoryReaderError, match="header"):
        reader.read_new()


def test_wrong_header_is_rejected(tmp_path):
    history, _, reader = make_reader(tmp_path)
    history.write_bytes(b"wrong;header\\r\\n")

    with pytest.raises(Exception, match="header"):
        reader.read_new()


def test_utf8_bom_header_is_accepted(tmp_path):
    history, _, reader = make_reader(tmp_path)
    history.write_bytes(
        b"\\xef\\xbb\\xbf" + header_bytes() + row_bytes(make_row(1))
    )

    batch = reader.read_new()

    assert [sample.sample_sequence for sample in batch.samples] == [1]


def test_incremental_reader_rejects_non_advancing_monotonic_counter(
    tmp_path,
):
    history, _, reader = make_reader(tmp_path)
    write_history(history, [make_row(1)])
    first = reader.read_new()
    reader.commit(first)

    with history.open("ab") as handle:
        handle.write(
            row_bytes(
                make_row(
                    2,
                    monotonic_ms="60000",
                )
            )
        )

    with pytest.raises(Exception, match="did not advance"):
        reader.read_new()


def test_incremental_reader_accepts_monotonic_counter_wraparound(
    tmp_path,
):
    history, _, reader = make_reader(tmp_path)
    write_history(
        history,
        [make_row(1, monotonic_ms="4294967290")],
    )
    first = reader.read_new()
    reader.commit(first)

    with history.open("ab") as handle:
        handle.write(
            row_bytes(
                make_row(
                    2,
                    monotonic_ms="20",
                )
            )
        )

    second = reader.read_new()

    assert [sample.sample_sequence for sample in second.samples] == [2]


def test_failed_checkpoint_save_keeps_batch_pending_for_retry(
    tmp_path,
    monkeypatch,
):
    history, checkpoint, reader = make_reader(tmp_path)
    write_history(history, [make_row(1)])
    batch = reader.read_new()
    original_save = reader.store.save

    def fail_save(_checkpoint):
        raise OSError("simulated checkpoint write failure")

    monkeypatch.setattr(reader.store, "save", fail_save)

    with pytest.raises(OSError, match="simulated"):
        reader.commit(batch)

    assert not checkpoint.exists()

    with pytest.raises(ClockHistoryReaderError, match="committed"):
        reader.read_new()

    monkeypatch.setattr(reader.store, "save", original_save)
    reader.commit(batch)

    assert checkpoint.exists()
    assert reader.read_new().samples == ()
