from __future__ import annotations

import argparse
import csv
import errno
import math
import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


MAX_NTP_AGE_SECONDS = 6 * 60 * 60
MIN_NTP_SAMPLES = 3
MAX_NTP_OFFSET_SECONDS = 1.0
MAX_NTP_UNCERTAINTY_SECONDS = 1.0
NTP_AGE_PRECISION_TOLERANCE_SECONDS = 0.000001

SOURCE_FIELDS = {
    "cycle_end_local_utc",
    "account_login",
    "broker_server",
    "symbol",
    "ntp_verified",
    "ntp_checked_at_unix",
    "ntp_offset_seconds",
    "ntp_uncertainty_seconds",
    "ntp_sample_count",
    "ntp_age_seconds",
    "tick_time_epoch_raw",
    "tick_update_status",
    "tick_corrected_read_midpoint_utc",
    "tick_minus_corrected_utc_seconds",
    "m1_latest_closed_open_raw",
    "m1_update_status",
    "m1_corrected_read_midpoint_utc",
    "m1_latest_closed_open_minus_corrected_utc_seconds",
    "m5_latest_closed_open_raw",
    "m5_update_status",
    "m5_corrected_read_midpoint_utc",
    "m5_latest_closed_open_minus_corrected_utc_seconds",
    "m1_active_open_raw",
    "m1_active_read_status",
    "m1_active_corrected_read_midpoint_utc",
    "m1_active_open_minus_expected_utc_boundary_seconds",
    "m5_active_open_raw",
    "m5_active_read_status",
    "m5_active_corrected_read_midpoint_utc",
    "m5_active_open_minus_expected_utc_boundary_seconds",
    "read_error",
}

# Preserve the existing archive format.
HISTORY_FIELDS = [
    "observed_at_local_utc",
    "account_login",
    "broker_server",
    "symbol",
    "evidence_kind",
    "raw_timestamp_epoch",
    "comparison_utc",
    "observed_offset_seconds",
    "ntp_checked_at_unix",
    "ntp_offset_seconds",
    "ntp_uncertainty_seconds",
    "source_file",
]

LOCK_TIMEOUT_SECONDS = 30.0
LOCK_RETRY_SECONDS = 0.1

STREAM_DEFINITIONS = (
    (
        "tick",
        "tick_time_epoch_raw",
        "tick_update_status",
        "tick_corrected_read_midpoint_utc",
        "tick_minus_corrected_utc_seconds",
        "UPDATED",
    ),
    (
        "m1_closed_candle",
        "m1_latest_closed_open_raw",
        "m1_update_status",
        "m1_corrected_read_midpoint_utc",
        "m1_latest_closed_open_minus_corrected_utc_seconds",
        "UPDATED",
    ),
    (
        "m5_closed_candle",
        "m5_latest_closed_open_raw",
        "m5_update_status",
        "m5_corrected_read_midpoint_utc",
        "m5_latest_closed_open_minus_corrected_utc_seconds",
        "UPDATED",
    ),
    (
        "m1_active_candle",
        "m1_active_open_raw",
        "m1_active_read_status",
        "m1_active_corrected_read_midpoint_utc",
        "m1_active_open_minus_expected_utc_boundary_seconds",
        "OK",
    ),
    (
        "m5_active_candle",
        "m5_active_open_raw",
        "m5_active_read_status",
        "m5_active_corrected_read_midpoint_utc",
        "m5_active_open_minus_expected_utc_boundary_seconds",
        "OK",
    ),
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Archive validated MT5 broker clock observations."
    )
    parser.add_argument("--input", required=True)
    parser.add_argument(
        "--output",
        default="diagnostics/broker_clock_history.csv",
    )
    return parser.parse_args()


def number(value):
    if value is None or not str(value).strip():
        return None

    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None

    return result if math.isfinite(result) else None


def valid_iso_datetime(value):
    if value is None or not str(value).strip():
        return False

    try:
        parsed = datetime.fromisoformat(str(value).strip())
    except (TypeError, ValueError, OverflowError):
        return False

    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def iso_epoch(value):
    try:
        return datetime.fromisoformat(
            str(value).strip()
        ).timestamp()
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def valid_unix_timestamp(value):
    parsed = number(value)

    if parsed is None:
        return False

    try:
        datetime.fromtimestamp(parsed, tz=timezone.utc)
    except (ValueError, OverflowError, OSError):
        return False

    return True


def valid_nonnegative_number(value):
    parsed = number(value)
    return parsed is not None and parsed >= 0


def observation_key(row):
    return tuple(
        str(value).strip()
        for value in (
            row.get("account_login", ""),
            row.get("broker_server", ""),
            row.get("symbol", ""),
            row.get("observed_at_local_utc", ""),
            row.get("evidence_kind", ""),
            row.get("raw_timestamp_epoch", ""),
        )
    )


def _lock_owner_is_running(lock_path):
    """Return True/False when the recorded owner PID can be checked.

    Unknown or malformed lock metadata is treated conservatively as active.
    """
    try:
        metadata = lock_path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError):
        return None

    if not metadata.startswith("pid="):
        return None

    try:
        pid = int(metadata[4:])
    except ValueError:
        return None

    if pid <= 0:
        return None

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        if exc.errno == errno.EPERM:
            return True
        return None

    return True


@contextmanager
def exclusive_file_lock(lock_path):
    """Cross-process lock using atomic file creation and dead-owner recovery."""
    lock_path = Path(lock_path)
    deadline = time.monotonic() + LOCK_TIMEOUT_SECONDS
    descriptor = None

    while descriptor is None:
        try:
            descriptor = os.open(
                str(lock_path),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
        except FileExistsError:
            # Recover only when the operating system confirms the recorded
            # owner process no longer exists. Unknown metadata fails closed.
            if _lock_owner_is_running(lock_path) is False:
                try:
                    lock_path.unlink()
                except FileNotFoundError:
                    pass
                continue

            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for archive lock: {lock_path}"
                )
            time.sleep(LOCK_RETRY_SECONDS)

    try:
        os.write(
            descriptor,
            f"pid={os.getpid()}\\n".encode("ascii"),
        )
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        yield
    finally:
        if descriptor is not None:
            os.close(descriptor)

        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


def load_existing_keys(destination):
    if not destination.exists():
        return set()

    with destination.open(
        "r",
        newline="",
        encoding="utf-8-sig",
    ) as handle:
        reader = csv.DictReader(handle)

        if reader.fieldnames != HISTORY_FIELDS:
            raise ValueError("history CSV schema mismatch")

        return {observation_key(row) for row in reader}


def read_source_rows(source):
    with source.open(
        "r",
        newline="",
        encoding="utf-8-sig",
    ) as handle:
        reader = csv.DictReader(handle)

        missing = SOURCE_FIELDS.difference(reader.fieldnames or ())

        if missing:
            raise ValueError(
                "unsupported evidence CSV; missing: "
                + ", ".join(sorted(missing))
            )

        return list(reader)


def valid_identity(row):
    return all(
        (row.get(field) or "").strip()
        for field in ("account_login", "broker_server", "symbol")
    )


def valid_ntp_metadata(row):
    if (row.get("ntp_verified") or "").strip().lower() != "true":
        return False

    ntp_offset = number(row.get("ntp_offset_seconds"))
    ntp_uncertainty = number(
        row.get("ntp_uncertainty_seconds")
    )
    ntp_checked = number(row.get("ntp_checked_at_unix"))
    sample_count = number(row.get("ntp_sample_count"))
    reported_age = number(row.get("ntp_age_seconds"))

    if (
        ntp_offset is None
        or ntp_uncertainty is None
        or ntp_checked is None
        or sample_count is None
        or reported_age is None
    ):
        return False

    if not valid_unix_timestamp(ntp_checked):
        return False

    if sample_count < MIN_NTP_SAMPLES:
        return False

    if not sample_count.is_integer():
        return False

    if abs(ntp_offset) >= MAX_NTP_OFFSET_SECONDS:
        return False

    if not 0 <= ntp_uncertainty <= MAX_NTP_UNCERTAINTY_SECONDS:
        return False

    if (
        reported_age < -NTP_AGE_PRECISION_TOLERANCE_SECONDS
        or reported_age > MAX_NTP_AGE_SECONDS
    ):
        return False

    return True


def valid_ntp_at_observation(row, observed_at):
    checked = number(row.get("ntp_checked_at_unix"))
    observed_epoch = iso_epoch(observed_at)

    if checked is None or observed_epoch is None:
        return False

    actual_age = observed_epoch - checked

    if actual_age < -NTP_AGE_PRECISION_TOLERANCE_SECONDS:
        return False

    if actual_age > MAX_NTP_AGE_SECONDS:
        return False

    # Treat sub-microsecond negative values as timestamp precision noise.
    actual_age = max(0.0, actual_age)

    reported_age = number(row.get("ntp_age_seconds"))

    if reported_age is None:
        return False

    if reported_age < -NTP_AGE_PRECISION_TOLERANCE_SECONDS:
        return False

    reported_age = max(0.0, reported_age)

    # Detect inconsistent NTP age metadata.
    if abs(actual_age - reported_age) > 5.0:
        return False

    return True


def build_observations(source_rows, source_name):
    observations = []
    rejected_timestamps = 0
    rejected_numeric = 0
    rejected_identity = 0
    skipped_untrusted_ntp = 0
    skipped_status = {}
    skipped_stale_active = 0

    for row in source_rows:
        if not valid_identity(row):
            rejected_identity += 1
            continue

        observed_at = (
            row.get("cycle_end_local_utc") or ""
        ).strip()

        if not valid_iso_datetime(observed_at):
            rejected_timestamps += 1
            continue

        if not valid_ntp_metadata(row):
            skipped_untrusted_ntp += 1
            continue

        if not valid_ntp_at_observation(row, observed_at):
            skipped_untrusted_ntp += 1
            continue

        ntp_offset = number(row.get("ntp_offset_seconds"))
        ntp_uncertainty = number(
            row.get("ntp_uncertainty_seconds")
        )
        ntp_checked = number(row.get("ntp_checked_at_unix"))

        for (
            kind,
            raw_field,
            status_field,
            utc_field,
            difference_field,
            required_status,
        ) in STREAM_DEFINITIONS:

            status = (
                row.get(status_field) or ""
            ).strip().upper()

            # INITIAL is a valid first observation for streams whose
            # normal update status is UPDATED.
            allowed_statuses = (
                {"UPDATED", "INITIAL"}
                if required_status == "UPDATED"
                else {required_status}
            )

            if kind in {"m1_active_candle", "m5_active_candle"}:
                # A successful read is not proof that the market is moving.
                # Require a tick change observed in this same capture cycle.
                tick_status = (
                    row.get("tick_update_status") or ""
                ).strip().upper()
                if tick_status != "UPDATED":
                    skipped_stale_active += 1
                    continue

            if status not in allowed_statuses:
                skipped_status[status or "UNKNOWN"] = (
                    skipped_status.get(status or "UNKNOWN", 0) + 1
                )
                continue

            raw = number(row.get(raw_field))
            difference = number(row.get(difference_field))
            comparison = (
                row.get(utc_field) or ""
            ).strip()

            if raw is None or difference is None:
                rejected_numeric += 1
                continue

            # Tick timestamps may contain millisecond fractions.
            # Candle-open timestamps must remain whole seconds.
            if (
                not valid_unix_timestamp(raw)
                or (kind != "tick" and not raw.is_integer())
            ):
                rejected_numeric += 1
                continue

            if not valid_iso_datetime(comparison):
                rejected_timestamps += 1
                continue

            observations.append({
                "observed_at_local_utc": observed_at,
                "account_login": (
                    row.get("account_login") or ""
                ).strip(),
                "broker_server": (
                    row.get("broker_server") or ""
                ).strip(),
                "symbol": (
                    row.get("symbol") or ""
                ).strip(),
                "evidence_kind": kind,
                "raw_timestamp_epoch": str(
                    row.get(raw_field) or ""
                ).strip(),
                "comparison_utc": comparison,
                "observed_offset_seconds": str(difference),
                "ntp_checked_at_unix": str(ntp_checked),
                "ntp_offset_seconds": str(ntp_offset),
                "ntp_uncertainty_seconds": str(ntp_uncertainty),
                "source_file": source_name,
            })

    stats = {
        "rejected_timestamps": rejected_timestamps,
        "rejected_numeric": rejected_numeric,
        "rejected_identity": rejected_identity,
        "skipped_untrusted_ntp": skipped_untrusted_ntp,
        "skipped_status": skipped_status,
        "skipped_stale_active": skipped_stale_active,
    }

    return observations, stats


def append_observations(destination, observations):
    destination.parent.mkdir(parents=True, exist_ok=True)

    lock_path = destination.with_name(destination.name + ".lock")

    with exclusive_file_lock(lock_path):
        existing_keys = load_existing_keys(destination)
        new_rows = []

        for row in observations:
            key = observation_key(row)

            if key in existing_keys:
                continue

            new_rows.append(row)
            existing_keys.add(key)

        write_header = (
            not destination.exists()
            or destination.stat().st_size == 0
        )

        if new_rows or write_header:
            with destination.open(
                "a",
                newline="",
                encoding="utf-8",
            ) as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=HISTORY_FIELDS,
                )

                if write_header:
                    writer.writeheader()

                writer.writerows(new_rows)
                handle.flush()
                os.fsync(handle.fileno())

    return new_rows


def main():
    args = parse_args()
    source = Path(args.input)
    destination = Path(args.output)

    if not source.is_file():
        print(f"ERROR: input file not found: {source}")
        return 2

    try:
        source_rows = read_source_rows(source)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 3
    except (OSError, csv.Error) as exc:
        print(f"ERROR: cannot read input: {exc}")
        return 4

    observations, stats = build_observations(
        source_rows,
        source.name,
    )

    if not observations:
        print("ERROR: no eligible trusted observations found.")
        print(f"Rejected timestamps: {stats['rejected_timestamps']}")
        print(f"Rejected numeric values: {stats['rejected_numeric']}")
        print(f"Rejected identity rows: {stats['rejected_identity']}")
        print(f"Skipped untrusted NTP rows: {stats['skipped_untrusted_ntp']}")
        print(f"Skipped statuses: {stats['skipped_status']}")
        print(f"Skipped stale active-candle observations: {stats['skipped_stale_active']}")
        return 5

    try:
        new_rows = append_observations(
            destination,
            observations,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 6
    except TimeoutError as exc:
        print(f"ERROR: {exc}")
        return 7
    except (OSError, csv.Error) as exc:
        print(f"ERROR: cannot append history: {exc}")
        return 8

    duplicates = len(observations) - len(new_rows)

    print(f"Source rows: {len(source_rows)}")
    print(f"Eligible observations: {len(observations)}")
    print(f"New observations: {len(new_rows)}")
    print(f"Duplicates skipped: {duplicates}")
    print(f"Rejected timestamps: {stats['rejected_timestamps']}")
    print(f"Rejected numeric values: {stats['rejected_numeric']}")
    print(f"Rejected identity rows: {stats['rejected_identity']}")
    print(f"Skipped untrusted NTP rows: {stats['skipped_untrusted_ntp']}")
    print(f"Skipped statuses: {stats['skipped_status']}")
    print(f"Skipped stale active-candle observations: {stats['skipped_stale_active']}")
    print(f"History: {destination.resolve()}")

    print(
        "IMPORTANT: this archive is NOT a broker UTC offset. "
        "observed_offset_seconds is a measured comparison "
        "difference. Closed-candle values include candle age. "
        "Active-candle values compare the active candle open "
        "with its expected UTC boundary. None of these "
        "measurements authorizes production timestamp correction."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
