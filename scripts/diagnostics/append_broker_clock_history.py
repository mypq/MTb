
from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path


SOURCE_FIELDS = {
    "cycle_end_local_utc",
    "account_login",
    "broker_server",
    "symbol",
    "ntp_verified",
    "ntp_checked_at_unix",
    "ntp_offset_seconds",
    "ntp_uncertainty_seconds",
    "tick_time_epoch_raw",
    "tick_corrected_read_midpoint_utc",
    "tick_minus_corrected_utc_seconds",
    "m1_latest_closed_open_raw",
    "m1_corrected_read_midpoint_utc",
    "m1_latest_closed_open_minus_corrected_utc_seconds",
    "m5_latest_closed_open_raw",
    "m5_corrected_read_midpoint_utc",
    "m5_latest_closed_open_minus_corrected_utc_seconds",
    "read_error",
}

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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Archive MT5 broker clock evidence."
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


def observation_key(row):
    return tuple(
        row.get(field, "")
        for field in (
            "account_login",
            "broker_server",
            "symbol",
            "observed_at_local_utc",
            "evidence_kind",
            "raw_timestamp_epoch",
        )
    )


def main():
    args = parse_args()
    source = Path(args.input)
    destination = Path(args.output)

    if not source.is_file():
        print(f"ERROR: input file not found: {source}")
        return 2

    try:
        with source.open(
            "r", newline="", encoding="utf-8-sig"
        ) as handle:
            reader = csv.DictReader(handle)

            missing = SOURCE_FIELDS.difference(
                reader.fieldnames or ()
            )
            if missing:
                print(
                    "ERROR: unsupported evidence CSV; missing: "
                    + ", ".join(sorted(missing))
                )
                return 3

            source_rows = list(reader)

    except (OSError, csv.Error) as exc:
        print(f"ERROR: cannot read input: {exc}")
        return 4

    observations = []

    definitions = (
        (
            "tick",
            "tick_time_epoch_raw",
            "tick_corrected_read_midpoint_utc",
            "tick_minus_corrected_utc_seconds",
        ),
        (
            "m1_closed_candle",
            "m1_latest_closed_open_raw",
            "m1_corrected_read_midpoint_utc",
            "m1_latest_closed_open_minus_corrected_utc_seconds",
        ),
        (
            "m5_closed_candle",
            "m5_latest_closed_open_raw",
            "m5_corrected_read_midpoint_utc",
            "m5_latest_closed_open_minus_corrected_utc_seconds",
        ),
    )

    for row in source_rows:
        if (row.get("ntp_verified") or "").lower() != "true":
            continue

        if (row.get("read_error") or "").strip():
            continue

        ntp_offset = number(row.get("ntp_offset_seconds"))
        ntp_uncertainty = number(
            row.get("ntp_uncertainty_seconds")
        )
        ntp_checked = row.get("ntp_checked_at_unix", "").strip()

        if (
            ntp_offset is None
            or ntp_uncertainty is None
            or not ntp_checked
        ):
            continue

        for kind, raw_field, utc_field, offset_field in definitions:
            raw = number(row.get(raw_field))
            offset = number(row.get(offset_field))
            comparison = row.get(utc_field, "").strip()

            if raw is None or offset is None or not comparison:
                continue

            observations.append({
                "observed_at_local_utc": row.get(
                    "cycle_end_local_utc", ""
                ),
                "account_login": row.get("account_login", ""),
                "broker_server": row.get("broker_server", ""),
                "symbol": row.get("symbol", ""),
                "evidence_kind": kind,
                "raw_timestamp_epoch": str(raw),
                "comparison_utc": comparison,
                "observed_offset_seconds": str(offset),
                "ntp_checked_at_unix": ntp_checked,
                "ntp_offset_seconds": str(ntp_offset),
                "ntp_uncertainty_seconds": str(ntp_uncertainty),
                "source_file": source.name,
            })

    if not observations:
        print("ERROR: no complete NTP-trusted observations found.")
        return 5

    destination.parent.mkdir(parents=True, exist_ok=True)
    existing_keys = set()

    if destination.exists():
        try:
            with destination.open(
                "r", newline="", encoding="utf-8-sig"
            ) as handle:
                reader = csv.DictReader(handle)

                if reader.fieldnames != HISTORY_FIELDS:
                    print("ERROR: history CSV schema mismatch.")
                    return 6

                existing_keys = {
                    observation_key(row) for row in reader
                }

        except (OSError, csv.Error) as exc:
            print(f"ERROR: cannot read history: {exc}")
            return 7

    new_rows = [
        row for row in observations
        if observation_key(row) not in existing_keys
    ]

    try:
        write_header = (
            not destination.exists()
            or destination.stat().st_size == 0
        )

        with destination.open(
            "a", newline="", encoding="utf-8"
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=HISTORY_FIELDS,
            )

            if write_header:
                writer.writeheader()

            writer.writerows(new_rows)
            handle.flush()

    except (OSError, csv.Error) as exc:
        print(f"ERROR: cannot append history: {exc}")
        return 8

    print(f"Source rows: {len(source_rows)}")
    print(f"Observations found: {len(observations)}")
    print(f"New observations: {len(new_rows)}")
    print(f"Duplicates skipped: {len(observations) - len(new_rows)}")
    print(f"History: {destination.resolve()}")
    print(
        "These are observed differences, not verified offsets. "
        "Do not use them to correct production candles."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
