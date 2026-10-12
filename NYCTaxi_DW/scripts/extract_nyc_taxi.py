#!/usr/bin/env python3
"""Chay tron ven buoc Extract: tai Parquet va chuyen sang CSV."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


DATASETS = ("yellow", "green", "fhv", "fhvhv")
DEFAULT_DATA_ROOT = Path(r"D:\Kho_Du_lieu\NYCTaxi")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tai va chuyen du lieu NYC Taxi cua mot nam/thang."
    )
    parser.add_argument("--year", type=int, required=True, help="Nam can xu ly")
    parser.add_argument(
        "--month", type=int, required=True, help="Thang can xu ly (1-12)"
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=DATASETS,
        default=list(DATASETS),
        help="Cac bo du lieu can xu ly (mac dinh: ca 4)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DEFAULT_DATA_ROOT,
        help=f"Thu muc goc chua raw va csv (mac dinh: {DEFAULT_DATA_ROOT})",
    )
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Chuyen lai CSV du da ton tai; Parquet hop le van duoc bo qua",
    )
    args = parser.parse_args()
    if args.year < 2009:
        parser.error("NYC TLC trip data bat dau tu nam 2009")
    if not 1 <= args.month <= 12:
        parser.error("--month phai nam trong khoang 1-12")
    if args.retries < 1:
        parser.error("--retries phai lon hon hoac bang 1")
    if args.batch_size < 1:
        parser.error("--batch-size phai lon hon hoac bang 1")
    return args


def run_step(command: list[str], label: str) -> int:
    print(f"\n===== {label} =====", flush=True)
    completed = subprocess.run(command, check=False)
    if completed.returncode:
        print(f"[THAT BAI] {label} tra ve ma {completed.returncode}", file=sys.stderr)
    return completed.returncode


def main() -> int:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    data_root = args.data_root.expanduser().resolve()
    common_selection = [
        "--year",
        str(args.year),
        "--month",
        str(args.month),
        "--datasets",
        *args.datasets,
    ]

    download_command = [
        sys.executable,
        str(script_dir / "download_nyc_taxi_parquet.py"),
        *common_selection,
        "--output",
        str(data_root / "raw"),
        "--retries",
        str(args.retries),
    ]
    if run_step(download_command, "1. DOWNLOAD PARQUET"):
        return 1

    convert_command = [
        sys.executable,
        str(script_dir / "convert_nyc_taxi_parquet_to_csv.py"),
        *common_selection,
        "--input",
        str(data_root / "raw"),
        "--output",
        str(data_root / "csv"),
        "--batch-size",
        str(args.batch_size),
    ]
    if args.overwrite:
        convert_command.append("--overwrite")
    if run_step(convert_command, "2. CONVERT PARQUET TO CSV"):
        return 1

    print(
        f"\n[HOAN TAT] Extract {args.year:04d}-{args.month:02d}. "
        "File da hoan chinh duoc bo qua an toan."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
