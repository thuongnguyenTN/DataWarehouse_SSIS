#!/usr/bin/env python3
r"""Download all four NYC TLC trip datasets by year and month.

Example on Windows:
    python download_nyc_taxi_parquet.py --year 2026 --month 1

Or download a range:
    python download_nyc_taxi_parquet.py \
        --start 2024-01 \
        --end 2026-08 \
        --output "D:\\Kho_Du_lieu\\NYCTaxi\\raw"

Output structure:
    D:\Kho_Du_lieu\NYCTaxi\raw\yellow\year=2024\month=01\
        yellow_tripdata_2024-01.parquet
    D:\Kho_Du_lieu\NYCTaxi\raw\green\year=2024\month=01\
        green_tripdata_2024-01.parquet
    D:\Kho_Du_lieu\NYCTaxi\raw\fhv\year=2024\month=01\
        fhv_tripdata_2024-01.parquet
    D:\Kho_Du_lieu\NYCTaxi\raw\fhvhv\year=2024\month=01\
        fhvhv_tripdata_2024-01.parquet
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DEFAULT_OUTPUT = r"D:\Kho_Du_lieu\NYCTaxi\raw"
CHUNK_SIZE = 1024 * 1024
PARQUET_MAGIC = b"PAR1"
DATASETS = {
    "yellow": "yellow_tripdata",
    "green": "green_tripdata",
    "fhv": "fhv_tripdata",
    "fhvhv": "fhvhv_tripdata",
}


@dataclass(frozen=True, order=True)
class YearMonth:
    year: int
    month: int

    @classmethod
    def parse(cls, value: str) -> "YearMonth":
        try:
            year_text, month_text = value.split("-", maxsplit=1)
            year = int(year_text)
            month = int(month_text)
        except (ValueError, AttributeError) as exc:
            raise argparse.ArgumentTypeError(
                f"Gia tri '{value}' khong hop le. Dung dinh dang YYYY-MM."
            ) from exc

        if year < 2009:
            raise argparse.ArgumentTypeError("NYC TLC trip data bat dau tu nam 2009.")
        if not 1 <= month <= 12:
            raise argparse.ArgumentTypeError("Thang phai nam trong khoang 01-12.")
        return cls(year, month)

    def next_month(self) -> "YearMonth":
        if self.month == 12:
            return YearMonth(self.year + 1, 1)
        return YearMonth(self.year, self.month + 1)

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def month_range(start: YearMonth, end: YearMonth):
    if start > end:
        raise ValueError("Thang bat dau phai nho hon hoac bang thang ket thuc.")

    current = start
    while current <= end:
        yield current
        current = current.next_month()


def is_valid_parquet(path: Path) -> bool:
    """Perform a lightweight Parquet signature check."""
    try:
        if path.stat().st_size < 8:
            return False
        with path.open("rb") as file:
            header = file.read(4)
            file.seek(-4, os.SEEK_END)
            footer = file.read(4)
        return header == PARQUET_MAGIC and footer == PARQUET_MAGIC
    except OSError:
        return False


def format_size(size_bytes: int) -> str:
    size = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size_bytes} B"


def download_file(url: str, destination: Path, retries: int) -> str:
    """Download atomically and return downloaded/skipped/not_found/failed."""
    if is_valid_parquet(destination):
        print(f"[BO QUA] Da co file hop le: {destination}")
        return "skipped"

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".part")

    for attempt in range(1, retries + 1):
        try:
            if temporary.exists():
                temporary.unlink()

            request = Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 NYC-Taxi-DWH-Downloader/1.0"
                },
            )

            print(f"[TAI] {url}")
            with urlopen(request, timeout=90) as response, temporary.open("wb") as out:
                total = int(response.headers.get("Content-Length", 0))
                downloaded = 0

                while True:
                    chunk = response.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    out.write(chunk)
                    downloaded += len(chunk)

                    if total:
                        percent = downloaded * 100 / total
                        print(
                            f"\r      {format_size(downloaded)} / "
                            f"{format_size(total)} ({percent:5.1f}%)",
                            end="",
                            flush=True,
                        )

            if total:
                print()

            if not is_valid_parquet(temporary):
                raise ValueError("File tai ve khong phai Parquet hop le.")

            temporary.replace(destination)
            print(f"[XONG] {destination} ({format_size(destination.stat().st_size)})")
            return "downloaded"

        except HTTPError as exc:
            if exc.code == 404:
                print(f"[CHUA CO] NYC TLC chua cong bo du lieu thang nay: {url}")
                temporary.unlink(missing_ok=True)
                return "not_found"
            error_message = f"HTTP {exc.code}: {exc.reason}"
        except (URLError, TimeoutError, OSError, ValueError) as exc:
            error_message = str(exc)

        temporary.unlink(missing_ok=True)
        print(f"[LOI {attempt}/{retries}] {error_message}")
        if attempt < retries:
            wait_seconds = attempt * 3
            print(f"      Thu lai sau {wait_seconds} giay...")
            time.sleep(wait_seconds)

    print(f"[THAT BAI] Khong tai duoc: {url}")
    return "failed"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Tai Yellow, Green, FHV va High Volume FHV Parquet; "
            "chia thu muc theo loai/nam/thang."
        )
    )
    parser.add_argument(
        "--start",
        type=YearMonth.parse,
        help="Thang bat dau, dinh dang YYYY-MM.",
    )
    parser.add_argument(
        "--end",
        type=YearMonth.parse,
        help="Thang ket thuc, dinh dang YYYY-MM.",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Nam can tai. Phai dung cung --month.",
    )
    parser.add_argument(
        "--month",
        type=int,
        help="Thang can tai (1-12). Phai dung cung --year.",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=tuple(DATASETS),
        default=list(DATASETS),
        metavar="DATASET",
        help=(
            "Bo du lieu can tai: yellow green fhv fhvhv "
            "(mac dinh: tai tat ca)."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(DEFAULT_OUTPUT),
        help=f"Thu muc goc luu du lieu (mac dinh: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="So lan thu lai khi tai loi (mac dinh: 3).",
    )
    return parser


def resolve_range(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> tuple[YearMonth, YearMonth]:
    exact_month_selected = args.year is not None or args.month is not None
    range_selected = args.start is not None or args.end is not None

    if exact_month_selected:
        if args.year is None or args.month is None:
            parser.error("Phai cung cap dong thoi --year va --month")
        if range_selected:
            parser.error("Khong dung --year/--month chung voi --start/--end")
        if args.year < 2009:
            parser.error("NYC TLC trip data bat dau tu nam 2009")
        if not 1 <= args.month <= 12:
            parser.error("--month phai nam trong khoang 1-12")
        selected = YearMonth(args.year, args.month)
        return selected, selected

    now = time.localtime()
    start = args.start or YearMonth(now.tm_year, 1)
    end = args.end or YearMonth(now.tm_year, now.tm_mon)
    return start, end


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    start, end = resolve_range(args, parser)

    if args.retries < 1:
        print("--retries phai lon hon hoac bang 1.", file=sys.stderr)
        return 2

    try:
        months = list(month_range(start, end))
    except ValueError as exc:
        print(f"Loi: {exc}", file=sys.stderr)
        return 2

    output_root = args.output.expanduser().resolve()
    statuses = ("downloaded", "skipped", "not_found", "failed")
    counters = {
        dataset: {status: 0 for status in statuses}
        for dataset in args.datasets
    }

    print(f"Khoang du lieu : {start} den {end}")
    print(f"Thu muc luu    : {output_root}")
    print(f"Bo du lieu     : {', '.join(args.datasets)}")
    print(f"Tong so file   : {len(months) * len(args.datasets)}")
    print()

    for dataset in args.datasets:
        print(f"\n===== {dataset.upper()} =====")
        file_prefix = DATASETS[dataset]
        for year_month in months:
            file_name = f"{file_prefix}_{year_month}.parquet"
            url = f"{BASE_URL}/{file_name}"
            destination = (
                output_root
                / dataset
                / f"year={year_month.year:04d}"
                / f"month={year_month.month:02d}"
                / file_name
            )
            status = download_file(url, destination, args.retries)
            counters[dataset][status] += 1

    print("\n===== KET QUA =====")
    for dataset in args.datasets:
        result = counters[dataset]
        print(
            f"{dataset:<7} | tai: {result['downloaded']:<3} "
            f"| da co: {result['skipped']:<3} "
            f"| chua cong bo: {result['not_found']:<3} "
            f"| loi: {result['failed']:<3}"
        )

    has_failure = any(result["failed"] for result in counters.values())
    exact_month_requested = args.year is not None and args.month is not None
    exact_month_missing = exact_month_requested and any(
        result["not_found"] for result in counters.values()
    )
    return 1 if has_failure or exact_month_missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
