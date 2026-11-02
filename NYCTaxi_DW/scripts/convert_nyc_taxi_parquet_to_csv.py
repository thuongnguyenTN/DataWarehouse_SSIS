#!/usr/bin/env python3
"""Chuyen du lieu NYC Taxi tu Parquet sang CSV theo tung batch.

Mac dinh:
  Input : D:\\Kho_Du_lieu\\NYCTaxi\\raw
  Output: D:\\Kho_Du_lieu\\NYCTaxi\\csv

Cau truc thu muc year=YYYY/month=MM va ten file duoc giu nguyen.
Script ho tro bon tap du lieu: yellow, green, fhv va fhvhv.

Vi du chuyen mot thang:
  python convert_nyc_taxi_parquet_to_csv.py --year 2026 --month 1
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from pathlib import Path


DATASET_PREFIXES = {
    "yellow": "yellow_tripdata",
    "green": "green_tripdata",
    "fhv": "fhv_tripdata",
    "fhvhv": "fhvhv_tripdata",
}
DATASETS = tuple(DATASET_PREFIXES)
DEFAULT_INPUT = Path(r"D:\Kho_Du_lieu\NYCTaxi\raw")
DEFAULT_OUTPUT = Path(r"D:\Kho_Du_lieu\NYCTaxi\csv")


@dataclass
class Result:
    converted: int = 0
    skipped: int = 0
    failed: int = 0
    rows: int = 0


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


def human_size(byte_count: int) -> str:
    size = float(byte_count)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def destination_for(source: Path, dataset_root: Path, output_root: Path) -> Path:
    relative_path = source.relative_to(dataset_root)
    return output_root / dataset_root.name / relative_path.with_suffix(".csv")


def convert_one(
    source: Path,
    destination: Path,
    batch_size: int,
    delimiter: str,
    overwrite: bool,
) -> tuple[str, int]:
    try:
        import pyarrow.csv as arrow_csv
        import pyarrow.parquet as parquet
    except ImportError as exc:
        raise RuntimeError(
            "Thieu pyarrow. Cai bang lenh: py -m pip install pyarrow"
        ) from exc

    if destination.exists() and destination.stat().st_size > 0 and not overwrite:
        destination.with_suffix(destination.suffix + ".part").unlink(missing_ok=True)
        print(f"[BO QUA] Da ton tai: {destination}")
        return "skipped", 0

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".part")
    temporary.unlink(missing_ok=True)

    parquet_file = parquet.ParquetFile(source)
    total_rows = parquet_file.metadata.num_rows
    write_options = arrow_csv.WriteOptions(
        include_header=True,
        delimiter=delimiter,
    )

    processed_rows = 0
    started_at = time.monotonic()
    print(
        f"[CHUYEN] {source} -> {destination} "
        f"({total_rows:,} dong, {human_size(source.stat().st_size)})"
    )

    try:
        with arrow_csv.CSVWriter(
            str(temporary),
            parquet_file.schema_arrow,
            write_options=write_options,
        ) as writer:
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                writer.write_batch(batch)
                processed_rows += batch.num_rows
                percent = 100.0 if total_rows == 0 else processed_rows * 100 / total_rows
                print(
                    f"\r         {processed_rows:,}/{total_rows:,} dong "
                    f"({percent:5.1f}%)",
                    end="",
                    flush=True,
                )
        if total_rows:
            print()

        if not temporary.exists() or temporary.stat().st_size == 0:
            raise RuntimeError("File CSV tam khong duoc tao hoac rong.")

        temporary.replace(destination)
        elapsed = time.monotonic() - started_at
        print(
            f"[XONG] {destination} - {processed_rows:,} dong, "
            f"{human_size(destination.stat().st_size)}, {elapsed:.1f} giay"
        )
        return "converted", processed_rows
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def parse_args() -> tuple[argparse.Namespace, list[YearMonth] | None]:
    parser = argparse.ArgumentParser(
        description=(
            "Chuyen cac file NYC Taxi Parquet sang CSV theo tung batch, "
            "khong nap toan bo du lieu vao RAM."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Thu muc Parquet goc (mac dinh: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Thu muc CSV dich (mac dinh: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=DATASETS,
        default=list(DATASETS),
        help="Loai du lieu can chuyen (mac dinh: ca 4 loai)",
    )
    parser.add_argument("--year", type=int, help="Nam can chuyen. Dung cung --month.")
    parser.add_argument(
        "--month", type=int, help="Thang can chuyen (1-12). Dung cung --year."
    )
    parser.add_argument(
        "--start", type=YearMonth.parse, help="Thang bat dau, dinh dang YYYY-MM."
    )
    parser.add_argument(
        "--end", type=YearMonth.parse, help="Thang ket thuc, dinh dang YYYY-MM."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
        help="So dong doc moi batch (mac dinh: 100000)",
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help="Ky tu phan tach CSV, chi duoc mot ky tu (mac dinh: dau phay)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Ghi de file CSV da ton tai",
    )
    args = parser.parse_args()

    if args.batch_size <= 0:
        parser.error("--batch-size phai lon hon 0")
    if len(args.delimiter) != 1:
        parser.error("--delimiter phai la dung mot ky tu")

    exact_month_selected = args.year is not None or args.month is not None
    range_selected = args.start is not None or args.end is not None
    selected_months: list[YearMonth] | None = None

    if exact_month_selected:
        if args.year is None or args.month is None:
            parser.error("Phai cung cap dong thoi --year va --month")
        if range_selected:
            parser.error("Khong dung --year/--month chung voi --start/--end")
        if args.year < 2009:
            parser.error("NYC TLC trip data bat dau tu nam 2009")
        if not 1 <= args.month <= 12:
            parser.error("--month phai nam trong khoang 1-12")
        selected_months = [YearMonth(args.year, args.month)]
    elif range_selected:
        if args.start is None or args.end is None:
            parser.error("Phai cung cap dong thoi --start va --end")
        try:
            selected_months = list(month_range(args.start, args.end))
        except ValueError as exc:
            parser.error(str(exc))

    return args, selected_months


def sources_for(
    dataset: str,
    dataset_root: Path,
    selected_months: list[YearMonth] | None,
) -> list[Path]:
    if selected_months is None:
        return sorted(dataset_root.rglob("*.parquet"))

    prefix = DATASET_PREFIXES[dataset]
    return [
        dataset_root
        / f"year={year_month.year:04d}"
        / f"month={year_month.month:02d}"
        / f"{prefix}_{year_month}.parquet"
        for year_month in selected_months
    ]


def main() -> int:
    args, selected_months = parse_args()
    result = Result()

    print(f"Input : {args.input}")
    print(f"Output: {args.output}")
    print(f"Dataset: {', '.join(args.datasets)}")

    for dataset in args.datasets:
        dataset_root = args.input / dataset
        if not dataset_root.is_dir():
            print(f"[BO QUA] Khong tim thay thu muc: {dataset_root}")
            continue

        sources = sources_for(dataset, dataset_root, selected_months)
        if not sources:
            print(f"[BO QUA] Khong co file Parquet trong: {dataset_root}")
            continue

        for source in sources:
            if not source.is_file():
                print(f"[BO QUA] Khong tim thay file: {source}")
                continue
            destination = destination_for(source, dataset_root, args.output)
            try:
                status, row_count = convert_one(
                    source=source,
                    destination=destination,
                    batch_size=args.batch_size,
                    delimiter=args.delimiter,
                    overwrite=args.overwrite,
                )
                if status == "converted":
                    result.converted += 1
                    result.rows += row_count
                else:
                    result.skipped += 1
            except Exception as exc:
                result.failed += 1
                print(f"[LOI] {source}: {exc}", file=sys.stderr)

    print("\n===== TONG KET =====")
    print(f"Da chuyen : {result.converted} file")
    print(f"Da bo qua : {result.skipped} file")
    print(f"Bi loi     : {result.failed} file")
    print(f"Tong dong : {result.rows:,}")
    return 1 if result.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
