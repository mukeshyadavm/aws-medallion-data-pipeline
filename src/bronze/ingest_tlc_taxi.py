#!/usr/bin/env python3
"""
Ingest NYC TLC Yellow Taxi Parquet files into the Bronze S3 layer.

Bronze is an immutable/raw landing layer: this script does not transform,
rewrite, or inspect the Parquet payload. It downloads a source file or reads a
local file, then uploads the unchanged bytes to S3.
"""

import argparse
import os
import tempfile
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

DEFAULT_TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload raw NYC TLC Yellow Taxi Parquet data to S3 Bronze."
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="Target S3 bucket name, for example my-portfolio-data-bucket.",
    )
    parser.add_argument(
        "--bronze-prefix",
        default="Bronze/yellow_tripdata",
        help="S3 prefix for raw Bronze objects.",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Dataset year, for example 2025. Required unless --source-file is provided.",
    )
    parser.add_argument(
        "--month",
        type=int,
        choices=range(1, 13),
        metavar="[1-12]",
        help="Dataset month. Required unless --source-file is provided.",
    )
    parser.add_argument(
        "--source-url",
        help="Optional explicit source URL. Defaults to the public TLC URL for year/month.",
    )
    parser.add_argument(
        "--source-file",
        help="Optional local Parquet file to upload instead of downloading.",
    )
    parser.add_argument(
        "--s3-key",
        help="Optional full S3 key. Defaults to Bronze/yellow_tripdata/year=YYYY/month=MM/<filename>.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned upload without downloading or writing to S3.",
    )
    return parser.parse_args()


def default_filename(year: int, month: int) -> str:
    return f"yellow_tripdata_{year}-{month:02d}.parquet"


def default_source_url(year: int, month: int) -> str:
    return f"{DEFAULT_TLC_BASE_URL}/{default_filename(year, month)}"


def default_s3_key(prefix: str, year: int, month: int, filename: str) -> str:
    clean_prefix = prefix.strip("/")
    return f"{clean_prefix}/year={year}/month={month:02d}/{filename}"


def download_file(url: str, destination: Path) -> None:
    with urllib.request.urlopen(url) as response, destination.open("wb") as output_file:
        output_file.write(response.read())


def upload_file(bucket: str, key: str, local_path: Path) -> None:
    import boto3

    s3 = boto3.client("s3")
    s3.upload_file(str(local_path), bucket, key)


def main() -> None:
    args = parse_args()

    if args.source_file:
        local_path = Path(args.source_file).expanduser().resolve()
        if not local_path.exists():
            raise FileNotFoundError(f"Source file not found: {local_path}")
        filename = local_path.name
        year = args.year
        month = args.month
    else:
        if args.year is None or args.month is None:
            raise ValueError("--year and --month are required when --source-file is not used")
        year = args.year
        month = args.month
        filename = Path(urlparse(args.source_url or default_source_url(year, month)).path).name

    if args.s3_key:
        s3_key = args.s3_key
    else:
        if year is None or month is None:
            raise ValueError("--year and --month are required to build the default S3 key")
        s3_key = default_s3_key(args.bronze_prefix, year, month, filename)

    if args.dry_run:
        source = args.source_file or args.source_url or default_source_url(year, month)
        print(f"source={source}")
        print(f"target=s3://{args.bucket}/{s3_key}")
        return

    if args.source_file:
        upload_file(args.bucket, s3_key, local_path)
    else:
        source_url = args.source_url or default_source_url(year, month)
        with tempfile.TemporaryDirectory() as tmpdir:
            download_path = Path(tmpdir) / filename
            download_file(source_url, download_path)
            upload_file(args.bucket, s3_key, download_path)

    print(f"Uploaded raw file to s3://{args.bucket}/{s3_key}")


if __name__ == "__main__":
    main()
