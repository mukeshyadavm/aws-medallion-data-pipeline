import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "src" / "bronze" / "ingest_tlc_taxi.py"
spec = importlib.util.spec_from_file_location("ingest_tlc_taxi", MODULE_PATH)
ingest_tlc_taxi = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ingest_tlc_taxi)


def test_default_filename_uses_tlc_monthly_pattern():
    assert ingest_tlc_taxi.default_filename(2025, 1) == "yellow_tripdata_2025-01.parquet"
    assert ingest_tlc_taxi.default_filename(2025, 12) == "yellow_tripdata_2025-12.parquet"


def test_default_source_url_points_to_public_tlc_parquet():
    assert (
        ingest_tlc_taxi.default_source_url(2025, 1)
        == "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
    )


def test_default_s3_key_preserves_raw_bronze_partition_layout():
    key = ingest_tlc_taxi.default_s3_key(
        "Bronze/yellow_tripdata/",
        2025,
        1,
        "yellow_tripdata_2025-01.parquet",
    )

    assert key == "Bronze/yellow_tripdata/year=2025/month=01/yellow_tripdata_2025-01.parquet"
