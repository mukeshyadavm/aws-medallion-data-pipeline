import importlib.util
from pathlib import Path

import pytest


pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession


MODULE_PATH = Path(__file__).resolve().parents[1] / "glue_jobs" / "bronze_to_silver_glue.py"
spec = importlib.util.spec_from_file_location("bronze_to_silver_glue", MODULE_PATH)
bronze_to_silver_glue = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bronze_to_silver_glue)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("nyc-taxi-silver-unit-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_build_silver_df_filters_invalid_rows_and_deduplicates(spark):
    rows = [
        {
            "vendorid": "1",
            "tpep_pickup_datetime": "2025-01-01 10:00:00",
            "tpep_dropoff_datetime": "2025-01-01 10:20:00",
            "passenger_count": "1",
            "trip_distance": "3.5",
            "ratecodeid": "1",
            "store_and_fwd_flag": "N",
            "pulocationid": "100",
            "dolocationid": "200",
            "payment_type": "1",
            "fare_amount": "18.5",
            "extra": "1.0",
            "mta_tax": "0.5",
            "tip_amount": "3.0",
            "tolls_amount": "0.0",
            "improvement_surcharge": "1.0",
            "total_amount": "24.0",
            "source_file_name": "file_a.parquet",
        },
        {
            "vendorid": "1",
            "tpep_pickup_datetime": "2025-01-01 10:00:00",
            "tpep_dropoff_datetime": "2025-01-01 10:20:00",
            "passenger_count": "1",
            "trip_distance": "3.5",
            "ratecodeid": "1",
            "store_and_fwd_flag": "N",
            "pulocationid": "100",
            "dolocationid": "200",
            "payment_type": "1",
            "fare_amount": "18.5",
            "extra": "1.0",
            "mta_tax": "0.5",
            "tip_amount": "3.0",
            "tolls_amount": "0.0",
            "improvement_surcharge": "1.0",
            "total_amount": "24.0",
            "source_file_name": "file_a.parquet",
        },
        {
            "vendorid": "2",
            "tpep_pickup_datetime": "2025-01-01 11:00:00",
            "tpep_dropoff_datetime": "2025-01-01 11:10:00",
            "passenger_count": "0",
            "trip_distance": "2.0",
            "ratecodeid": "1",
            "store_and_fwd_flag": "N",
            "pulocationid": "101",
            "dolocationid": "201",
            "payment_type": "2",
            "fare_amount": "12.0",
            "extra": "0.0",
            "mta_tax": "0.5",
            "tip_amount": "0.0",
            "tolls_amount": "0.0",
            "improvement_surcharge": "1.0",
            "total_amount": "13.5",
            "source_file_name": "file_b.parquet",
        },
    ]

    result = bronze_to_silver_glue.build_silver_df(spark.createDataFrame(rows)).collect()

    assert len(result) == 1
    assert result[0]["passenger_count"] == 1
    assert result[0]["pickup_year"] == 2025
    assert result[0]["pickup_month"] == 1
    assert round(result[0]["trip_duration_minutes"], 2) == 20.0


def test_build_silver_df_adds_optional_fee_columns_when_missing(spark):
    df = spark.createDataFrame(
        [
            {
                "vendorid": "1",
                "tpep_pickup_datetime": "2025-01-01 10:00:00",
                "tpep_dropoff_datetime": "2025-01-01 10:20:00",
                "passenger_count": "1",
                "trip_distance": "3.5",
                "ratecodeid": "1",
                "store_and_fwd_flag": "N",
                "pulocationid": "100",
                "dolocationid": "200",
                "payment_type": "1",
                "fare_amount": "18.5",
                "extra": "1.0",
                "mta_tax": "0.5",
                "tip_amount": "3.0",
                "tolls_amount": "0.0",
                "improvement_surcharge": "1.0",
                "total_amount": "24.0",
                "source_file_name": "file_a.parquet",
            }
        ]
    )

    result = bronze_to_silver_glue.build_silver_df(df)

    assert "airport_fee" in result.columns
    assert "congestion_surcharge" in result.columns
    assert "cbd_congestion_fee" in result.columns
