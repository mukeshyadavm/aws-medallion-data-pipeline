import importlib.util
from pathlib import Path

import pytest


pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession


MODULE_PATH = Path(__file__).resolve().parents[1] / "glue_jobs" / "silver_to_gold_aggregates_glue.py"
spec = importlib.util.spec_from_file_location("silver_to_gold_aggregates_glue", MODULE_PATH)
silver_to_gold_aggregates_glue = importlib.util.module_from_spec(spec)
spec.loader.exec_module(silver_to_gold_aggregates_glue)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("nyc-taxi-gold-unit-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_build_gold_aggregates_calculates_expected_metrics(spark):
    rows = [
        {
            "vendorid": 1,
            "payment_type": 1,
            "passenger_count": 1,
            "tpep_pickup_datetime": "2025-01-01 10:00:00",
            "trip_distance": 2.0,
            "fare_amount": 10.0,
            "tip_amount": 2.0,
            "total_amount": 13.0,
        },
        {
            "vendorid": 1,
            "payment_type": 1,
            "passenger_count": 2,
            "tpep_pickup_datetime": "2025-01-01 11:00:00",
            "trip_distance": 4.0,
            "fare_amount": 20.0,
            "tip_amount": 4.0,
            "total_amount": 26.0,
        },
        {
            "vendorid": 2,
            "payment_type": 2,
            "passenger_count": 1,
            "tpep_pickup_datetime": "2025-01-02 09:00:00",
            "trip_distance": 3.0,
            "fare_amount": 15.0,
            "tip_amount": 0.0,
            "total_amount": 17.0,
        },
    ]

    aggregates = silver_to_gold_aggregates_glue.build_gold_aggregates(spark.createDataFrame(rows))

    daily = {row["pickup_date"].isoformat(): row.asDict() for row in aggregates["daily_revenue"].collect()}
    assert daily["2025-01-01"]["total_trips"] == 2
    assert daily["2025-01-01"]["total_passengers"] == 3
    assert daily["2025-01-01"]["total_revenue"] == 39.0
    assert daily["2025-01-01"]["avg_trip_distance"] == 3.0

    payment = {row["payment_type"]: row.asDict() for row in aggregates["payment_type_analysis"].collect()}
    assert payment[1]["total_trips"] == 2
    assert payment[1]["tip_pct"] == 0.2

    vendor = {row["vendorid"]: row.asDict() for row in aggregates["vendor_revenue"].collect()}
    assert vendor[1]["total_revenue"] == 39.0

    passenger = {row["passenger_count"]: row.asDict() for row in aggregates["passenger_analysis"].collect()}
    assert passenger[1]["total_trips"] == 2
