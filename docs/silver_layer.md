# Silver Layer

## Purpose

The Silver layer stores cleaned, typed, analytics-ready NYC Yellow Taxi trip records. It is the curated source for both legacy Gold aggregates and the Star Schema warehouse job.

## Input

```text
s3://<bucket>/Bronze/yellow_tripdata/
```

## Output

```text
s3://<bucket>/Silver/yellow_tripdata/
```

Partition layout:

```text
pickup_year=YYYY/pickup_month=MM/
```

## Glue Job

Script:

```text
glue_jobs/bronze_to_silver_glue.py
```

Required Glue arguments:

```text
--BRONZE_INPUT_PATH=s3://<bucket>/Bronze/yellow_tripdata/
--SILVER_OUTPUT_PATH=s3://<bucket>/Silver/yellow_tripdata/
```

## Cleaning Rules

The Silver job:

- Casts core taxi fields to explicit types.
- Removes rows with null `passenger_count`.
- Removes rows where `passenger_count <= 0`.
- Removes rows where `trip_distance <= 0`.
- Removes rows where `fare_amount <= 0`.
- Removes rows where `total_amount <= 0`.
- Removes rows with null pickup/dropoff timestamps.
- Removes rows where dropoff time is before pickup time.
- Removes duplicate rows.
- Adds `pickup_date`, `dropoff_date`, `pickup_year`, `pickup_month`, `pickup_day`, and `trip_duration_minutes`.
- Carries `source_file_name` for lineage.

## Notes

This job uses Parquet overwrite for portfolio reproducibility. In production, use Iceberg or a staged write pattern with atomic promotion.
