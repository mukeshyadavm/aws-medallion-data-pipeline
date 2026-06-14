# Gold Layer

## Purpose

The Gold aggregate layer stores business-ready summary tables for Athena and BI dashboards. These are reporting aggregates, not replacements for the atomic Star Schema fact table.

## Input

```text
s3://<bucket>/Silver/yellow_tripdata/
```

## Output

```text
s3://<bucket>/Gold/
```

## Glue Job

Script:

```text
glue_jobs/silver_to_gold_aggregates_glue.py
```

Required Glue arguments:

```text
--SILVER_INPUT_PATH=s3://<bucket>/Silver/yellow_tripdata/
--GOLD_OUTPUT_PATH=s3://<bucket>/Gold/
```

## Tables

```text
s3://<bucket>/Gold/daily_revenue/
s3://<bucket>/Gold/vendor_revenue/
s3://<bucket>/Gold/payment_type_analysis/
s3://<bucket>/Gold/passenger_analysis/
```

## Table Grains

- `gold_daily_revenue`: one row per pickup date.
- `gold_vendor_revenue`: one row per vendor per pickup month partition.
- `gold_payment_type_analysis`: one row per payment type per pickup month partition.
- `gold_passenger_analysis`: one row per passenger count per pickup month partition.

## Relationship To Star Schema

The aggregate Gold layer is useful for simple dashboards and quick validation. The dimensional warehouse is built separately by:

```text
glue_jobs/nyc_taxi_star_schema_glue.py
```

For portfolio interviews, explain that `fact_trip` is the atomic source of truth and Gold aggregate tables are performance/reporting marts.
