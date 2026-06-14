# Pre-Deployment Architecture Review

## Scope Reviewed

- Glue job: `glue_jobs/nyc_taxi_star_schema_glue.py`
- Athena DDL: `sql/athena_star_schema_ddl.sql`
- Validation SQL: `sql/athena_validation_queries.sql`
- ER diagram: `docs/star_schema_er_diagram.md`
- Target S3 base path: `s3://<bucket>/StarSchema/`

## Mandatory Issues Resolved

1. Exposed credentials removed.
   - `.env` was removed.
   - `.env.example` contains only redacted placeholders.
   - `.gitignore` now excludes `.env`.

2. Glue 4.0 Spark date-pattern failure fixed.
   - Removed unsupported `date_format(..., "u")`.
   - ISO weekday number now uses `dayofweek` arithmetic.

3. Star schema output location corrected.
   - Glue, DDL, validation, and docs now target `s3://<bucket>/StarSchema/`.

4. Missing dimensions added.
   - `dim_ingestion_batch`
   - `dim_trip_quality`
   - Enhanced `dim_date`
   - `dim_location` retained and enhanced with optional TLC lookup support.

5. Fact table updated.
   - `fact_trip` now includes `ingestion_batch_key`, `trip_quality_key`, `source_file_name`, and `source_row_number`.
   - Unknown/default foreign key handling added.
   - Batch and quality dimensions are connected in the ER model.

6. Glue performance improved.
   - `clean` and `fact_trip` are persisted with `MEMORY_AND_DISK`.
   - `fact_trip` is repartitioned by `pickup_year`, `pickup_month` before write.

## Current Star Schema Assessment

The model is now a proper Kimball-style star schema for a portfolio-grade taxi analytics warehouse.

Atomic grain:

```text
One row per valid completed taxi trip from Silver.
```

Conformed dimensions:

- `dim_date`
- `dim_time`
- `dim_vendor`
- `dim_payment_type`
- `dim_rate_code`
- `dim_location`
- `dim_passenger_count`
- `dim_trip_flags`
- `dim_trip_quality`
- `dim_ingestion_batch`

Facts:

- `fact_trip`
- `fact_daily_trip_summary`
- `fact_vendor_daily_summary`
- `fact_payment_daily_summary`
- `fact_location_daily_summary`

The current grains are correct. The aggregate facts are derived from `fact_trip`, so they do not create conflicting sources of truth.

## Remaining Issues Before Deployment

### P1: Real location enrichment is still optional

`dim_location` supports `--LOCATION_LOOKUP_PATH`, but if that argument is empty the job will create placeholder zone values.

Recommendation:

- Upload the TLC taxi zone lookup CSV to S3, for example:

```text
s3://<bucket>/Reference/taxi_zone_lookup.csv
```

- Pass:

```text
--LOCATION_LOOKUP_PATH=s3://your-bucket/Reference/taxi_zone_lookup.csv
```

Do this before using maps, borough reporting, zone reporting, or route analytics in QuickSight.

### P2: Parquet overwrite is acceptable for portfolio, not production

The job overwrites dimensions and facts except `dim_ingestion_batch`, which appends.

Recommendation:

- For interview/demo deployment: acceptable.
- For production: use Apache Iceberg tables with partition overwrite, merge semantics, compaction, and schema evolution.

### P2: Source row number should ideally be stamped in Silver

The job derives `source_row_number` by sorting records within `source_file_name`. This is useful, but lineage is stronger if immutable row ids are created when Silver is written.

Recommendation:

- Add `source_file_name`, `source_row_number`, `source_system`, `ingestion_timestamp`, and `batch_id` in the Silver ETL.
- Treat Gold as a consumer of already-stamped Silver records.

### P2: DDL drops and recreates external tables

The Athena DDL uses `DROP TABLE IF EXISTS` for clean rebuilds.

Recommendation:

- Accept for the initial portfolio deployment.
- For production, use migration-controlled DDL or Iceberg.

### P3: Holiday and event calendar is not included

The enhanced date dimension includes standard calendar attributes but not holiday/event flags.

Recommendation:

- Add `is_federal_holiday`, `holiday_name`, and `is_major_nyc_event_day` later.

### P3: Data quality dimension is rule-based and simple

`dim_trip_quality` currently classifies valid rows into standard, zero distance, long duration, or high amount.

Recommendation:

- Expand it after profiling with percentile thresholds by month and rate code.

## Deployment Decision

Deploy after supplying the TLC taxi zone lookup path.

Without the location lookup, the pipeline will run, but the warehouse will not be strong enough for location-based dashboard claims.

For a portfolio interview, this is now a defensible architecture if you clearly state:

- Silver is the curated source.
- `fact_trip` is the atomic source of truth.
- Aggregates are performance marts.
- Parquet overwrite is used for simplicity.
- Iceberg is the recommended production upgrade.
