# NYC Taxi Star Schema ER Diagram

Target base path:

```text
s3://<bucket>/StarSchema/
```

## Complete ER Diagram

```text
                                      dim_date
                                  date_key PK
                     calendar_date, year, quarter, month,
                  week, day, weekend, month_start/end, year_month
                                          |
                 pickup_date_key / dropoff_date_key role-playing joins
                                          |
dim_time              dim_vendor           dim_payment_type       dim_rate_code
time_key PK           vendor_key PK        payment_type_key PK    rate_code_key PK
hour, minute,         vendorid,            payment_type,          ratecodeid,
second, daypart       vendor_name          description            description
    |                     |                       |                     |
    +---------------------+-----------------------+---------------------+
                                          |
                                      fact_trip
                                      trip_id PK
                                      ingestion_batch_key FK
                                      pickup_date_key FK
                                      dropoff_date_key FK
                                      pickup_time_key FK
                                      dropoff_time_key FK
                                      vendor_key FK
                                      payment_type_key FK
                                      rate_code_key FK
                                      pu_location_key FK
                                      do_location_key FK
                                      passenger_count_key FK
                                      trip_flags_key FK
                                      trip_quality_key FK
                                      source_file_name
                                      source_row_number
                                      pickup_ts, dropoff_ts
                                      distance, duration
                                      fare, tax, tip, toll, fee
                                      total_amount, tip_pct
                                          |
       +---------------------+------------+-------------+----------------------+
       |                     |                          |                      |
dim_location       dim_passenger_count          dim_trip_flags       dim_trip_quality
location_key PK    passenger_count_key PK       trip_flags_key PK    trip_quality_key PK
location_id,       passenger_count,             store_and_fwd_flag,  quality_code,
borough, zone,     passenger_group              description          quality flags
service_zone
       ^
       |
       +-- role-playing joins from fact_trip:
           pu_location_key and do_location_key

dim_ingestion_batch
ingestion_batch_key PK
batch_id, job_name, job_run_id, source system/table,
target path, load timestamps, source/valid/rejected counts
       ^
       |
       +-- fact_trip.ingestion_batch_key
```

## Aggregate Facts

```text
fact_daily_trip_summary
  Grain: pickup_date_key
  FK: pickup_date_key -> dim_date.date_key

fact_vendor_daily_summary
  Grain: pickup_date_key + vendor_key
  FK: pickup_date_key -> dim_date.date_key
  FK: vendor_key -> dim_vendor.vendor_key

fact_payment_daily_summary
  Grain: pickup_date_key + payment_type_key
  FK: pickup_date_key -> dim_date.date_key
  FK: payment_type_key -> dim_payment_type.payment_type_key

fact_location_daily_summary
  Grain: pickup_date_key + pu_location_key + do_location_key
  FK: pickup_date_key -> dim_date.date_key
  FK: pu_location_key -> dim_location.location_key
  FK: do_location_key -> dim_location.location_key
```

## Current Grain

`fact_trip` grain:

```text
One row per valid completed taxi trip from Silver, identified by batch id,
source file, source row number, timestamps, locations, vendor, and amount.
```

Summary fact grains are intentionally derived from `fact_trip`; they are not independent sources of truth.

## Remaining Architecture Review Before Deployment

Resolved mandatory items:

- Glue output base moved to `s3://<bucket>/StarSchema/`.
- Unsupported Spark `date_format(..., "u")` pattern removed.
- Unknown dimension rows added for operational dimensions.
- `dim_ingestion_batch` added for lineage and batch audit.
- `dim_trip_quality` added for data quality analysis.
- `dim_date` enhanced and generated from a fixed configurable range.
- `fact_trip` now carries batch, quality, source file, and source row lineage.
- `.env` removed and replaced with `.env.example`.

Remaining issues to address before claiming production readiness:

1. `dim_location` still uses placeholder geography unless `--LOCATION_LOOKUP_PATH` points to the TLC taxi zone lookup CSV. Upload the lookup before publishing maps or borough reports.
2. The job still uses Parquet overwrite instead of Iceberg ACID tables. This is acceptable for a portfolio rebuild, but production should use Iceberg with partition overwrite or merge semantics.
3. `source_row_number` is derived by sorting source-file rows. It is much better than no lineage, but a true ingestion framework should stamp immutable row ids at Silver creation time.
4. `dim_ingestion_batch` now appends, but duplicate batch ids are still possible if the same `--BATCH_ID` is reused. Production should enforce uniqueness with orchestration or Iceberg merge semantics.
5. The date dimension has no holiday calendar. Add federal holiday and NYC-specific event flags for richer demand analysis.
6. Aggregate facts should be created only for proven dashboard latency needs. Keep `fact_trip` as the governed source of truth.
