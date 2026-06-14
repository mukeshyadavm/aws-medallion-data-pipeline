# NYC Taxi AWS Data Warehouse Design

## A. Current Architecture Review

Observed from AWS Glue and S3 on 2026-06-14:

- Glue database: `nyc_taxi_db`
- Bronze table: `bronze`, stored at `s3://mukesh-bucket420/Bronze/`
- Silver table: `silver_silver`, stored at `s3://mukesh-bucket420/Silver/`
- Gold tables:
  - `gold_daily_revenue` at `s3://mukesh-bucket420/Gold/daily_revenue/`
  - `gold_vendor_revenue` at `s3://mukesh-bucket420/Gold/vendor_revenue/`
  - `gold_payment_type_analysis` at `s3://mukesh-bucket420/Gold/payment_type_analysis/`
  - `gold_passenger_analysis` at `s3://mukesh-bucket420/Gold/passenger_analysis/`
- S3 root prefixes: `Bronze/`, `Silver/`, `Gold/`, `Documentation/`, `athena-results/`, `scripts/`
- Bronze contains one file: `yellow_tripdata_2025-01.parquet`
- Bronze record count from crawler metadata: about 3.48M rows
- Silver record count from crawler metadata: about 2.82M rows
- Current Glue tables have no partition keys registered.

Current flow:

```text
NYC Taxi Parquet
   -> S3 Bronze raw landing
   -> Glue/Spark Silver cleaning and derived pickup date columns
   -> Gold pre-aggregated reporting tables
   -> Athena / BI
```

Assessment:

- The Bronze/Silver/Gold layering is directionally correct for a portfolio lakehouse.
- Silver is the right source for curated dimensional modeling because it has cleaned fields and derived pickup date attributes.
- Gold currently serves reporting summaries, not a conformed dimensional warehouse.
- No registered partitions means Athena scans are not optimized by date.
- The current catalog has no dimensions, no trip-grain fact table, no conformed date/time/location/vendor/payment dimensions, and no durable business keys.

## B. Current Gold Layer Assessment

The current Gold tables are aggregated reporting tables, not true fact tables in the Kimball sense.

Why:

- `gold_daily_revenue` is aggregated to one row per pickup date.
- `gold_vendor_revenue` is aggregated to one row per vendor.
- `gold_payment_type_analysis` is aggregated to one row per payment type.
- `gold_passenger_analysis` is aggregated to one row per passenger count.
- They do not preserve the atomic business event: one taxi trip.
- They do not contain foreign keys to conformed dimensions.
- Their grains are different, so they cannot be safely joined together without double-counting.

They are still useful as BI acceleration marts, but they should sit after the dimensional model, not replace it.

## C. Recommended Star Schema

Recommended Kimball model:

- Atomic fact: `fact_trip`
- Optional aggregate facts:
  - `fact_daily_trip_summary`
  - `fact_vendor_daily_summary`
  - `fact_payment_daily_summary`
  - `fact_location_daily_summary`
- Conformed dimensions:
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

Primary business process:

```text
Yellow taxi passenger trip completion and payment
```

Atomic grain:

```text
One row per completed taxi trip from Silver.
```

## D. Fact Tables

### `fact_trip`

Grain: one row per taxi trip.

Built from: `silver_silver`

Keys:

- `trip_id`: deterministic hash of vendor, timestamps, locations, fare, total, and distance
- `pickup_date_key`, `dropoff_date_key`
- `pickup_time_key`, `dropoff_time_key`
- `vendor_key`
- `payment_type_key`
- `rate_code_key`
- `pu_location_key`
- `do_location_key`
- `passenger_count_key`
- `trip_flags_key`

Measures:

- `trip_count`
- `passenger_count`
- `trip_distance`
- `trip_duration_minutes`
- `fare_amount`
- `extra`
- `mta_tax`
- `tip_amount`
- `tolls_amount`
- `improvement_surcharge`
- `congestion_surcharge`
- `airport_fee`
- `cbd_congestion_fee`
- `total_amount`
- `tip_pct`

Business questions:

- What is total revenue by day, hour, vendor, payment type, pickup zone, or dropoff zone?
- What are average fare, average distance, and average duration trends?
- Which pickup/dropoff routes produce the highest revenue?
- Which payment methods produce the highest tips?
- Which vendors have better revenue per mile?

Partitioning:

- `pickup_year`, `pickup_month`
- Add `pickup_day` only if daily refreshes and data volume justify it.

### `fact_daily_trip_summary`

Grain: one row per pickup date.

Built from: `fact_trip`, or directly from Silver if the atomic fact has not yet been deployed.

Measures:

- `total_trips`
- `total_passengers`
- `total_revenue`
- `total_fare`
- `total_tips`
- `avg_trip_distance`
- `avg_trip_duration_minutes`
- `avg_revenue`

Business questions:

- What are daily trip and revenue trends?
- Which days are peak demand days?
- Is average trip value increasing or decreasing?

Partitioning:

- `pickup_year`

### `fact_vendor_daily_summary`

Grain: one row per pickup date and vendor.

Built from: `fact_trip`.

Business questions:

- Which vendor generates more trips and revenue by day?
- Which vendor has higher fare per mile or tip percentage?

Partitioning:

- `pickup_year`, `pickup_month`

### `fact_payment_daily_summary`

Grain: one row per pickup date and payment type.

Built from: `fact_trip`.

Business questions:

- How do card, cash, dispute, and no-charge trips differ?
- Which payment type has the highest tip rate?

Partitioning:

- `pickup_year`, `pickup_month`

### `fact_location_daily_summary`

Grain: one row per pickup date, pickup location, and dropoff location.

Built from: `fact_trip`.

Business questions:

- What are the highest-volume pickup/dropoff routes?
- Which zones produce the highest revenue and tips?
- Which routes have the longest average distance or duration?

Partitioning:

- `pickup_year`, `pickup_month`

## E. Dimension Tables

### `dim_date`

Why needed: standard calendar analysis, conformed across all facts.

Key:

- `date_key`: `yyyyMMdd`

Attributes:

- date, year, quarter, month, month name, day, day of week, weekend flag

Answers:

- Revenue by month, quarter, weekday, weekend, or holiday.

Build from:

- Silver pickup and dropoff dates.

Partition:

- Do not partition; small dimension.

### `dim_time`

Why needed: hour and time-of-day analysis without timestamp parsing in BI.

Key:

- `time_key`: `HHMMSS`

Attributes:

- hour, minute, second, daypart

Answers:

- Rush-hour demand, overnight behavior, hourly revenue patterns.

Build from:

- Generated 24-hour time dimension.

Partition:

- Do not partition.

### `dim_vendor`

Why needed: translates vendor codes into business labels.

Key:

- `vendor_key`

Natural key:

- `vendorid`

Attributes:

- vendor name, active flag

Answers:

- Vendor market share, vendor revenue, vendor fare patterns.

Build from:

- Reference mapping plus Silver distinct vendor IDs.

Partition:

- Do not partition.

### `dim_payment_type`

Why needed: conformed payment classification.

Key:

- `payment_type_key`

Natural key:

- `payment_type`

Answers:

- Revenue, tip behavior, and dispute/no-charge patterns by payment type.

Build from:

- Reference mapping plus Silver distinct payment codes.

Partition:

- Do not partition.

### `dim_rate_code`

Why needed: identifies fare/rate context such as standard, JFK, Newark, negotiated fare, or group ride.

Key:

- `rate_code_key`

Natural key:

- `ratecodeid`

Answers:

- Revenue by rate category, airport trip analysis, negotiated-fare analysis.

Build from:

- Reference mapping plus Silver distinct rate codes.

Partition:

- Do not partition.

### `dim_location`

Why needed: pickup and dropoff geography is one of the most important taxi analytics dimensions.

Key:

- `location_key`

Natural key:

- `location_id`

Recommended attributes:

- borough, zone, service_zone

Current limitation:

- The inspected project does not show a taxi zone lookup file. Until one is added, build this dimension from distinct `pulocationid` and `dolocationid` with placeholder geographic attributes.

Answers:

- Revenue by zone, pickup/dropoff route, borough performance, airport flow.

Build from:

- Silver distinct pickup/dropoff location IDs now.
- Production improvement: add TLC taxi zone lookup as a reference source.

Partition:

- Do not partition.

### `dim_passenger_count`

Why needed: useful small demographic-like trip grouping.

Key:

- `passenger_count_key`

Natural key:

- `passenger_count`

Answers:

- Revenue, distance, and tip behavior by passenger count.

Build from:

- Silver distinct passenger counts.

Partition:

- Do not partition.

### `dim_trip_flags`

Why needed: decodes operational flags such as store-and-forward.

Key:

- `trip_flags_key`

Natural key:

- `store_and_fwd_flag`

Answers:

- Are stored-and-forward trips materially different?

Build from:

- Silver distinct flags.

Partition:

- Do not partition.

## F. ER Diagram

```text
                         dim_date
                      date_key PK
                            |
                            | pickup_date_key / dropoff_date_key
                            |
dim_time          dim_vendor        dim_payment_type       dim_rate_code
time_key PK       vendor_key PK     payment_type_key PK    rate_code_key PK
     |                 |                   |                    |
     |                 |                   |                    |
     +-----------------+-------------------+--------------------+
                         fact_trip
                      trip_id PK
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
                      fare/tax/tip/toll/fee/revenue measures
                            |
     +----------------------+----------------------+
     |                      |                      |
dim_location          dim_passenger_count     dim_trip_flags
location_key PK       passenger_count_key PK  trip_flags_key PK

Aggregate facts derived from fact_trip:

fact_daily_trip_summary
  pickup_date_key -> dim_date

fact_vendor_daily_summary
  pickup_date_key -> dim_date
  vendor_key -> dim_vendor

fact_payment_daily_summary
  pickup_date_key -> dim_date
  payment_type_key -> dim_payment_type

fact_location_daily_summary
  pickup_date_key -> dim_date
  pu_location_key -> dim_location
  do_location_key -> dim_location
```

## G. Glue ETL Design

Recommended ETL sequence:

1. Bronze job: land raw monthly TLC Parquet files unchanged under `Bronze/yellow_tripdata/year=YYYY/month=MM/`.
2. Silver job: standardize schema, cast data types, remove invalid records, add data quality columns, write partitioned Parquet under `Silver/yellow_tripdata/pickup_year=YYYY/pickup_month=MM/`.
3. Gold dimensional job: build dimensions and `fact_trip` from Silver.
4. Gold aggregate job: build summary facts from `fact_trip`.
5. Crawlers or explicit Glue Catalog DDL: register dimensions and facts.

Build from Silver:

- `dim_date`
- `dim_time`
- `dim_vendor`
- `dim_payment_type`
- `dim_rate_code`
- `dim_location`
- `dim_passenger_count`
- `dim_trip_flags`
- `fact_trip`

Build from existing Gold:

- Nothing required for the dimensional core. Existing Gold tables can be retained for comparison or rebuilt from `fact_trip`.

Recommended S3 layout:

```text
s3://mukesh-bucket420/StarSchema/dim_date/
s3://mukesh-bucket420/StarSchema/dim_time/
s3://mukesh-bucket420/StarSchema/dim_vendor/
s3://mukesh-bucket420/StarSchema/dim_payment_type/
s3://mukesh-bucket420/StarSchema/dim_rate_code/
s3://mukesh-bucket420/StarSchema/dim_location/
s3://mukesh-bucket420/StarSchema/dim_passenger_count/
s3://mukesh-bucket420/StarSchema/dim_trip_flags/
s3://mukesh-bucket420/StarSchema/dim_trip_quality/
s3://mukesh-bucket420/StarSchema/dim_ingestion_batch/
s3://mukesh-bucket420/StarSchema/fact_trip/pickup_year=YYYY/pickup_month=MM/
s3://mukesh-bucket420/StarSchema/fact_daily_trip_summary/pickup_year=YYYY/
s3://mukesh-bucket420/StarSchema/fact_vendor_daily_summary/pickup_year=YYYY/pickup_month=MM/
s3://mukesh-bucket420/StarSchema/fact_payment_daily_summary/pickup_year=YYYY/pickup_month=MM/
s3://mukesh-bucket420/StarSchema/fact_location_daily_summary/pickup_year=YYYY/pickup_month=MM/
```

Surrogate key strategy:

- Use deterministic smart keys for small static dimensions in this portfolio project.
- `date_key = yyyyMMdd`
- `time_key = HHMMSS`
- `location_key = TLC LocationID`
- `vendor_key = vendorid`
- `payment_type_key = payment_type`
- `rate_code_key = ratecodeid`
- `passenger_count_key = passenger_count`
- `trip_flags_key = fixed code for store-and-forward flag`
- `trip_quality_key = fixed code for quality category`
- `ingestion_batch_key = numeric batch id`
- `trip_id = SHA-256 hash of batch id, source file, source row number, and stable trip attributes`

Production upgrade:

- Use Apache Iceberg tables for ACID upserts, schema evolution, compaction, and reliable surrogate-key management.
- Keep SCD Type 2 columns on dimensions if business attributes change: `effective_start_date`, `effective_end_date`, `is_current`.

## H. Athena Validation Queries

See `sql/athena_validation_queries.sql`.

Validation categories:

- Row counts by layer
- Fact-to-Silver reconciliation
- Revenue reconciliation
- Null foreign key checks
- Dimension uniqueness checks
- Partition checks
- Business metric sanity checks

## I. Power BI / Tableau / QuickSight Reporting Model

Use `fact_trip` as the central model table for detailed analysis. Import or direct-query dimensions around it in a classic star schema.

Recommended relationships:

- `fact_trip.pickup_date_key` -> `dim_date.date_key`
- `fact_trip.pickup_time_key` -> `dim_time.time_key`
- `fact_trip.vendor_key` -> `dim_vendor.vendor_key`
- `fact_trip.payment_type_key` -> `dim_payment_type.payment_type_key`
- `fact_trip.rate_code_key` -> `dim_rate_code.rate_code_key`
- `fact_trip.pu_location_key` -> `dim_location.location_key`
- `fact_trip.do_location_key` -> a role-playing copy of `dim_location`
- `fact_trip.passenger_count_key` -> `dim_passenger_count.passenger_count_key`
- `fact_trip.trip_quality_key` -> `dim_trip_quality.trip_quality_key`
- `fact_trip.ingestion_batch_key` -> `dim_ingestion_batch.ingestion_batch_key`

Recommended dashboards:

- Executive overview: trips, revenue, fares, tips, distance, revenue per trip.
- Time analysis: daily, weekday/weekend, hour, daypart.
- Vendor analysis: trips, revenue, average fare, revenue per mile.
- Payment analysis: card versus cash revenue and tip percentage.
- Location analysis: pickup zones, dropoff zones, routes, boroughs once zone lookup is added.

BI modeling notes:

- Hide raw technical keys from report users.
- Expose measures from facts and labels from dimensions.
- Use aggregate facts for high-level dashboards if Athena latency becomes an issue.
- Do not join current aggregate Gold tables together in BI; use the star schema to avoid double-counting.

## Glue Crawler Recommendations

Recommended crawlers:

- `nyc_taxi_bronze_crawler`: crawl only `s3://mukesh-bucket420/Bronze/`
- `nyc_taxi_silver_crawler`: crawl only `s3://mukesh-bucket420/Silver/`
- `nyc_taxi_star_schema_dimensions_crawler`: crawl `s3://mukesh-bucket420/StarSchema/dim_*/`
- `nyc_taxi_star_schema_facts_crawler`: crawl `s3://mukesh-bucket420/StarSchema/fact_*/`

Crawler settings:

- Use one database: `nyc_taxi_db`
- Update table definitions for compatible schema changes.
- Do not delete tables automatically; deprecate intentionally.
- Enable partition discovery for partitioned facts.
- Prefer explicit table names or separate folders per table to avoid crawler-created names like `silver_silver`.
- Run crawlers after Glue ETL jobs complete.

Best practice improvement:

- For production, create external tables explicitly with Athena/Glue DDL or use Iceberg instead of relying only on crawlers. Crawlers are convenient for portfolios and discovery, but explicit schemas are more controlled.
