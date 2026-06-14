# Glue Deployment and QuickSight Design

## AWS Glue Job Deployment Steps

1. Upload the Glue script:

```bash
aws s3 cp glue_jobs/nyc_taxi_star_schema_glue.py s3://<bucket>/scripts/nyc_taxi_star_schema_glue.py
```

2. Create the Glue job:

```bash
aws glue create-job \
  --name nyc_taxi_gold_star_schema_job \
  --role Glue-Access-S3 \
  --glue-version 4.0 \
  --worker-type G.1X \
  --number-of-workers 4 \
  --command 'Name=glueetl,ScriptLocation=s3://<bucket>/scripts/nyc_taxi_star_schema_glue.py,PythonVersion=3' \
  --default-arguments '{
    "--job-language": "python",
    "--DATABASE_NAME": "nyc_taxi_db",
    "--SILVER_TABLE_NAME": "silver_silver",
    "--OUTPUT_BASE": "s3://your-bucket/StarSchema/",
    "--LOCATION_LOOKUP_PATH": "",
    "--enable-metrics": "true",
    "--enable-continuous-cloudwatch-log": "true",
    "--enable-glue-datacatalog": "true",
    "--TempDir": "s3://<bucket>/temp/glue/"
  }'
```

3. Run the job:

```bash
aws glue start-job-run --job-name nyc_taxi_gold_star_schema_job
```

4. Check job status:

```bash
aws glue get-job-runs --job-name nyc_taxi_gold_star_schema_job --max-results 5
```

5. Create Athena external tables:

```bash
aws s3 cp sql/athena_star_schema_ddl.sql s3://<bucket>/scripts/athena_star_schema_ddl.sql
```

Run `sql/athena_star_schema_ddl.sql` in Athena using database `nyc_taxi_db`.

`--OUTPUT_BASE` is a required Glue job parameter. Replace `your-bucket` with the target S3 bucket before deployment.

6. Validate:

Run `sql/athena_validation_queries.sql` in Athena.

7. Optional crawler alternative:

Use crawlers for discovery, but prefer explicit DDL for portfolio interviews because it shows schema control.

Recommended crawlers:

- `nyc_taxi_gold_star_dimensions_crawler`
- `nyc_taxi_gold_star_facts_crawler`

## QuickSight Dataset Design

Primary dataset:

- `fact_trip`

Joined dimensions:

- `dim_date` on `fact_trip.pickup_date_key = dim_date.date_key`
- `dim_time` on `fact_trip.pickup_time_key = dim_time.time_key`
- `dim_vendor` on `fact_trip.vendor_key = dim_vendor.vendor_key`
- `dim_payment_type` on `fact_trip.payment_type_key = dim_payment_type.payment_type_key`
- `dim_rate_code` on `fact_trip.rate_code_key = dim_rate_code.rate_code_key`
- `dim_location` as pickup location on `fact_trip.pu_location_key = dim_location.location_key`
- `dim_location` as dropoff location on `fact_trip.do_location_key = dim_location.location_key`
- `dim_passenger_count` on `fact_trip.passenger_count_key = dim_passenger_count.passenger_count_key`
- `dim_trip_flags` on `fact_trip.trip_flags_key = dim_trip_flags.trip_flags_key`
- `dim_trip_quality` on `fact_trip.trip_quality_key = dim_trip_quality.trip_quality_key`
- `dim_ingestion_batch` on `fact_trip.ingestion_batch_key = dim_ingestion_batch.ingestion_batch_key`

Calculated fields:

- `Revenue Per Trip = sum(total_amount) / sum(trip_count)`
- `Revenue Per Mile = sum(total_amount) / sum(trip_distance)`
- `Tip Percentage = sum(tip_amount) / sum(fare_amount)`
- `Average Fare = avg(fare_amount)`
- `Average Distance = avg(trip_distance)`
- `Average Duration = avg(trip_duration_minutes)`

Dashboard pages:

1. Executive Overview
   - KPI cards: total trips, total revenue, total fare, total tips, average revenue per trip.
   - Line chart: revenue and trips by date.
   - Bar chart: revenue by vendor.
   - Donut chart: trips by payment type.

2. Time Analysis
   - Heat map: trips by day of week and hour.
   - Line chart: revenue by calendar date.
   - Bar chart: average revenue by daypart.
   - Filter controls: month, day of week, weekend flag.

3. Vendor Performance
   - Table: vendor, trips, revenue, revenue per mile, tip percentage.
   - Bar chart: total revenue by vendor.
   - Scatter plot: average trip distance versus average revenue by vendor.

4. Payment and Tip Analysis
   - Bar chart: total revenue by payment type.
   - Bar chart: tip percentage by payment type.
   - Table: payment type, trips, total fare, total tips, disputes/no-charge where applicable.

5. Location and Route Analysis
   - Bar chart: top pickup zones by trips.
   - Bar chart: top dropoff zones by revenue.
   - Table: pickup zone, dropoff zone, trips, revenue, average distance.
   - Map visual after TLC zone lookup is added with borough/zone geometry.

Recommended filters:

- Date range
- Vendor
- Payment type
- Rate code
- Pickup zone
- Dropoff zone
- Passenger group
- Daypart

Portfolio talking points:

- The model separates atomic facts from aggregate reporting tables.
- Dimensions are conformed and reusable across all facts.
- Facts are partitioned by pickup date attributes to reduce Athena scan cost.
- BI dashboards use a star schema instead of joining unrelated aggregates.
