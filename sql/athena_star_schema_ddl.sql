CREATE DATABASE IF NOT EXISTS nyc_taxi_db;
USE nyc_taxi_db;

DROP TABLE IF EXISTS dim_date;
CREATE EXTERNAL TABLE dim_date (
    date_key INT,
    calendar_date DATE,
    calendar_year INT,
    calendar_quarter INT,
    calendar_month INT,
    calendar_month_name STRING,
    calendar_month_short_name STRING,
    calendar_day INT,
    day_of_year INT,
    week_of_year INT,
    day_of_week STRING,
    day_of_week_number INT,
    is_weekend BOOLEAN,
    month_start_date DATE,
    month_end_date DATE,
    year_month STRING
)
STORED AS PARQUET
LOCATION 's3://mukesh-bucket420/StarSchema/dim_date/';

DROP TABLE IF EXISTS dim_time;
CREATE EXTERNAL TABLE dim_time (
    time_key INT,
    hour INT,
    minute INT,
    second INT,
    daypart STRING
)
STORED AS PARQUET
LOCATION 's3://mukesh-bucket420/StarSchema/dim_time/';

DROP TABLE IF EXISTS dim_vendor;
CREATE EXTERNAL TABLE dim_vendor (
    vendor_key INT,
    vendorid INT,
    vendor_name STRING,
    is_current BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://mukesh-bucket420/StarSchema/dim_vendor/';

DROP TABLE IF EXISTS dim_payment_type;
CREATE EXTERNAL TABLE dim_payment_type (
    payment_type_key INT,
    payment_type INT,
    payment_type_description STRING
)
STORED AS PARQUET
LOCATION 's3://mukesh-bucket420/StarSchema/dim_payment_type/';

DROP TABLE IF EXISTS dim_rate_code;
CREATE EXTERNAL TABLE dim_rate_code (
    rate_code_key INT,
    ratecodeid INT,
    rate_code_description STRING
)
STORED AS PARQUET
LOCATION 's3://mukesh-bucket420/StarSchema/dim_rate_code/';

DROP TABLE IF EXISTS dim_location;
CREATE EXTERNAL TABLE dim_location (
    location_key INT,
    location_id INT,
    borough STRING,
    zone STRING,
    service_zone STRING
)
STORED AS PARQUET
LOCATION 's3://mukesh-bucket420/StarSchema/dim_location/';

DROP TABLE IF EXISTS dim_passenger_count;
CREATE EXTERNAL TABLE dim_passenger_count (
    passenger_count_key INT,
    passenger_count INT,
    passenger_group STRING
)
STORED AS PARQUET
LOCATION 's3://mukesh-bucket420/StarSchema/dim_passenger_count/';

DROP TABLE IF EXISTS dim_trip_flags;
CREATE EXTERNAL TABLE dim_trip_flags (
    trip_flags_key INT,
    store_and_fwd_flag STRING,
    store_and_fwd_description STRING
)
STORED AS PARQUET
LOCATION 's3://mukesh-bucket420/StarSchema/dim_trip_flags/';

DROP TABLE IF EXISTS dim_trip_quality;
CREATE EXTERNAL TABLE dim_trip_quality (
    trip_quality_key INT,
    quality_code STRING,
    quality_description STRING,
    is_zero_distance BOOLEAN,
    is_long_duration BOOLEAN,
    is_high_amount BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://mukesh-bucket420/StarSchema/dim_trip_quality/';

DROP TABLE IF EXISTS dim_ingestion_batch;
CREATE EXTERNAL TABLE dim_ingestion_batch (
    ingestion_batch_key BIGINT,
    batch_id STRING,
    job_name STRING,
    job_run_id STRING,
    source_system STRING,
    source_dataset STRING,
    source_table STRING,
    target_base_path STRING,
    load_started_utc STRING,
    load_completed_utc STRING,
    source_record_count BIGINT,
    valid_record_count BIGINT,
    rejected_record_count BIGINT
)
STORED AS PARQUET
LOCATION 's3://mukesh-bucket420/StarSchema/dim_ingestion_batch/';

DROP TABLE IF EXISTS fact_trip;
CREATE EXTERNAL TABLE fact_trip (
    trip_id STRING,
    ingestion_batch_key BIGINT,
    pickup_date_key INT,
    dropoff_date_key INT,
    pickup_time_key INT,
    dropoff_time_key INT,
    vendor_key INT,
    payment_type_key INT,
    rate_code_key INT,
    pu_location_key INT,
    do_location_key INT,
    passenger_count_key INT,
    trip_flags_key INT,
    trip_quality_key INT,
    source_file_name STRING,
    source_row_number INT,
    pickup_ts TIMESTAMP,
    dropoff_ts TIMESTAMP,
    trip_count INT,
    passenger_count INT,
    trip_distance DOUBLE,
    trip_duration_minutes DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    cbd_congestion_fee DOUBLE,
    total_amount DOUBLE,
    tip_pct DOUBLE,
    pickup_day INT
)
PARTITIONED BY (
    pickup_year INT,
    pickup_month INT
)
STORED AS PARQUET
LOCATION 's3://mukesh-bucket420/StarSchema/fact_trip/';

DROP TABLE IF EXISTS fact_daily_trip_summary;
CREATE EXTERNAL TABLE fact_daily_trip_summary (
    pickup_date_key INT,
    total_trips BIGINT,
    total_passengers BIGINT,
    total_revenue DOUBLE,
    total_fare DOUBLE,
    total_tips DOUBLE,
    avg_trip_distance DOUBLE,
    avg_trip_duration_minutes DOUBLE,
    avg_revenue DOUBLE
)
PARTITIONED BY (
    pickup_year INT
)
STORED AS PARQUET
LOCATION 's3://mukesh-bucket420/StarSchema/fact_daily_trip_summary/';

DROP TABLE IF EXISTS fact_vendor_daily_summary;
CREATE EXTERNAL TABLE fact_vendor_daily_summary (
    pickup_date_key INT,
    vendor_key INT,
    total_trips BIGINT,
    total_revenue DOUBLE,
    total_tips DOUBLE,
    avg_trip_distance DOUBLE,
    avg_revenue DOUBLE
)
PARTITIONED BY (
    pickup_year INT,
    pickup_month INT
)
STORED AS PARQUET
LOCATION 's3://mukesh-bucket420/StarSchema/fact_vendor_daily_summary/';

DROP TABLE IF EXISTS fact_payment_daily_summary;
CREATE EXTERNAL TABLE fact_payment_daily_summary (
    pickup_date_key INT,
    payment_type_key INT,
    total_trips BIGINT,
    total_revenue DOUBLE,
    total_fare DOUBLE,
    total_tips DOUBLE,
    avg_tip_pct DOUBLE
)
PARTITIONED BY (
    pickup_year INT,
    pickup_month INT
)
STORED AS PARQUET
LOCATION 's3://mukesh-bucket420/StarSchema/fact_payment_daily_summary/';

DROP TABLE IF EXISTS fact_location_daily_summary;
CREATE EXTERNAL TABLE fact_location_daily_summary (
    pickup_date_key INT,
    pu_location_key INT,
    do_location_key INT,
    total_trips BIGINT,
    total_revenue DOUBLE,
    total_tips DOUBLE,
    avg_trip_distance DOUBLE,
    avg_trip_duration_minutes DOUBLE
)
PARTITIONED BY (
    pickup_year INT,
    pickup_month INT
)
STORED AS PARQUET
LOCATION 's3://mukesh-bucket420/StarSchema/fact_location_daily_summary/';

MSCK REPAIR TABLE fact_trip;
MSCK REPAIR TABLE fact_daily_trip_summary;
MSCK REPAIR TABLE fact_vendor_daily_summary;
MSCK REPAIR TABLE fact_payment_daily_summary;
MSCK REPAIR TABLE fact_location_daily_summary;
