import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


DEFAULT_DATABASE = "nyc_taxi_db"
DEFAULT_SILVER_TABLE = "silver_silver"
DEFAULT_OUTPUT_BASE = "s3://mukesh-bucket420/StarSchema"
DEFAULT_DATE_START = "2020-01-01"
DEFAULT_DATE_END = "2035-12-31"


def get_arg(name, default_value):
    flag = f"--{name}"
    if flag in sys.argv:
        return sys.argv[sys.argv.index(flag) + 1]
    return default_value


def write_parquet(df, table_name, partition_cols=None, mode="overwrite"):
    writer = df.write.mode(mode).format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(f"{output_base}/{table_name}/")


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

database_name = get_arg("DATABASE_NAME", DEFAULT_DATABASE)
silver_table_name = get_arg("SILVER_TABLE_NAME", DEFAULT_SILVER_TABLE)
output_base = get_arg("OUTPUT_BASE", DEFAULT_OUTPUT_BASE).rstrip("/")
date_start = get_arg("DATE_START", DEFAULT_DATE_START)
date_end = get_arg("DATE_END", DEFAULT_DATE_END)
location_lookup_path = get_arg("LOCATION_LOOKUP_PATH", "")
batch_id = get_arg("BATCH_ID", datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"))
job_run_id = get_arg("JOB_RUN_ID", "not_provided")
source_system = get_arg("SOURCE_SYSTEM", "NYC TLC Yellow Taxi")
source_dataset = get_arg("SOURCE_DATASET", "yellow_tripdata")
load_started_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

silver = (
    glue_context.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=silver_table_name,
    )
    .toDF()
    .withColumn("source_file_name", F.input_file_name())
)

source_count = silver.count()

prepared = (
    silver
    .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
    .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
    .withColumn("pickup_date", F.to_date("pickup_ts"))
    .withColumn("dropoff_date", F.to_date("dropoff_ts"))
    .withColumn("pickup_year", F.year("pickup_ts").cast("int"))
    .withColumn("pickup_month", F.month("pickup_ts").cast("int"))
    .withColumn("pickup_day", F.dayofmonth("pickup_ts").cast("int"))
    .withColumn("vendorid", F.coalesce(F.col("vendorid").cast("int"), F.lit(-1)))
    .withColumn("ratecodeid", F.coalesce(F.col("ratecodeid").cast("int"), F.lit(-1)))
    .withColumn("payment_type", F.coalesce(F.col("payment_type").cast("int"), F.lit(-1)))
    .withColumn("pulocationid", F.coalesce(F.col("pulocationid").cast("int"), F.lit(-1)))
    .withColumn("dolocationid", F.coalesce(F.col("dolocationid").cast("int"), F.lit(-1)))
    .withColumn("passenger_count", F.coalesce(F.col("passenger_count").cast("int"), F.lit(-1)))
    .withColumn("trip_distance", F.coalesce(F.col("trip_distance").cast("double"), F.lit(0.0)))
    .withColumn("fare_amount", F.coalesce(F.col("fare_amount").cast("double"), F.lit(0.0)))
    .withColumn("extra", F.coalesce(F.col("extra").cast("double"), F.lit(0.0)))
    .withColumn("mta_tax", F.coalesce(F.col("mta_tax").cast("double"), F.lit(0.0)))
    .withColumn("tip_amount", F.coalesce(F.col("tip_amount").cast("double"), F.lit(0.0)))
    .withColumn("tolls_amount", F.coalesce(F.col("tolls_amount").cast("double"), F.lit(0.0)))
    .withColumn("improvement_surcharge", F.coalesce(F.col("improvement_surcharge").cast("double"), F.lit(0.0)))
    .withColumn("total_amount", F.coalesce(F.col("total_amount").cast("double"), F.lit(0.0)))
    .withColumn("congestion_surcharge", F.coalesce(F.col("congestion_surcharge").cast("double"), F.lit(0.0)))
    .withColumn("airport_fee", F.coalesce(F.col("airport_fee").cast("double"), F.lit(0.0)))
    .withColumn("cbd_congestion_fee", F.coalesce(F.col("cbd_congestion_fee").cast("double"), F.lit(0.0)))
    .withColumn("trip_duration_minutes", (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / F.lit(60.0))
)

valid = (
    prepared
    .filter(F.col("pickup_ts").isNotNull())
    .filter(F.col("dropoff_ts").isNotNull())
    .filter(F.col("dropoff_ts") >= F.col("pickup_ts"))
    .filter(F.col("trip_distance") >= 0)
    .filter(F.col("total_amount") >= 0)
)

lineage_window = Window.partitionBy("source_file_name").orderBy(
    "pickup_ts",
    "dropoff_ts",
    "vendorid",
    "pulocationid",
    "dolocationid",
    "payment_type",
    "fare_amount",
    "total_amount",
    "trip_distance",
)

clean = (
    valid
    .withColumn("source_row_number", F.row_number().over(lineage_window))
    .withColumn("batch_id", F.lit(batch_id))
    .withColumn("ingestion_batch_key", F.lit(batch_id).cast("bigint"))
    .persist(StorageLevel.MEMORY_AND_DISK)
)

valid_count = clean.count()
rejected_count = source_count - valid_count

dim_date_base = spark.sql(
    f"""
    SELECT explode(sequence(to_date('{date_start}'), to_date('{date_end}'), interval 1 day)) AS calendar_date
    """
)

dim_date_actual = (
    dim_date_base
    .withColumn("date_key", F.date_format("calendar_date", "yyyyMMdd").cast("int"))
    .withColumn("calendar_year", F.year("calendar_date"))
    .withColumn("calendar_quarter", F.quarter("calendar_date"))
    .withColumn("calendar_month", F.month("calendar_date"))
    .withColumn("calendar_month_name", F.date_format("calendar_date", "MMMM"))
    .withColumn("calendar_month_short_name", F.date_format("calendar_date", "MMM"))
    .withColumn("calendar_day", F.dayofmonth("calendar_date"))
    .withColumn("day_of_year", F.dayofyear("calendar_date"))
    .withColumn("week_of_year", F.weekofyear("calendar_date"))
    .withColumn("day_of_week", F.date_format("calendar_date", "EEEE"))
    .withColumn("day_of_week_number", ((F.dayofweek("calendar_date") + F.lit(5)) % F.lit(7) + F.lit(1)).cast("int"))
    .withColumn("is_weekend", F.col("day_of_week_number").isin(6, 7))
    .withColumn("month_start_date", F.trunc("calendar_date", "MM"))
    .withColumn("month_end_date", F.last_day("calendar_date"))
    .withColumn("year_month", F.date_format("calendar_date", "yyyy-MM"))
    .select(
        "date_key",
        "calendar_date",
        "calendar_year",
        "calendar_quarter",
        "calendar_month",
        "calendar_month_name",
        "calendar_month_short_name",
        "calendar_day",
        "day_of_year",
        "week_of_year",
        "day_of_week",
        "day_of_week_number",
        "is_weekend",
        "month_start_date",
        "month_end_date",
        "year_month",
    )
)

unknown_date = dim_date_actual.limit(0).select(
    F.lit(-1).cast("int").alias("date_key"),
    F.lit(None).cast("date").alias("calendar_date"),
    F.lit(-1).cast("int").alias("calendar_year"),
    F.lit(-1).cast("int").alias("calendar_quarter"),
    F.lit(-1).cast("int").alias("calendar_month"),
    F.lit("Unknown").alias("calendar_month_name"),
    F.lit("UNK").alias("calendar_month_short_name"),
    F.lit(-1).cast("int").alias("calendar_day"),
    F.lit(-1).cast("int").alias("day_of_year"),
    F.lit(-1).cast("int").alias("week_of_year"),
    F.lit("Unknown").alias("day_of_week"),
    F.lit(-1).cast("int").alias("day_of_week_number"),
    F.lit(False).alias("is_weekend"),
    F.lit(None).cast("date").alias("month_start_date"),
    F.lit(None).cast("date").alias("month_end_date"),
    F.lit("Unknown").alias("year_month"),
)
dim_date = unknown_date.unionByName(dim_date_actual)

time_rows = [(h, m, s) for h in range(24) for m in range(60) for s in range(60)]
dim_time_actual = (
    spark.createDataFrame(time_rows, ["hour", "minute", "second"])
    .withColumn("time_key", (F.col("hour") * 10000 + F.col("minute") * 100 + F.col("second")).cast("int"))
    .withColumn(
        "daypart",
        F.when(F.col("hour").between(5, 10), F.lit("Morning"))
        .when(F.col("hour").between(11, 15), F.lit("Midday"))
        .when(F.col("hour").between(16, 20), F.lit("Evening"))
        .otherwise(F.lit("Night")),
    )
    .select("time_key", "hour", "minute", "second", "daypart")
)
unknown_time = spark.createDataFrame([(-1, -1, -1, -1, "Unknown")], dim_time_actual.schema)
dim_time = unknown_time.unionByName(dim_time_actual)

unknown_vendor = spark.createDataFrame([(-1, -1, "Unknown Vendor", True)], ["vendor_key", "vendorid", "vendor_name", "is_current"])
vendor_ref = spark.createDataFrame(
    [(1, "Creative Mobile Technologies"), (2, "VeriFone Inc"), (6, "Myle Technologies"), (7, "Helix")],
    ["vendorid", "vendor_name"],
)
dim_vendor = (
    clean.select("vendorid").distinct()
    .filter(F.col("vendorid") != -1)
    .join(vendor_ref, "vendorid", "left")
    .withColumn("vendor_key", F.col("vendorid"))
    .withColumn("vendor_name", F.coalesce("vendor_name", F.concat(F.lit("Unknown Vendor "), F.col("vendorid"))))
    .withColumn("is_current", F.lit(True))
    .select("vendor_key", "vendorid", "vendor_name", "is_current")
    .unionByName(unknown_vendor)
)

unknown_payment = spark.createDataFrame([(-1, -1, "Unknown Payment Type")], ["payment_type_key", "payment_type", "payment_type_description"])
payment_ref = spark.createDataFrame(
    [(1, "Credit card"), (2, "Cash"), (3, "No charge"), (4, "Dispute"), (5, "Unknown"), (6, "Voided trip")],
    ["payment_type", "payment_type_description"],
)
dim_payment_type = (
    clean.select("payment_type").distinct()
    .filter(F.col("payment_type") != -1)
    .join(payment_ref, "payment_type", "left")
    .withColumn("payment_type_key", F.col("payment_type"))
    .withColumn("payment_type_description", F.coalesce("payment_type_description", F.concat(F.lit("Unknown Payment Type "), F.col("payment_type"))))
    .select("payment_type_key", "payment_type", "payment_type_description")
    .unionByName(unknown_payment)
)

unknown_rate = spark.createDataFrame([(-1, -1, "Unknown Rate Code")], ["rate_code_key", "ratecodeid", "rate_code_description"])
rate_ref = spark.createDataFrame(
    [(1, "Standard rate"), (2, "JFK"), (3, "Newark"), (4, "Nassau or Westchester"), (5, "Negotiated fare"), (6, "Group ride")],
    ["ratecodeid", "rate_code_description"],
)
dim_rate_code = (
    clean.select("ratecodeid").distinct()
    .filter(F.col("ratecodeid") != -1)
    .join(rate_ref, "ratecodeid", "left")
    .withColumn("rate_code_key", F.col("ratecodeid"))
    .withColumn("rate_code_description", F.coalesce("rate_code_description", F.concat(F.lit("Unknown Rate Code "), F.col("ratecodeid"))))
    .select("rate_code_key", "ratecodeid", "rate_code_description")
    .unionByName(unknown_rate)
)

unknown_location = spark.createDataFrame(
    [(-1, -1, "Unknown", "Unknown Location", "Unknown")],
    ["location_key", "location_id", "borough", "zone", "service_zone"],
)
location_ids = (
    clean.select(F.col("pulocationid").alias("location_id"))
    .union(clean.select(F.col("dolocationid").alias("location_id")))
    .distinct()
    .filter(F.col("location_id") != -1)
)

if location_lookup_path:
    location_lookup = (
        spark.read.option("header", True).csv(location_lookup_path)
        .select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone"),
            F.col("service_zone").alias("service_zone"),
        )
    )
    dim_location_actual = (
        location_ids
        .join(location_lookup, "location_id", "left")
        .withColumn("location_key", F.col("location_id"))
        .withColumn("borough", F.coalesce("borough", F.lit("Unknown")))
        .withColumn("zone", F.coalesce("zone", F.concat(F.lit("Location "), F.col("location_id"))))
        .withColumn("service_zone", F.coalesce("service_zone", F.lit("Unknown")))
        .select("location_key", "location_id", "borough", "zone", "service_zone")
    )
else:
    dim_location_actual = (
        location_ids
    .withColumn("location_key", F.col("location_id"))
    .withColumn("borough", F.lit("Unknown"))
    .withColumn("zone", F.concat(F.lit("Location "), F.col("location_id")))
    .withColumn("service_zone", F.lit("Unknown"))
    .select("location_key", "location_id", "borough", "zone", "service_zone")
    )

dim_location = dim_location_actual.unionByName(unknown_location)

unknown_passenger_count = spark.createDataFrame([(-1, -1, "Unknown")], ["passenger_count_key", "passenger_count", "passenger_group"])
dim_passenger_count = (
    clean.select("passenger_count").distinct()
    .filter(F.col("passenger_count") != -1)
    .withColumn("passenger_count_key", F.col("passenger_count"))
    .withColumn(
        "passenger_group",
        F.when(F.col("passenger_count") == 1, F.lit("Single"))
        .when(F.col("passenger_count").between(2, 4), F.lit("Small Group"))
        .when(F.col("passenger_count") >= 5, F.lit("Large Group"))
        .otherwise(F.lit("Unknown")),
    )
    .select("passenger_count_key", "passenger_count", "passenger_group")
    .unionByName(unknown_passenger_count)
)

dim_trip_flags = spark.createDataFrame(
    [
        (-1, "Unknown", "Unknown"),
        (1, "N", "Not stored before forwarding"),
        (2, "Y", "Stored before forwarding"),
    ],
    ["trip_flags_key", "store_and_fwd_flag", "store_and_fwd_description"],
)

dim_trip_quality = spark.createDataFrame(
    [
        (-1, "UNKNOWN", "Unknown quality state", False, False, False),
        (1, "VALID_STANDARD", "Valid standard trip", False, False, False),
        (2, "ZERO_DISTANCE", "Valid trip with zero distance", True, False, False),
        (3, "LONG_DURATION", "Valid trip longer than four hours", False, True, False),
        (4, "HIGH_AMOUNT", "Valid trip with total amount over 500", False, False, True),
    ],
    ["trip_quality_key", "quality_code", "quality_description", "is_zero_distance", "is_long_duration", "is_high_amount"],
)

dim_ingestion_batch = spark.createDataFrame(
    [
        (
            int(batch_id),
            batch_id,
            args["JOB_NAME"],
            job_run_id,
            source_system,
            source_dataset,
            silver_table_name,
            output_base,
            load_started_utc,
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            source_count,
            valid_count,
            rejected_count,
        )
    ],
    [
        "ingestion_batch_key",
        "batch_id",
        "job_name",
        "job_run_id",
        "source_system",
        "source_dataset",
        "source_table",
        "target_base_path",
        "load_started_utc",
        "load_completed_utc",
        "source_record_count",
        "valid_record_count",
        "rejected_record_count",
    ],
)

quality_key_expr = (
    F.when(F.col("trip_distance") == 0, F.lit(2))
    .when(F.col("trip_duration_minutes") > 240, F.lit(3))
    .when(F.col("total_amount") > 500, F.lit(4))
    .otherwise(F.lit(1))
)

fact_trip = (
    clean
    .withColumn("pickup_date_key", F.coalesce(F.date_format("pickup_date", "yyyyMMdd").cast("int"), F.lit(-1)))
    .withColumn("dropoff_date_key", F.coalesce(F.date_format("dropoff_date", "yyyyMMdd").cast("int"), F.lit(-1)))
    .withColumn("pickup_time_key", F.coalesce(F.date_format("pickup_ts", "HHmmss").cast("int"), F.lit(-1)))
    .withColumn("dropoff_time_key", F.coalesce(F.date_format("dropoff_ts", "HHmmss").cast("int"), F.lit(-1)))
    .withColumn("vendor_key", F.col("vendorid"))
    .withColumn("payment_type_key", F.col("payment_type"))
    .withColumn("rate_code_key", F.col("ratecodeid"))
    .withColumn("pu_location_key", F.col("pulocationid"))
    .withColumn("do_location_key", F.col("dolocationid"))
    .withColumn("passenger_count_key", F.col("passenger_count"))
    .withColumn(
        "trip_flags_key",
        F.when(F.col("store_and_fwd_flag") == "N", F.lit(1))
        .when(F.col("store_and_fwd_flag") == "Y", F.lit(2))
        .otherwise(F.lit(-1)),
    )
    .withColumn("trip_quality_key", quality_key_expr)
    .withColumn(
        "trip_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.lit(batch_id),
                F.coalesce(F.col("source_file_name"), F.lit("")),
                F.coalesce(F.col("source_row_number").cast("string"), F.lit("")),
                F.coalesce(F.col("pickup_ts").cast("string"), F.lit("")),
                F.coalesce(F.col("dropoff_ts").cast("string"), F.lit("")),
                F.coalesce(F.col("vendorid").cast("string"), F.lit("")),
                F.coalesce(F.col("pulocationid").cast("string"), F.lit("")),
                F.coalesce(F.col("dolocationid").cast("string"), F.lit("")),
                F.coalesce(F.col("total_amount").cast("string"), F.lit("")),
            ),
            256,
        ),
    )
    .withColumn("trip_count", F.lit(1))
    .withColumn("tip_pct", F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount")).otherwise(F.lit(0.0)))
    .select(
        "trip_id",
        "ingestion_batch_key",
        "pickup_date_key",
        "dropoff_date_key",
        "pickup_time_key",
        "dropoff_time_key",
        "vendor_key",
        "payment_type_key",
        "rate_code_key",
        "pu_location_key",
        "do_location_key",
        "passenger_count_key",
        "trip_flags_key",
        "trip_quality_key",
        "source_file_name",
        "source_row_number",
        "pickup_ts",
        "dropoff_ts",
        "trip_count",
        "passenger_count",
        "trip_distance",
        "trip_duration_minutes",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
        "total_amount",
        "tip_pct",
        "pickup_year",
        "pickup_month",
        "pickup_day",
    )
    .persist(StorageLevel.MEMORY_AND_DISK)
)

fact_trip.count()

fact_daily_trip_summary = (
    fact_trip.groupBy("pickup_date_key", "pickup_year")
    .agg(
        F.sum("trip_count").alias("total_trips"),
        F.sum("passenger_count").alias("total_passengers"),
        F.sum("total_amount").alias("total_revenue"),
        F.sum("fare_amount").alias("total_fare"),
        F.sum("tip_amount").alias("total_tips"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
        F.avg("total_amount").alias("avg_revenue"),
    )
)

fact_vendor_daily_summary = (
    fact_trip.groupBy("pickup_date_key", "vendor_key", "pickup_year", "pickup_month")
    .agg(
        F.sum("trip_count").alias("total_trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.sum("tip_amount").alias("total_tips"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("total_amount").alias("avg_revenue"),
    )
)

fact_payment_daily_summary = (
    fact_trip.groupBy("pickup_date_key", "payment_type_key", "pickup_year", "pickup_month")
    .agg(
        F.sum("trip_count").alias("total_trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.sum("fare_amount").alias("total_fare"),
        F.sum("tip_amount").alias("total_tips"),
        F.avg("tip_pct").alias("avg_tip_pct"),
    )
)

fact_location_daily_summary = (
    fact_trip.groupBy("pickup_date_key", "pu_location_key", "do_location_key", "pickup_year", "pickup_month")
    .agg(
        F.sum("trip_count").alias("total_trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.sum("tip_amount").alias("total_tips"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
    )
)

write_parquet(dim_date, "dim_date")
write_parquet(dim_time, "dim_time")
write_parquet(dim_vendor, "dim_vendor")
write_parquet(dim_payment_type, "dim_payment_type")
write_parquet(dim_rate_code, "dim_rate_code")
write_parquet(dim_location, "dim_location")
write_parquet(dim_passenger_count, "dim_passenger_count")
write_parquet(dim_trip_flags, "dim_trip_flags")
write_parquet(dim_trip_quality, "dim_trip_quality")
write_parquet(dim_ingestion_batch, "dim_ingestion_batch", mode="append")

write_parquet(fact_trip.repartition("pickup_year", "pickup_month"), "fact_trip", ["pickup_year", "pickup_month"])
write_parquet(fact_daily_trip_summary, "fact_daily_trip_summary", ["pickup_year"])
write_parquet(fact_vendor_daily_summary, "fact_vendor_daily_summary", ["pickup_year", "pickup_month"])
write_parquet(fact_payment_daily_summary, "fact_payment_daily_summary", ["pickup_year", "pickup_month"])
write_parquet(fact_location_daily_summary, "fact_location_daily_summary", ["pickup_year", "pickup_month"])

fact_trip.unpersist()
clean.unpersist()

job.commit()
