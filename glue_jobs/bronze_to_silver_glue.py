import sys

from pyspark.sql import functions as F


REQUIRED_ARGS = [
    "JOB_NAME",
    "BRONZE_INPUT_PATH",
    "SILVER_OUTPUT_PATH",
]


def add_missing_optional_columns(df):
    optional_double_columns = [
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
    ]
    existing_columns = {column.lower() for column in df.columns}

    for column_name in optional_double_columns:
        if column_name not in existing_columns:
            df = df.withColumn(column_name, F.lit(None).cast("double"))

    return df


def build_silver_df(bronze_df):
    bronze_df = add_missing_optional_columns(bronze_df)

    return (
        bronze_df
        .withColumn("vendorid", F.col("vendorid").cast("int"))
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("ratecodeid", F.col("ratecodeid").cast("int"))
        .withColumn("store_and_fwd_flag", F.col("store_and_fwd_flag").cast("string"))
        .withColumn("pulocationid", F.col("pulocationid").cast("int"))
        .withColumn("dolocationid", F.col("dolocationid").cast("int"))
        .withColumn("payment_type", F.col("payment_type").cast("int"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("extra", F.col("extra").cast("double"))
        .withColumn("mta_tax", F.col("mta_tax").cast("double"))
        .withColumn("tip_amount", F.col("tip_amount").cast("double"))
        .withColumn("tolls_amount", F.col("tolls_amount").cast("double"))
        .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("congestion_surcharge", F.col("congestion_surcharge").cast("double"))
        .withColumn("airport_fee", F.col("airport_fee").cast("double"))
        .withColumn("cbd_congestion_fee", F.col("cbd_congestion_fee").cast("double"))
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("dropoff_date", F.to_date("tpep_dropoff_datetime"))
        .withColumn("pickup_year", F.year("tpep_pickup_datetime").cast("int"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime").cast("int"))
        .withColumn("pickup_day", F.dayofmonth("tpep_pickup_datetime").cast("int"))
        .withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / F.lit(60.0),
        )
        .filter(F.col("passenger_count").isNotNull())
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("total_amount") > 0)
        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
        .dropDuplicates()
    )


def write_silver_df(silver_df, silver_output_path):
    (
        silver_df
        .repartition("pickup_year", "pickup_month")
        .write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("pickup_year", "pickup_month")
        .save(silver_output_path)
    )


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, REQUIRED_ARGS)

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

    bronze_input_path = args["BRONZE_INPUT_PATH"].rstrip("/")
    silver_output_path = args["SILVER_OUTPUT_PATH"].rstrip("/")

    bronze_df = (
        spark.read.parquet(bronze_input_path)
        .withColumn("source_file_name", F.input_file_name())
    )

    write_silver_df(build_silver_df(bronze_df), silver_output_path)

    job.commit()


if __name__ == "__main__":
    main()
