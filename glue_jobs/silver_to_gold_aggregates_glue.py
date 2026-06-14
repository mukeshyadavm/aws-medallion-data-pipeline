import sys

from pyspark.sql import functions as F


REQUIRED_ARGS = [
    "JOB_NAME",
    "SILVER_INPUT_PATH",
    "GOLD_OUTPUT_PATH",
]


def prepare_gold_source(silver_df):
    return (
        silver_df
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_year", F.year("tpep_pickup_datetime").cast("int"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime").cast("int"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("vendorid", F.col("vendorid").cast("int"))
        .withColumn("payment_type", F.col("payment_type").cast("int"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("tip_amount", F.col("tip_amount").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
    )


def build_gold_aggregates(silver_df):
    source_df = prepare_gold_source(silver_df)

    gold_daily_revenue = (
        source_df
        .groupBy("pickup_date", "pickup_year", "pickup_month")
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("passenger_count").alias("total_passengers"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.sum("tip_amount"), 2).alias("total_tips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
            F.round(F.avg("total_amount"), 2).alias("avg_revenue_per_trip"),
        )
    )

    gold_vendor_revenue = (
        source_df
        .groupBy("vendorid", "pickup_year", "pickup_month")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.sum("tip_amount"), 2).alias("total_tips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
            F.round(F.avg("total_amount"), 2).alias("avg_revenue_per_trip"),
        )
    )

    gold_payment_type_analysis = (
        source_df
        .groupBy("payment_type", "pickup_year", "pickup_month")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.sum("tip_amount"), 2).alias("total_tips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(
                F.when(F.sum("fare_amount") != 0, F.sum("tip_amount") / F.sum("fare_amount"))
                .otherwise(F.lit(0.0)),
                4,
            ).alias("tip_pct"),
        )
    )

    gold_passenger_analysis = (
        source_df
        .groupBy("passenger_count", "pickup_year", "pickup_month")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.sum("tip_amount"), 2).alias("total_tips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
            F.round(F.avg("total_amount"), 2).alias("avg_revenue_per_trip"),
        )
    )

    return {
        "daily_revenue": gold_daily_revenue,
        "vendor_revenue": gold_vendor_revenue,
        "payment_type_analysis": gold_payment_type_analysis,
        "passenger_analysis": gold_passenger_analysis,
    }


def write_gold_table(df, gold_base_path, table_name, partition_cols=None):
    writer = df.write.mode("overwrite").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(f"{gold_base_path}/{table_name}/")


def write_gold_aggregates(aggregates, gold_output_path):
    write_gold_table(aggregates["daily_revenue"], gold_output_path, "daily_revenue", ["pickup_year", "pickup_month"])
    write_gold_table(aggregates["vendor_revenue"], gold_output_path, "vendor_revenue", ["pickup_year", "pickup_month"])
    write_gold_table(
        aggregates["payment_type_analysis"],
        gold_output_path,
        "payment_type_analysis",
        ["pickup_year", "pickup_month"],
    )
    write_gold_table(aggregates["passenger_analysis"], gold_output_path, "passenger_analysis", ["pickup_year", "pickup_month"])


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

    silver_input_path = args["SILVER_INPUT_PATH"].rstrip("/")
    gold_output_path = args["GOLD_OUTPUT_PATH"].rstrip("/")

    silver_df = spark.read.parquet(silver_input_path)
    write_gold_aggregates(build_gold_aggregates(silver_df), gold_output_path)

    job.commit()


if __name__ == "__main__":
    main()
