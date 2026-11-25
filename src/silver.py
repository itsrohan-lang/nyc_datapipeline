"""SILVER layer: typed, validated, de-duplicated trips (fact) and taxi zones (dimension).

Local:  spark-submit src/silver.py --lake-root ~/tlc/medallion
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import LakePaths, base_parser, get_spark


def transform_trips(bronze: DataFrame, min_pickup: str = "2019-01-01", max_pickup: str | None = None) -> DataFrame:
    upper = F.lit(max_pickup).cast("timestamp") if max_pickup else F.date_add(F.current_date(), 1).cast("timestamp")
    return (
        bronze.select(
            F.col("VendorID").cast("int").alias("vendor_id"),
            F.to_timestamp("tpep_pickup_datetime").alias("pickup_at"),
            F.to_timestamp("tpep_dropoff_datetime").alias("dropoff_at"),
            F.col("PULocationID").cast("int").alias("pickup_location_id"),
            F.col("DOLocationID").cast("int").alias("dropoff_location_id"),
            F.col("passenger_count").cast("int").alias("passenger_count"),
            F.col("trip_distance").cast("double").alias("trip_distance_miles"),
            F.col("payment_type").cast("int").alias("payment_type_id"),
            F.col("fare_amount").cast("double").alias("fare_amount"),
            F.col("tip_amount").cast("double").alias("tip_amount"),
            F.col("total_amount").cast("double").alias("total_amount"),
            "source_file",
            "ingested_at",
        )
        .filter(F.col("pickup_at").isNotNull() & F.col("dropoff_at").isNotNull())
        .filter(F.col("dropoff_at") >= F.col("pickup_at"))
        .filter(F.col("trip_distance_miles") >= 0)
        .filter((F.col("pickup_at") >= F.lit(min_pickup).cast("timestamp")) & (F.col("pickup_at") < upper))
        .withColumn("pickup_date", F.to_date("pickup_at"))
        .withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("dropoff_at") - F.unix_timestamp("pickup_at")) / F.lit(60.0),
        )
        .withColumn(
            "trip_id",
            F.sha2(
                F.concat_ws("|", "pickup_at", "dropoff_at", "pickup_location_id", "dropoff_location_id", "vendor_id", "source_file"),
                256,
            ),
        )
        .dropDuplicates(["trip_id"])
    )


def transform_zones(bronze: DataFrame) -> DataFrame:
    return (
        bronze.select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.trim("Borough").alias("borough"),
            F.trim("Zone").alias("zone"),
            F.trim("service_zone").alias("service_zone"),
        )
        .filter(F.col("location_id").isNotNull())
        .dropDuplicates(["location_id"])
    )


def run(spark: SparkSession, args) -> None:
    paths = LakePaths.from_root(args.lake_root)
    if args.only in ("all", "fact"):
        (
            transform_trips(spark.read.parquet(paths.brz_trips), args.min_pickup_date, args.max_pickup_date)
            .write.mode("overwrite")
            .option("maxRecordsPerFile", 500000)
            .partitionBy("pickup_date")
            .parquet(paths.slv_trips)
        )
    if args.only in ("all", "dim"):
        transform_zones(spark.read.parquet(paths.brz_zones)).write.mode("overwrite").parquet(paths.slv_zones)


def main(argv=None) -> None:
    args = base_parser("Silver layer").parse_args(argv)
    spark = get_spark("nyc-taxi-silver")
    try:
        run(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
