"""GOLD layer: business-ready zone dimension and analytics-ready trip fact.

Local:  spark-submit src/gold.py --lake-root ~/tlc/medallion
Note: the fact needs the Gold dimension, so --only fact requires an existing gld_dim_taxi_zones.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import LakePaths, base_parser, get_spark


def transform_zones(silver: DataFrame) -> DataFrame:
    return silver.select(
        "location_id",
        F.coalesce("borough", F.lit("Unknown")).alias("borough"),
        F.coalesce("zone", F.lit("Unknown")).alias("zone"),
        F.coalesce("service_zone", F.lit("Unknown")).alias("service_zone"),
    )


def build_fact(silver_trips: DataFrame, gold_zones: DataFrame) -> DataFrame:
    return (
        silver_trips.alias("t")
        .join(gold_zones.alias("pu"), F.col("t.pickup_location_id") == F.col("pu.location_id"), "left")
        .join(gold_zones.alias("do"), F.col("t.dropoff_location_id") == F.col("do.location_id"), "left")
        .select(
            "t.trip_id", "t.pickup_date", "t.pickup_at", "t.dropoff_at", "t.vendor_id",
            "t.passenger_count", "t.trip_distance_miles", "t.trip_duration_minutes",
            "t.payment_type_id", "t.fare_amount", "t.tip_amount", "t.total_amount",
            F.col("pu.borough").alias("pickup_borough"),
            F.col("pu.zone").alias("pickup_zone"),
            F.col("do.borough").alias("dropoff_borough"),
            F.col("do.zone").alias("dropoff_zone"),
            F.round(
                F.when(F.col("t.fare_amount") > 0, F.col("t.tip_amount") / F.col("t.fare_amount") * 100), 2
            ).alias("tip_percentage"),
        )
    )


def run(spark: SparkSession, args) -> None:
    paths = LakePaths.from_root(args.lake_root)
    if args.only in ("all", "dim"):
        transform_zones(spark.read.parquet(paths.slv_zones)).write.mode("overwrite").parquet(paths.gld_zones)
    if args.only in ("all", "fact"):
        build_fact(spark.read.parquet(paths.slv_trips), spark.read.parquet(paths.gld_zones)) \
            .write.option("partitionOverwriteMode", "static").mode("overwrite").partitionBy("pickup_date").parquet(paths.gld_trips)


def main(argv=None) -> None:
    args = base_parser("Gold layer").parse_args(argv)
    spark = get_spark("nyc-taxi-gold")
    try:
        run(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
