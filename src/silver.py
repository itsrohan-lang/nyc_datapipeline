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


def _run_trips_in_batches(spark: SparkSession, paths: LakePaths, args) -> None:
    import datetime as dt

    bronze = spark.read.parquet(paths.brz_trips)
    min_pickup = dt.date.fromisoformat(args.min_pickup_date)
    max_pickup = (
        dt.date.fromisoformat(args.max_pickup_date) - dt.timedelta(days=1)
        if args.max_pickup_date
        else dt.datetime.utcnow().date()
    )
    batch_size = max(1, int(getattr(args, "batch_days", 30)))

    start = min_pickup
    first = True
    while start <= max_pickup:
        end = min(start + dt.timedelta(days=batch_size - 1), max_pickup)
        # Rough prefilter on raw timestamp string/column reduces data before expensive transforms.
        raw = bronze.filter(
            (F.to_date(F.col("tpep_pickup_datetime")) >= F.lit(start.isoformat()))
            & (F.to_date(F.col("tpep_pickup_datetime")) <= F.lit(end.isoformat()))
        )
        batch_df = transform_trips(raw, args.min_pickup_date, args.max_pickup_date).filter(
            F.col("pickup_date").between(F.lit(start.isoformat()), F.lit(end.isoformat()))
        )
        writer = batch_df.write.option("maxRecordsPerFile", 250000).partitionBy("pickup_date")
        if first:
            writer.mode("overwrite").parquet(paths.slv_trips)
            first = False
        else:
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            writer.mode("overwrite").parquet(paths.slv_trips)
        start = end + dt.timedelta(days=1)


def run(spark: SparkSession, args) -> None:
    paths = LakePaths.from_root(args.lake_root)
    if args.only in ("all", "fact"):
        _run_trips_in_batches(spark, paths, args)
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
