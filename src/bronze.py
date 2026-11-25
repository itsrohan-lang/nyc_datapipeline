"""BRONZE layer: raw trips (fact) and taxi zones (dimension) plus lineage columns.

Local:  spark-submit src/bronze.py --trip-path ~/tlc/yellow --taxi-zone-path ~/tlc/reference/taxi_zone_lookup.csv --lake-root ~/tlc/medallion
"""
from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import LakePaths, base_parser, get_spark

# Every monthly file is cast to this schema, which absorbs historical INT32/INT64/double drift.
TRIP_COLUMNS = {
    "VendorID": "bigint",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "PULocationID": "bigint",
    "DOLocationID": "bigint",
    "passenger_count": "double",
    "trip_distance": "double",
    "payment_type": "bigint",
    "fare_amount": "double",
    "tip_amount": "double",
    "total_amount": "double",
}


def list_parquet_files(spark: SparkSession, root: str) -> list[str]:
    path = spark._jvm.org.apache.hadoop.fs.Path(root)
    iterator = path.getFileSystem(spark._jsc.hadoopConfiguration()).listFiles(path, True)
    files = []
    while iterator.hasNext():
        status_path = iterator.next().getPath()
        if status_path.getName().endswith(".parquet"):
            files.append(status_path.toString())
    return sorted(files)


def standardize_trips(df: DataFrame, source_file: str) -> DataFrame:
    return (
        df.select(*[F.col(name).cast(dtype).alias(name) for name, dtype in TRIP_COLUMNS.items()])
        .withColumn("source_file", F.lit(source_file))
        .withColumn("ingested_at", F.current_timestamp())
    )


def build_bronze_trips(spark: SparkSession, trip_path: str, out_path: str) -> None:
    files = list_parquet_files(spark, trip_path)
    if not files:
        raise FileNotFoundError(f"No Parquet files found under {trip_path}")
    frames = [standardize_trips(spark.read.parquet(f), f) for f in files]
    reduce(DataFrame.unionByName, frames).write.mode("overwrite").parquet(out_path)


def build_bronze_zones(spark: SparkSession, zone_path: str, out_path: str) -> None:
    (
        spark.read.option("header", True).option("inferSchema", True).csv(zone_path)
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingested_at", F.current_timestamp())
        .write.mode("overwrite").parquet(out_path)
    )


def run(spark: SparkSession, args) -> None:
    paths = LakePaths.from_root(args.lake_root)
    if args.only in ("all", "fact"):
        build_bronze_trips(spark, args.trip_path, paths.brz_trips)
    if args.only in ("all", "dim"):
        build_bronze_zones(spark, args.taxi_zone_path, paths.brz_zones)


def main(argv=None) -> None:
    args = base_parser("Bronze layer").parse_args(argv)
    spark = get_spark("nyc-taxi-bronze")
    try:
        run(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
