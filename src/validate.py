"""Row-count sanity checks on Bronze / Silver / Gold. Exits non-zero (job fails) on any problem."""
from __future__ import annotations

import argparse

DEFAULT_LAKE_ROOT = "gs://nyc_taxi_51124112026/medallion"


def evaluate(counts: dict[str, int], min_keep: float = 0.90) -> float:
    """Raise ValueError when the counts look wrong; return the Silver/Bronze keep ratio."""
    bronze_key = "bronze_trips_in_scope" if "bronze_trips_in_scope" in counts else "bronze_trips"
    if counts[bronze_key] == 0 or counts["gold_zones"] == 0:
        raise ValueError(f"Empty Bronze trips or zone dimension: {counts}")
    if counts["gold_trips"] != counts["silver_trips"]:
        raise ValueError(f"Gold should match Silver (left joins only): {counts}")
    keep = counts["silver_trips"] / counts[bronze_key]
    if keep < min_keep:
        raise ValueError(f"Silver kept only {keep:.1%} of Bronze rows (minimum {min_keep:.0%}): {counts}")
    return keep


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Validate medallion output")
    parser.add_argument("--lake-root", default=DEFAULT_LAKE_ROOT)
    parser.add_argument("--min-keep-ratio", type=float, default=0.90)
    parser.add_argument("--min-pickup-date", default="2019-01-01", help="Count Bronze trips picked up on/after this date")
    parser.add_argument(
        "--max-pickup-date", default=None,
        help="Count Bronze trips picked up before this date (exclusive). Default: tomorrow (UTC)",
    )
    args = parser.parse_args(argv)

    from common import LakePaths, get_spark

    spark = get_spark("nyc-taxi-validate")
    try:
        paths = LakePaths.from_root(args.lake_root)
        fs_path = spark._jvm.org.apache.hadoop.fs.Path
        conf = spark._jsc.hadoopConfiguration()

        def count_rows(table_path: str) -> int:
            marker = fs_path(f"{table_path}/_SUCCESS")
            if not marker.getFileSystem(conf).exists(marker):
                raise ValueError(f"{table_path} has no _SUCCESS marker: the Spark write did not finish")
            return spark.read.parquet(table_path).count()

        def count_bronze_in_scope(table_path: str) -> int:
            marker = fs_path(f"{table_path}/_SUCCESS")
            if not marker.getFileSystem(conf).exists(marker):
                raise ValueError(f"{table_path} has no _SUCCESS marker: the Spark write did not finish")
            from pyspark.sql import functions as F

            upper = F.lit(args.max_pickup_date).cast("timestamp") if args.max_pickup_date else F.date_add(F.current_date(), 1).cast("timestamp")
            df = spark.read.parquet(table_path)
            return df.where(
                (F.to_timestamp("tpep_pickup_datetime") >= F.lit(args.min_pickup_date).cast("timestamp"))
                & (F.to_timestamp("tpep_pickup_datetime") < upper)
            ).count()

        counts = {
            "bronze_trips": count_rows(paths.brz_trips),
            "bronze_trips_in_scope": count_bronze_in_scope(paths.brz_trips),
            "silver_trips": count_rows(paths.slv_trips),
            "gold_trips": count_rows(paths.gld_trips),
            "gold_zones": count_rows(paths.gld_zones),
        }
        print(counts)
        keep = evaluate(counts, args.min_keep_ratio)
        print(f"OK: Silver kept {keep:.1%} of in-scope Bronze rows")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
