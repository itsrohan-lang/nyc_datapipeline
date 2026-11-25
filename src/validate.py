"""Row-count sanity checks on Bronze / Silver / Gold. Exits non-zero (job fails) on any problem."""
from __future__ import annotations

import argparse

from common import DEFAULT_LAKE_ROOT, LakePaths, get_spark


def evaluate(counts: dict[str, int], min_keep: float = 0.90) -> float:
    """Raise ValueError when the counts look wrong; return the Silver/Bronze keep ratio."""
    if counts["bronze_trips"] == 0 or counts["gold_zones"] == 0:
        raise ValueError(f"Empty Bronze trips or zone dimension: {counts}")
    if counts["gold_trips"] != counts["silver_trips"]:
        raise ValueError(f"Gold should match Silver (left joins only): {counts}")
    keep = counts["silver_trips"] / counts["bronze_trips"]
    if keep < min_keep:
        raise ValueError(f"Silver kept only {keep:.1%} of Bronze rows (minimum {min_keep:.0%}): {counts}")
    return keep


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Validate medallion output")
    parser.add_argument("--lake-root", default=DEFAULT_LAKE_ROOT)
    parser.add_argument("--min-keep-ratio", type=float, default=0.90)
    args = parser.parse_args(argv)

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

        counts = {
            "bronze_trips": count_rows(paths.brz_trips),
            "silver_trips": count_rows(paths.slv_trips),
            "gold_trips": count_rows(paths.gld_trips),
            "gold_zones": count_rows(paths.gld_zones),
        }
        print(counts)
        keep = evaluate(counts, args.min_keep_ratio)
        print(f"OK: Silver kept {keep:.1%} of Bronze rows")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
