"""Shared helpers for the Bronze / Silver / Gold jobs: lake paths, Spark session, CLI arguments."""
from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import SparkSession

DEFAULT_BUCKET = "gs://nyc_taxi_51124112026"
DEFAULT_TRIP_PATH = f"{DEFAULT_BUCKET}/raw/nyc_tlc/yellow"
DEFAULT_ZONE_PATH = f"{DEFAULT_BUCKET}/raw/nyc_tlc/reference/taxi_zone_lookup.csv"
DEFAULT_LAKE_ROOT = f"{DEFAULT_BUCKET}/medallion"


@dataclass(frozen=True)
class LakePaths:
    brz_trips: str
    brz_zones: str
    slv_trips: str
    slv_zones: str
    gld_zones: str
    gld_trips: str

    @classmethod
    def from_root(cls, lake_root: str) -> LakePaths:
        root = lake_root.rstrip("/")
        return cls(
            brz_trips=f"{root}/bronze/brz_trips",
            brz_zones=f"{root}/bronze/brz_taxi_zones",
            slv_trips=f"{root}/silver/slv_trips",
            slv_zones=f"{root}/silver/slv_taxi_zones",
            gld_zones=f"{root}/gold/gld_dim_taxi_zones",
            gld_trips=f"{root}/gold/gld_fact_trips",
        )


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def base_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--trip-path", default=DEFAULT_TRIP_PATH, help="Prefix containing Yellow Taxi Parquet files only")
    parser.add_argument("--taxi-zone-path", default=DEFAULT_ZONE_PATH, help="taxi_zone_lookup.csv location")
    parser.add_argument("--lake-root", default=DEFAULT_LAKE_ROOT, help="Root for bronze/, silver/ and gold/ output")
    parser.add_argument(
        "--only", choices=["all", "fact", "dim"], default="all",
        help="Build only the fact table (trips), only the dimension (taxi zones), or both",
    )
    parser.add_argument("--min-pickup-date", default="2019-01-01", help="Silver keeps trips picked up on/after this date")
    parser.add_argument(
        "--max-pickup-date", default=None,
        help="Silver keeps trips picked up before this date (exclusive). Default: tomorrow (UTC), so new years never get dropped",
    )
    parser.add_argument(
        "--batch-days", type=int, default=30,
        help="Silver fact writes this many pickup dates per batch to reduce single-node memory pressure",
    )
    return parser
