from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F

import bronze
import gold
import silver
import validate


def _bronze_trips(spark, rows):
    cols = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
            "passenger_count", "trip_distance", "payment_type", "fare_amount", "tip_amount", "total_amount",
            "source_file"]
    return spark.createDataFrame(rows, cols).withColumn("ingested_at", F.current_timestamp())


T = datetime
GOOD = (1, T(2024, 1, 1, 10, 0), T(2024, 1, 1, 10, 30), 1, 2, 1.0, 3.5, 1, 20.0, 4.0, 26.0, "f1")


def test_bronze_standardize_casts_and_adds_lineage(spark):
    df = spark.createDataFrame(
        [(1, T(2024, 1, 1), T(2024, 1, 1, 1), 1, 2, 1, 2.0, 1, 10.0, 1.0, 12.0)],  # ints for double columns
        ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
         "passenger_count", "trip_distance", "payment_type", "fare_amount", "tip_amount", "total_amount"],
    )
    out = bronze.standardize_trips(df, "file.parquet")
    types = dict(out.dtypes)
    assert types["VendorID"] == "bigint" and types["passenger_count"] == "double"
    assert out.first()["source_file"] == "file.parquet"


def test_silver_filters_dedups_and_derives(spark):
    rows = [
        GOOD,
        GOOD,  # exact duplicate -> same trip_id
        (1, T(2024, 1, 1, 11, 0), T(2024, 1, 1, 10, 0), 1, 2, 1.0, 1.0, 1, 5.0, 0.0, 5.0, "f1"),  # dropoff < pickup
        (1, T(2024, 1, 1, 11, 0), T(2024, 1, 1, 11, 5), 1, 2, 1.0, -1.0, 1, 5.0, 0.0, 5.0, "f1"),  # negative distance
        (1, T(2001, 1, 1, 11, 0), T(2001, 1, 1, 11, 5), 1, 2, 1.0, 1.0, 1, 5.0, 0.0, 5.0, "f1"),  # too old
        (1, T(2090, 1, 1, 11, 0), T(2090, 1, 1, 11, 5), 1, 2, 1.0, 1.0, 1, 5.0, 0.0, 5.0, "f1"),  # in the future
    ]
    out = silver.transform_trips(_bronze_trips(spark, rows)).collect()
    assert len(out) == 1
    assert out[0]["trip_duration_minutes"] == pytest.approx(30.0)
    assert str(out[0]["pickup_date"]) == "2024-01-01"
    assert len(out[0]["trip_id"]) == 64


def test_silver_default_upper_bound_moves_with_time_and_can_be_overridden(spark):
    rows = [(1, T(2027, 3, 1, 8, 0), T(2027, 3, 1, 8, 5), 1, 2, 1.0, 1.0, 1, 5.0, 0.0, 5.0, "f1")]
    assert silver.transform_trips(_bronze_trips(spark, rows), max_pickup="2028-01-01").count() == 1
    assert silver.transform_trips(_bronze_trips(spark, rows), max_pickup="2026-01-01").count() == 0


def test_silver_zones_trim_and_dedup(spark):
    df = spark.createDataFrame(
        [(1, " EWR ", "Newark Airport ", "EWR"), (1, "EWR", "dup", "EWR"), (None, "x", "y", "z")],
        ["LocationID", "Borough", "Zone", "service_zone"],
    )
    out = silver.transform_zones(df).collect()
    assert len(out) == 1 and out[0]["borough"] == "EWR"


def test_gold_fact_joins_zones_keeps_all_trips_and_tip_pct(spark):
    trips = silver.transform_trips(_bronze_trips(spark, [GOOD]))
    zones = gold.transform_zones(spark.createDataFrame(
        [(1, "Manhattan", "Alpha", "Yellow Zone")], ["location_id", "borough", "zone", "service_zone"]))
    row = gold.build_fact(trips, zones).collect()[0]  # dropoff zone 2 is unknown -> left join keeps the trip
    assert row["pickup_borough"] == "Manhattan" and row["dropoff_zone"] is None
    assert row["tip_percentage"] == pytest.approx(20.0)


def test_gold_zones_fill_unknown(spark):
    df = spark.createDataFrame([(1, None, None, None)], "location_id int, borough string, zone string, service_zone string")
    assert gold.transform_zones(df).first()["borough"] == "Unknown"


def test_validate_evaluate():
    ok = {"bronze_trips": 100, "silver_trips": 95, "gold_trips": 95, "gold_zones": 265}
    assert validate.evaluate(ok) == pytest.approx(0.95)
    with pytest.raises(ValueError):
        validate.evaluate({**ok, "gold_trips": 94})
    with pytest.raises(ValueError):
        validate.evaluate({**ok, "silver_trips": 50, "gold_trips": 50})
    with pytest.raises(ValueError):
        validate.evaluate({**ok, "bronze_trips": 0})

    scoped = {
        "bronze_trips": 200,
        "bronze_trips_in_scope": 100,
        "silver_trips": 95,
        "gold_trips": 95,
        "gold_zones": 265,
    }
    assert validate.evaluate(scoped) == pytest.approx(0.95)
    with pytest.raises(ValueError):
        validate.evaluate({**scoped, "bronze_trips_in_scope": 0})


def test_end_to_end_local(spark, tmp_path):
    """Writes two tiny parquet 'months' and runs bronze -> silver -> gold -> validate on local disk."""
    import argparse

    import common

    src = tmp_path / "yellow"
    for i, month in enumerate(("2024-01", "2024-02")):
        df = spark.createDataFrame(
            [(1, T(2024, i + 1, 5, 9, 0), T(2024, i + 1, 5, 9, 20), 1, 2, 1, 2.0, 1, 10.0, 2.0, 13.0)],
            ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
             "passenger_count", "trip_distance", "payment_type", "fare_amount", "tip_amount", "total_amount"],
        )
        df.write.parquet(str(src / f"year=2024/month={month[-2:]}"))
        # Spark writes part-*.parquet files inside, which list_parquet_files picks up
    zones = tmp_path / "zones.csv"
    zones.write_text("LocationID,Borough,Zone,service_zone\n1,Manhattan,Alpha,Yellow Zone\n2,Queens,Beta,Boro Zone\n")

    args = argparse.Namespace(trip_path=str(src), taxi_zone_path=str(zones), lake_root=str(tmp_path / "lake"),
                              only="all", min_pickup_date="2019-01-01", max_pickup_date=None)
    bronze.run(spark, args)
    silver.run(spark, args)
    gold.run(spark, args)

    paths = common.LakePaths.from_root(args.lake_root)
    assert spark.read.parquet(paths.brz_trips).count() == 2
    assert spark.read.parquet(paths.gld_trips).count() == 2
    assert spark.read.parquet(paths.gld_zones).count() == 2


def test_fact_tables_get_success_marker_with_dynamic_overwrite(spark, tmp_path):
    """Dynamic partition overwrite writes no _SUCCESS; validation needs a fresh marker anyway."""
    import argparse

    import common

    previous = spark.conf.get("spark.sql.sources.partitionOverwriteMode", None)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        paths = common.LakePaths.from_root(str(tmp_path / "lake"))
        _bronze_trips(spark, [GOOD]).write.parquet(paths.brz_trips)
        spark.createDataFrame(
            [(1, "Manhattan", "Alpha", "Yellow Zone")],
            ["LocationID", "Borough", "Zone", "service_zone"],
        ).write.parquet(paths.brz_zones)

        args = argparse.Namespace(
            trip_path=str(tmp_path / "yellow"),
            taxi_zone_path=str(tmp_path / "zones.csv"),
            lake_root=str(tmp_path / "lake"),
            only="all",
            min_pickup_date="2024-01-01",
            max_pickup_date="2024-01-02",
            batch_days=1,
        )
        silver.run(spark, args)
        gold.run(spark, args)

        for table in (paths.slv_trips, paths.gld_trips):
            assert (Path(table) / "_SUCCESS").is_file(), f"missing _SUCCESS in {table}"
    finally:
        if previous is None:
            spark.conf.unset("spark.sql.sources.partitionOverwriteMode")
        else:
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", previous)
