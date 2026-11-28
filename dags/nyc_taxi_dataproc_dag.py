"""NYC yellow-taxi medallion on GCP: ephemeral Dataproc cluster per run.

download_new_months -> create_cluster -> run_medallion -> validate_output -> delete_cluster -> done

* download_new_months  copies newly published TLC months into gs://BUCKET/raw/nyc_tlc/yellow/year=YYYY/month=MM/.
                       Skips the whole run (no cluster is created) when nothing is new and Gold already exists.
* create_cluster       single-node Dataproc cluster (cheap, trial-friendly). Has a hard auto-delete TTL as a safety net.
* run_medallion        pipeline_all.py (Bronze -> Silver -> Gold) as a PySpark job.
* validate_output      validate.py as a second job on the same cluster.
* delete_cluster       always runs after the cluster was created, even if a job failed.
* done                 makes the DAG run FAILED if any job failed (otherwise a successful delete_cluster would hide it).

Runs from your local Airflow using Application Default Credentials: `gcloud auth application-default login`.
Needs: pip install apache-airflow-providers-google
All settings are environment variables (defaults below).
"""
from __future__ import annotations

import os
import tempfile
import urllib.error
import urllib.request
from datetime import timedelta
from pathlib import Path

import pendulum

try:  # Airflow 3
    from airflow.sdk import DAG, task
except ImportError:  # Airflow 2
    from airflow import DAG
    from airflow.decorators import task
try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator
try:
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.exceptions import AirflowSkipException
try:
    from airflow.sdk import TriggerRule
except ImportError:
    from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
)

# ---------------------------------------------------------------- configuration
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "project-d744f265-a9df-4eb3-b93")
REGION = os.environ.get("DATAPROC_REGION", "asia-south1")  # bucket is ASIA-SOUTH1; keep Dataproc co-located
BUCKET = os.environ.get("GCS_BUCKET", "nyc_taxi_51124112026")
RAW_PREFIX = os.environ.get("GCS_RAW_PREFIX", "raw/nyc_tlc")
LAKE_PREFIX = os.environ.get("GCS_LAKE_PREFIX", "medallion")
CODE_PREFIX = os.environ.get("GCS_CODE_PREFIX", "code/latest")  # CI/CD uploads src/*.py here

MASTER_MACHINE = os.environ.get("DATAPROC_MACHINE_TYPE", "n2-highmem-8")  # 8 vCPU / 64 GB; global CPUS_ALL_REGIONS quota is 12, E2 quota is 0 here
SILVER_BATCH_DAYS = os.environ.get("SILVER_BATCH_DAYS", "90")  # fewer passes over Bronze on GCS; still small enough for one node
BOOT_DISK_GB = int(os.environ.get("DATAPROC_DISK_GB", "100"))
IMAGE_VERSION = os.environ.get("DATAPROC_IMAGE_VERSION", "2.2-debian12")  # Spark 3.5
CLUSTER_MAX_HOURS = int(os.environ.get("DATAPROC_MAX_HOURS", "6"))  # safety net if Airflow dies mid-run

BASE_URL = os.environ.get("TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net")
PUBLISH_LAG_MONTHS = 2
LOOKBACK_MONTHS = 3
MIN_KEEP_RATIO = 0.90

TRIP_URI = f"gs://{BUCKET}/{RAW_PREFIX}/yellow"
ZONE_URI = f"gs://{BUCKET}/{RAW_PREFIX}/reference/taxi_zone_lookup.csv"
LAKE_URI = f"gs://{BUCKET}/{LAKE_PREFIX}"
CODE_URI = f"gs://{BUCKET}/{CODE_PREFIX}"
MODULES = ["common.py", "bronze.py", "silver.py", "gold.py"]

CLUSTER_NAME = "nyc-taxi-{{ ts_nodash | lower }}"  # unique per run, <= 51 chars

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": MASTER_MACHINE,
        "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": BOOT_DISK_GB},
    },
    "worker_config": {"num_instances": 0},  # single node
    "software_config": {
        "image_version": IMAGE_VERSION,
        "properties": {"dataproc:dataproc.allow.zero.workers": "true"},
    },
    "lifecycle_config": {"auto_delete_ttl": {"seconds": CLUSTER_MAX_HOURS * 3600}},
}


# ---------------------------------------------------------------- helpers
def _target_month(context) -> str:
    override = (context["params"].get("month") or "").strip()
    if override:
        pendulum.from_format(override, "YYYY-MM")  # raises on a bad value
        return override
    reference = pendulum.instance(context.get("logical_date") or pendulum.now("UTC"))
    return reference.subtract(months=PUBLISH_LAG_MONTHS).format("YYYY-MM")


def _download(url: str, dest: Path) -> int | None:
    """Download url to dest. None on success, the HTTP status (403/404) when not published; other errors raise."""
    request = urllib.request.Request(url, headers={"User-Agent": "nyc-taxi-medallion-dag/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response, open(dest, "wb") as out:
            while chunk := response.read(1024 * 1024):
                out.write(chunk)
    except urllib.error.HTTPError as err:
        if err.code in (403, 404):
            return err.code
        raise
    if dest.suffix == ".parquet":
        with open(dest, "rb") as check:
            if check.read(4) != b"PAR1":  # catches HTML error pages and truncated downloads
                raise ValueError(f"{url} did not return a Parquet file")
    return None


def _job(main_file: str, args: list[str], extra_modules: bool) -> dict:
    pyspark_job = {
        "main_python_file_uri": f"{CODE_URI}/{main_file}",
        "args": args,
        "properties": {
            "spark.sql.ansi.enabled": "false",
            "spark.sql.shuffle.partitions": "64",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        },
    }
    if extra_modules:
        pyspark_job["python_file_uris"] = [f"{CODE_URI}/{m}" for m in MODULES]
    return {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": pyspark_job,
    }


# ---------------------------------------------------------------- DAG
with DAG(
    dag_id="nyc_taxi_dataproc",
    description="Download new NYC yellow-taxi months to GCS, run Bronze/Silver/Gold on an ephemeral Dataproc cluster",
    schedule="0 6 15 * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    params={"month": "", "force_rebuild": False},
    tags=["nyc-taxi", "dataproc", "medallion"],
) as dag:

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def download_new_months(**context) -> list[str]:
        from google.cloud import storage

        target = pendulum.from_format(_target_month(context), "YYYY-MM")
        wanted = [target.subtract(months=i).format("YYYY-MM") for i in range(LOOKBACK_MONTHS - 1, -1, -1)]

        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(BUCKET)
        existing = {Path(b.name).name for b in client.list_blobs(BUCKET, prefix=f"{RAW_PREFIX}/yellow/")}

        zone_blob = bucket.blob(f"{RAW_PREFIX}/reference/taxi_zone_lookup.csv")
        if not zone_blob.exists():
            with tempfile.TemporaryDirectory() as tmp:
                local = Path(tmp) / "taxi_zone_lookup.csv"
                if _download(f"{BASE_URL}/misc/taxi_zone_lookup.csv", local) is not None:
                    raise FileNotFoundError("Could not download taxi_zone_lookup.csv and it is not in GCS")
                zone_blob.upload_from_filename(str(local))

        downloaded, not_published = [], []
        for ym in wanted:
            name = f"yellow_tripdata_{ym}.parquet"
            if name in existing:
                continue
            year, month = ym.split("-")
            with tempfile.TemporaryDirectory() as tmp:
                local = Path(tmp) / name
                status = _download(f"{BASE_URL}/trip-data/{name}", local)
                if status is None:
                    bucket.blob(f"{RAW_PREFIX}/yellow/year={year}/month={month}/{name}").upload_from_filename(str(local))
                    downloaded.append(ym)
                else:
                    not_published.append(f"{ym} (HTTP {status})")
        print(f"target={target.format('YYYY-MM')} downloaded={downloaded} not_published={not_published}")

        if not existing and not downloaded:
            raise FileNotFoundError(f"No yellow Parquet files in gs://{BUCKET}/{RAW_PREFIX}/yellow/")
        gold_done = bucket.blob(f"{LAKE_PREFIX}/gold/gld_fact_trips/_SUCCESS").exists()
        if not downloaded and gold_done and not context["params"].get("force_rebuild"):
            raise AirflowSkipException(f"Nothing new to process (not published yet: {not_published or 'none'})")
        return downloaded

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
        delete_on_error=True,
        use_if_exists=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )

    run_medallion = DataprocSubmitJobOperator(
        task_id="run_medallion",
        project_id=PROJECT_ID,
        region=REGION,
        job=_job(
            "pipeline_all.py",
            ["--trip-path", TRIP_URI, "--taxi-zone-path", ZONE_URI, "--lake-root", LAKE_URI,
             "--batch-days", SILVER_BATCH_DAYS],
            extra_modules=True,
        ),
        execution_timeout=timedelta(hours=5),
    )

    validate_output = DataprocSubmitJobOperator(
        task_id="validate_output",
        project_id=PROJECT_ID,
        region=REGION,
        job=_job(
            "validate.py",
            [
                "--lake-root", LAKE_URI,
                "--min-keep-ratio", str(MIN_KEEP_RATIO),
                "--min-pickup-date", "2019-01-01",
            ],
            extra_modules=True,
        ),
        execution_timeout=timedelta(minutes=30),
    )

    @task(
        task_id="delete_cluster",
        trigger_rule=TriggerRule.ALL_DONE,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )
    def delete_cluster(cluster_name: str) -> None:  # passed as an argument so Jinja renders it
        from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
        from google.api_core.exceptions import NotFound

        hook = DataprocHook()
        try:
            op = hook.delete_cluster(project_id=PROJECT_ID, region=REGION, cluster_name=cluster_name)
            hook.wait_for_operation(timeout=20 * 60, result_retry=None, operation=op)
            print(f"Deleted cluster {cluster_name}")
        except NotFound:
            print(f"Cluster {cluster_name} not found; it may already be deleted")

    done = EmptyOperator(task_id="done")  # all_success: fails the run if run/validate failed

    cleanup = delete_cluster(CLUSTER_NAME)
    download_new_months() >> create_cluster >> run_medallion >> validate_output
    [create_cluster, run_medallion, validate_output] >> cleanup
    [validate_output, cleanup] >> done
