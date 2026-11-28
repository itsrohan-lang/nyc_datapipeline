#!/usr/bin/env bash
# Manual smoke test on a throw-away single-node Dataproc cluster (what the DAG does, minus Airflow).
# Uploads src/ to gs://BUCKET/code/manual/, creates the cluster, runs pipeline + validate, ALWAYS deletes the cluster.
# Usage: source .env && scripts/dataproc_once.sh
#        DATAPROC_MACHINE_TYPE=e2-standard-8 STEPS="silver gold validate" scripts/dataproc_once.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PROJECT_ID="${GCP_PROJECT_ID:-project-d744f265-a9df-4eb3-b93}"
BUCKET="${GCS_BUCKET:-nyc_taxi_51124112026}"
REGION="${DATAPROC_REGION:-asia-south1}"  # bucket is ASIA-SOUTH1
MACHINE="${DATAPROC_MACHINE_TYPE:-n2-highmem-8}"
CLUSTER="nyc-taxi-manual-$(date +%H%M%S)"
CODE="gs://${BUCKET}/code/manual"
PY_FILES="${CODE}/common.py,${CODE}/bronze.py,${CODE}/silver.py,${CODE}/gold.py"

gcloud storage cp src/*.py "${CODE}/"

cleanup() {
  echo "==> Deleting cluster ${CLUSTER}"
  gcloud dataproc clusters delete "$CLUSTER" --project="$PROJECT_ID" --region="$REGION" --quiet || true
}
trap cleanup EXIT

gcloud dataproc clusters create "$CLUSTER" --project="$PROJECT_ID" --region="$REGION" \
  --single-node --master-machine-type="$MACHINE" --master-boot-disk-size=100GB \
  --image-version=2.2-debian12 --max-age=6h

# STEPS lets you re-run only part of the pipeline, e.g. STEPS="silver gold validate" (Bronze already in GCS).
STEPS="${STEPS:-pipeline_all validate}"
# Extra Spark props help large rewrites on single-node clusters (override with SPARK_PROPS).
# Avoid hardcoding driver/executor memory here: on single-node Dataproc it can exceed YARN/container limits.
SPARK_PROPS="${SPARK_PROPS:-spark.sql.ansi.enabled=false,spark.sql.shuffle.partitions=200,spark.sql.sources.partitionOverwriteMode=dynamic,spark.sql.adaptive.enabled=true,spark.sql.adaptive.coalescePartitions.enabled=true}"
SILVER_BATCH_DAYS="${SILVER_BATCH_DAYS:-90}"
for step in $STEPS; do
  echo "==> Running ${step}"
  extra_args=()
  if [[ "$step" == "silver" || "$step" == "pipeline_all" ]]; then
    extra_args+=(-- --batch-days "$SILVER_BATCH_DAYS")
  fi
  gcloud dataproc jobs submit pyspark "${CODE}/${step}.py" --project="$PROJECT_ID" --region="$REGION" \
    --cluster="$CLUSTER" --py-files="$PY_FILES" --properties="$SPARK_PROPS" "${extra_args[@]}"
done
