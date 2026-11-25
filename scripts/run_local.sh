#!/usr/bin/env bash
# Run the layers locally, one after another (same layout as before: ~/tlc/...).
# Usage: scripts/run_local.sh [bronze|silver|gold|all|validate]...   (default: bronze silver gold validate)
set -euo pipefail
cd "$(dirname "$0")/.."

DATA_ROOT="${TLC_DATA_ROOT:-$HOME/tlc}"
SPARK_TMP="${TLC_SPARK_TMP:-$HOME/spark-tmp}"
mkdir -p "$SPARK_TMP"

COMMON=(--master "${TLC_SPARK_MASTER:-local[8]}" --driver-memory "${TLC_DRIVER_MEMORY:-8g}"
        --conf "spark.local.dir=$SPARK_TMP" --conf spark.sql.ansi.enabled=false)
LAYER_ARGS=(--trip-path "$DATA_ROOT/yellow" --taxi-zone-path "$DATA_ROOT/reference/taxi_zone_lookup.csv"
            --lake-root "$DATA_ROOT/medallion")

steps=("$@")
[ ${#steps[@]} -eq 0 ] && steps=(bronze silver gold validate)

for step in "${steps[@]}"; do
  echo "=== $step ==="
  case "$step" in
    bronze|silver|gold) spark-submit "${COMMON[@]}" "src/$step.py" "${LAYER_ARGS[@]}" ;;
    all)                spark-submit "${COMMON[@]}" src/pipeline_all.py "${LAYER_ARGS[@]}" ;;
    validate)           spark-submit "${COMMON[@]}" src/validate.py --lake-root "$DATA_ROOT/medallion" ;;
    *) echo "unknown step: $step" >&2; exit 1 ;;
  esac
done
