"""All-in-one entrypoint: Bronze -> Silver -> Gold in a single Spark session.

Used on Dataproc (one job, one Spark session). It reuses bronze.py / silver.py / gold.py / common.py,
which are shipped next to it as python_file_uris, so the logic lives in exactly one place.

Local:   spark-submit src/pipeline_all.py --trip-path ... --lake-root ...
Cluster: gcloud dataproc jobs submit pyspark gs://BUCKET/code/latest/pipeline_all.py \
           --py-files gs://BUCKET/code/latest/common.py,gs://BUCKET/code/latest/bronze.py,... --cluster=... --region=...
"""
from __future__ import annotations

import bronze
import gold
import silver
from common import base_parser, get_spark


def main(argv=None) -> None:
    args = base_parser("NYC taxi medallion pipeline (Bronze -> Silver -> Gold)").parse_args(argv)
    spark = get_spark("nyc-taxi-medallion")
    try:
        bronze.run(spark, args)
        silver.run(spark, args)
        gold.run(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
