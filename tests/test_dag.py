import os
from pathlib import Path

import pytest

pytest.importorskip("airflow")
pytest.importorskip("airflow.providers.google")

os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")


def test_dag_loads_and_wiring():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(Path(__file__).parent.parent / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.get_dag("nyc_taxi_dataproc")
    assert dag is not None
    assert set(dag.task_ids) == {
        "download_new_months", "create_cluster", "run_medallion", "validate_output", "delete_cluster", "done",
    }
    delete = dag.get_task("delete_cluster")
    assert "all_done" in str(delete.trigger_rule)
    assert {"create_cluster", "run_medallion", "validate_output"} <= delete.upstream_task_ids
    assert {"validate_output", "delete_cluster"} == dag.get_task("done").upstream_task_ids
