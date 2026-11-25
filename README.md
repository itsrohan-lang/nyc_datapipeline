# NYC Taxi medallion on GCP (Dataproc + Airflow + GitHub Actions)

```
src/
  common.py        paths, Spark session, CLI args
  bronze.py        raw trips (fact) + taxi zones (dim)      --only all|fact|dim
  silver.py        typed / validated / de-duplicated        --only all|fact|dim
  gold.py          business-ready zones + enriched trips    --only all|fact|dim
  pipeline_all.py  bronze -> silver -> gold in ONE Spark session  (used on the cluster)
  validate.py      row-count checks (fails the job on problems)
dags/nyc_taxi_dataproc_dag.py   download -> create cluster -> pipeline -> validate -> delete cluster
scripts/                        setup_gcp.sh, run_local.sh, dataproc_once.sh
.github/workflows/ci-cd.yml     lint + tests + DAG check on every PR/push; deploy code to GCS on main
```

## 1. Test locally (layer by layer)

```bash
python -m venv .venv && source .venv/bin/activate && pip install -r requirements-dev.txt
pytest -q --ignore=tests/test_dag.py        # unit tests + a tiny end-to-end run

scripts/run_local.sh bronze                 # or: silver | gold | all | validate
scripts/run_local.sh                        # bronze silver gold validate, using ~/tlc/... as before
```

## 2. One-time GCP setup

```bash
gcloud auth application-default login       # lets local Airflow talk to GCP
scripts/setup_gcp.sh YOUR_GITHUB_USER/YOUR_REPO
source .env                                 # sets GCP_PROJECT_ID, GCS_BUCKET, DATAPROC_REGION
```

The script enables the APIs, picks the Dataproc region from your bucket's location, grants the cluster's
service account access, and creates the keyless GitHub -> GCP link (Workload Identity Federation). It prints
the two GitHub secrets to add.

## 3. First cloud run without Airflow (recommended before the DAG)

```bash
source .env && scripts/dataproc_once.sh
```
Uploads `src/`, creates a single-node cluster, runs pipeline + validate, and **always deletes the cluster** (also on failure).

## 4. Airflow DAG

```bash
pip install apache-airflow apache-airflow-providers-google
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
source .env && airflow standalone
```
Trigger `nyc_taxi_dataproc`. Params: `month` (`YYYY-MM`) and `force_rebuild` (rebuild even if nothing new).
Cluster per run: single node, `e2-highmem-4` by default (4 vCPU/32 GB, override `DATAPROC_MACHINE_TYPE`), deleted at the end, plus a 6 h auto-delete TTL as a safety net.

## 4b. Run Airflow on Compute Engine (instead of your laptop)

```bash
source .env && scripts/create_airflow_vm.sh     # creates SA, IAM roles and an e2-medium VM
gcloud storage cp dags/*.py gs://nyc_taxi_51124112026/dags/   # first time only; afterwards CI/CD does it
```
The VM installs Airflow 2.10.5, runs it as a systemd service (restarts on crash/reboot) and pulls
`gs://BUCKET/dags` every minute. It authenticates with its own service account, so no keys or
`gcloud auth` are needed on the VM. The UI is not public: open it through an SSH tunnel
(`gcloud compute ssh airflow-vm --zone ZONE -- -L 8080:localhost:8080`, then http://localhost:8080).
Stop paying for it with `gcloud compute instances stop airflow-vm --zone ZONE` (the schedule won't fire while stopped).

## 5. CI/CD

* Pull request / push: ruff, pytest (local Spark), DAG import + wiring test.
* Push to `main`: copies `src/*.py` to `gs://BUCKET/code/<commit>/` and `gs://BUCKET/code/latest/` (the DAG runs `code/latest`), and `dags/*.py` to `gs://BUCKET/dags/` (the Airflow VM syncs from there).

## Trial-account notes

* Trial quotas are small: keep the single node (4 vCPU). Raise `DATAPROC_MACHINE_TYPE` only if quota allows.
* Keep the Dataproc region equal to the bucket region to avoid cross-region transfer charges and slowness.
* Check **Console -> Dataproc -> Clusters** is empty after runs.
