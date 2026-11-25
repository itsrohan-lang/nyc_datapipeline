#!/usr/bin/env bash
# Creates the Compute Engine VM that runs Airflow. Safe to re-run.
# Usage: source .env && scripts/create_airflow_vm.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PROJECT_ID="${GCP_PROJECT_ID:-project-d744f265-a9df-4eb3-b93}"
BUCKET="${GCS_BUCKET:-nyc_taxi_51124112026}"
REGION="${DATAPROC_REGION:-us-central1}"
ZONE="${VM_ZONE:-${REGION}-a}"
VM="${VM_NAME:-airflow-vm}"
MACHINE="${VM_MACHINE_TYPE:-e2-medium}"   # 2 vCPU / 4 GB; Airflow needs ~2 GB+
SA_NAME="airflow-runner"

gcloud config set project "$PROJECT_ID" >/dev/null
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
AF_SA="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

echo "==> Service account for Airflow (${AF_SA})"
gcloud iam service-accounts describe "$AF_SA" >/dev/null 2>&1 || \
  gcloud iam service-accounts create "$SA_NAME" --display-name="Airflow runner"

# Create/delete Dataproc clusters and submit jobs
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${AF_SA}" --role="roles/dataproc.editor" --condition=None >/dev/null
# Read/write the data bucket
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member="serviceAccount:${AF_SA}" --role="roles/storage.objectAdmin" >/dev/null
# Allow clusters to run as the default compute service account
gcloud iam service-accounts add-iam-policy-binding "$COMPUTE_SA" \
  --member="serviceAccount:${AF_SA}" --role="roles/iam.serviceAccountUser" >/dev/null

echo "==> Creating VM ${VM} in ${ZONE}"
if gcloud compute instances describe "$VM" --zone="$ZONE" >/dev/null 2>&1; then
  echo "    VM already exists; re-running its startup script"
  gcloud compute instances add-metadata "$VM" --zone="$ZONE" \
    --metadata="project-id=${PROJECT_ID},bucket=${BUCKET},region=${REGION}" \
    --metadata-from-file=startup-script=vm/startup.sh
  gcloud compute ssh "$VM" --zone="$ZONE" --command="sudo google_metadata_script_runner startup" || true
else
  gcloud compute instances create "$VM" --zone="$ZONE" \
    --machine-type="$MACHINE" \
    --image-family=debian-12 --image-project=debian-cloud \
    --boot-disk-size=30GB --boot-disk-type=pd-balanced \
    --service-account="$AF_SA" --scopes=cloud-platform \
    --metadata="project-id=${PROJECT_ID},bucket=${BUCKET},region=${REGION}" \
    --metadata-from-file=startup-script=vm/startup.sh
fi

cat <<EOF

VM is booting; the Airflow install takes ~5-8 minutes.

Watch progress:
  gcloud compute ssh ${VM} --zone ${ZONE} --command "sudo journalctl -u google-startup-scripts -f"

Open the UI (not exposed to the internet; use an SSH tunnel):
  gcloud compute ssh ${VM} --zone ${ZONE} -- -L 8080:localhost:8080
  then browse http://localhost:8080   (user: admin)

Admin password:
  gcloud compute ssh ${VM} --zone ${ZONE} --command "sudo cat /opt/airflow/standalone_admin_password.txt"
EOF
