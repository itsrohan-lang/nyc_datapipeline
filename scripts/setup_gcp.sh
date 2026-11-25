#!/usr/bin/env bash
# One-time GCP setup. Usage: scripts/setup_gcp.sh GITHUB_OWNER/GITHUB_REPO
# Safe to re-run. Needs `gcloud auth login` done and owner rights on the project.
set -euo pipefail

GH_REPO="${1:?Usage: $0 GITHUB_OWNER/GITHUB_REPO}"
PROJECT_ID="${GCP_PROJECT_ID:-project-d744f265-a9df-4eb3-b93}"
BUCKET="${GCS_BUCKET:-nyc_taxi_51124112026}"
DEPLOY_SA_NAME="github-deployer"
POOL="github-pool"
PROVIDER="github-provider"

gcloud config set project "$PROJECT_ID" >/dev/null
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
DEPLOY_SA="${DEPLOY_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

echo "==> Enabling APIs"
gcloud services enable dataproc.googleapis.com compute.googleapis.com storage.googleapis.com \
  iam.googleapis.com iamcredentials.googleapis.com cloudresourcemanager.googleapis.com sts.googleapis.com

echo "==> Detecting bucket location"
LOCATION="$(gcloud storage buckets describe "gs://${BUCKET}" --format='value(location)')"
case "$LOCATION" in
  US)   REGION="us-central1" ;;
  EU)   REGION="europe-west4" ;;
  ASIA) REGION="asia-south1" ;;
  *)    REGION="$(echo "$LOCATION" | tr '[:upper:]' '[:lower:]')" ;;
esac
echo "    bucket location: $LOCATION -> Dataproc region: $REGION"
cat > .env <<EOF
export GCP_PROJECT_ID=${PROJECT_ID}
export GCS_BUCKET=${BUCKET}
export DATAPROC_REGION=${REGION}
EOF
echo "    wrote .env  (run: source .env   before starting Airflow)"

echo "==> Dataproc worker role for the cluster's service account (${COMPUTE_SA})"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${COMPUTE_SA}" --role="roles/dataproc.worker" --condition=None >/dev/null
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member="serviceAccount:${COMPUTE_SA}" --role="roles/storage.objectAdmin" >/dev/null

echo "==> Service account for GitHub Actions (only writes code to the bucket)"
gcloud iam service-accounts describe "$DEPLOY_SA" >/dev/null 2>&1 || \
  gcloud iam service-accounts create "$DEPLOY_SA_NAME" --display-name="GitHub Actions deployer"
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member="serviceAccount:${DEPLOY_SA}" --role="roles/storage.objectAdmin" >/dev/null

echo "==> Workload Identity Federation (no JSON keys needed)"
gcloud iam workload-identity-pools describe "$POOL" --location=global >/dev/null 2>&1 || \
  gcloud iam workload-identity-pools create "$POOL" --location=global --display-name="GitHub pool"
gcloud iam workload-identity-pools providers describe "$PROVIDER" --location=global --workload-identity-pool="$POOL" >/dev/null 2>&1 || \
  gcloud iam workload-identity-pools providers create-oidc "$PROVIDER" \
    --location=global --workload-identity-pool="$POOL" \
    --display-name="GitHub provider" \
    --issuer-uri="https://token.actions.githubusercontent.com" \
    --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" \
    --attribute-condition="assertion.repository=='${GH_REPO}'"
gcloud iam service-accounts add-iam-policy-binding "$DEPLOY_SA" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL}/attribute.repository/${GH_REPO}" >/dev/null

cat <<EOF

Done. Add these in GitHub -> ${GH_REPO} -> Settings -> Secrets and variables -> Actions:

  Secret   GCP_WIF_PROVIDER = projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL}/providers/${PROVIDER}
  Secret   GCP_DEPLOY_SA    = ${DEPLOY_SA}
  Variable GCS_BUCKET       = ${BUCKET}   (optional, this is the default)
EOF
