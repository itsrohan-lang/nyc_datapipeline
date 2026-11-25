#!/usr/bin/env bash
# Compute Engine startup script (runs as root on every boot, idempotent).
# Installs Airflow 2.10.5 in a venv, runs `airflow standalone` as a systemd service, and pulls
# DAGs from gs://BUCKET/dags every minute (CI/CD publishes them there).
# Settings come from instance metadata: project-id, bucket, region.
set -euo pipefail

md() { curl -fsS -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"; }
PROJECT_ID="$(md project-id)"
BUCKET="$(md bucket)"
REGION="$(md region)"

AIRFLOW_VERSION="2.10.5"
AF_HOME="/opt/airflow"

apt-get update -y
apt-get install -y python3-venv python3-dev build-essential

id airflow >/dev/null 2>&1 || useradd --system --create-home --home-dir "$AF_HOME" --shell /bin/bash airflow
mkdir -p "$AF_HOME/dags"
chown -R airflow:airflow "$AF_HOME"

if [ ! -x "$AF_HOME/venv/bin/airflow" ]; then
  PYV="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
  runuser -u airflow -- python3 -m venv "$AF_HOME/venv"
  runuser -u airflow -- "$AF_HOME/venv/bin/pip" install --upgrade pip
  runuser -u airflow -- "$AF_HOME/venv/bin/pip" install \
    "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-google apache-airflow-providers-openlineage \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYV}.txt"
fi

cat > /etc/airflow.env <<EOF
AIRFLOW_HOME=${AF_HOME}
AIRFLOW__CORE__DAGS_FOLDER=${AF_HOME}/dags
AIRFLOW__CORE__LOAD_EXAMPLES=False
GCP_PROJECT_ID=${PROJECT_ID}
GCS_BUCKET=${BUCKET}
DATAPROC_REGION=${REGION}
EOF

cat > /etc/systemd/system/airflow.service <<EOF
[Unit]
Description=Apache Airflow (standalone)
After=network-online.target
Wants=network-online.target

[Service]
User=airflow
EnvironmentFile=/etc/airflow.env
Environment="PATH=${AF_HOME}/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin"
WorkingDirectory=${AF_HOME}
ExecStart=${AF_HOME}/venv/bin/airflow standalone
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/dag-sync.service <<EOF
[Unit]
Description=Sync DAGs from GCS

[Service]
Type=oneshot
User=airflow
EnvironmentFile=/etc/airflow.env
ExecStart=/bin/bash -c 'gcloud storage rsync --recursive --delete-unmatched-destination-objects gs://\${GCS_BUCKET}/dags ${AF_HOME}/dags || true'
EOF

cat > /etc/systemd/system/dag-sync.timer <<EOF
[Unit]
Description=Sync DAGs from GCS every minute

[Timer]
OnBootSec=20s
OnUnitActiveSec=60s

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now dag-sync.timer airflow.service
echo "Airflow startup finished"
