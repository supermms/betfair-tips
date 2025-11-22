#!/bin/bash
# Cloud-init user data — EC2 Ubuntu 24.04 (sa-east-1)
# Executa automaticamente o scraper uma vez a cada boot.

set -ex

SEU_BUCKET="betfair-scraper"
REGIAO="sa-east-1"

# ---- Base do sistema ----
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y unzip curl gnupg python3-venv xvfb

# Timezone local (opcional)
timedatectl set-timezone America/Sao_Paulo || true

# ---- Google Chrome ----
curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux.gpg
echo "deb [signed-by=/usr/share/keyrings/google-linux.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y google-chrome-stable

# ---- AWS CLI v2 ----
curl -sSLo /tmp/awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip
cd /tmp && unzip -q awscliv2.zip && ./aws/install && cd /
/usr/local/bin/aws --version || aws --version || true

# ---- Estrutura e ambiente Python ----
mkdir -p /opt/betfair/scripts /var/log/betfair
chown -R ubuntu:ubuntu /opt/betfair /var/log/betfair

python3 -m venv /opt/betfair/.venv
source /opt/betfair/.venv/bin/activate
pip install --no-cache-dir --upgrade pip
pip install --no-cache-dir boto3 pandas selenium python-dateutil

# ---- Variáveis globais ----
cat <<EOF >> /etc/environment
S3_BUCKET=$SEU_BUCKET
S3_CONFIG_KEY=config/links.json
S3_OUTPUT_PREFIX=outputs
AWS_DEFAULT_REGION=$REGIAO
LOOKAHEAD_DAYS=3
MIN_LIQUIDEZ=50
JUICE_MAX=0.20
EOF

# ---- Wrapper do job ----
cat <<'EOF' > /opt/betfair/run-job.sh
#!/usr/bin/env bash
set -Eeuo pipefail
exec 9>/tmp/betfair.lock
flock -n 9 || exit 0

source /etc/environment || true
S3_SCRIPTS_PREFIX="scripts"
LOCAL_DIR="/opt/betfair/scripts"
VENV="/opt/betfair/.venv"
LOG="/var/log/betfair/run.log"

aws s3 sync "s3://${S3_BUCKET}/${S3_SCRIPTS_PREFIX}/" "${LOCAL_DIR}/" --exact-timestamps
chmod 755 "${LOCAL_DIR}/"*.py 2>/dev/null || true

source "${VENV}/bin/activate"
xvfb-run -a -s "-screen 0 1280x1024x24" \
  python "${LOCAL_DIR}/run_scraper_ec2.py" >> "$LOG" 2>&1
EOF

chmod +x /opt/betfair/run-job.sh
chown ubuntu:ubuntu /opt/betfair/run-job.sh

# ---- Execução automática no boot ----
cat <<'EOF' > /etc/systemd/system/betfair-job.service
[Unit]
Description=Betfair Scraper Job
After=network.target

[Service]
User=ubuntu
ExecStart=/opt/betfair/run-job.sh
Restart=no

[Install]
WantedBy=multi-user.target
EOF

systemctl enable betfair-job.service
systemctl start betfair-job.service

echo "User Data finalizado com sucesso (job executa automaticamente no boot)."