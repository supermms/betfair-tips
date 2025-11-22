#!/bin/bash
# Cloud-init user data - EC2 Ubuntu 24.04 (sa-east-1)
# Executa automaticamente o scraper uma vez a cada boot

set -ex


# ---- Base do sistema ----
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y unzip curl gnupg python3-venv xvfb

# ---- Timezone Local ----
timedatectl set-timezone America/Sao_Paulo || true

# ---- Google Chrome ----
curl -sSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux.gpg
echo "deb [signed-by=/usr/share/keyrings/google-linux.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y google-chrome-stable

# ---- AWS CLI v2 ----
curl -SSLo /tmp/awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip
cd /tmp && unzip -q awscliv2.zip && ./aws/install && cd /
/usr/local/bin/aws --version || aws --version || true

# ---- Estrutura e ambiente Python ----
mkdir -p /opt/omqb/scripts /var/log/omqb
touch /var/log/omqb/omqb.log /var/log/omqb/omqb.err.log
chown -R ubuntu:ubuntu /opt/omqb /var/log/omqb

python3 -m venv /opt/omqb/.venv
source /opt/omqb/.venv/bin/activate
pip install --no-cache-dir --upgrade pip
pip install --no-cache-dir boto3 pandas selenium python-dateutil numpy python-dotenv
deactivate

# ---- Variáveis Globais ----
cat <<EOF > /etc/environment
AWS_DEFAULT_REGION=sa-east-1
S3_SCRIPT_BUCKET=omqb-scraper
S3_SECRETS_BUCKET=omqb-secrets
EOF

# ---- Baixar Secrets ----
source /etc/environment
aws s3 cp "s3://${S3_SECRETS_BUCKET}/omqb-secrets.env" /opt/omqb/secrets.env
sudo chown root:ubuntu /opt/omqb/secrets.env
sudo chmod 640 /opt/omqb/secrets.env

# ---- Wrapper do job
cat <<'EOF' >> /opt/omqb/run-job.sh
#!/usr/bin/env bash
set -Eeuo pipefail
exec 9>/tmp/omqb.lock
flock -n 9 || exit 0

source /etc/environment || true
LOCAL_DIR="/opt/omqb/scripts"
VENV="/opt/omqb/.venv"
LOG="/var/log/omqb/omqb.log"
S3_SCRIPTS_PREFIX="scripts"

aws s3 sync "s3://${S3_SCRIPT_BUCKET}/${S3_SCRIPTS_PREFIX}/" "${LOCAL_DIR}/" --exact-timestamps
chmod 755 "${LOCAL_DIR}/"*.py 2>/dev/null || true

source "${VENV}/bin/activate"
xvfb-run -a -s "-screen 0 1280x1024x24" \
python "${LOCAL_DIR}/omqb_scraper.py" >> "$LOG" 2>&1
EOF

chmod +x /opt/omqb/run-job.sh
chown ubuntu:ubuntu /opt/omqb/run-job.sh

# ---- Criar serviço para execução automática ----
cat <<'EOF' >> /etc/systemd/system/omqb.service
[Unit]
Description=OMQB Scraper Job
After=network.target

[Service]
User=ubuntu
ExecStart=/opt/omqb/run-job.sh
Restart=no

[Install]
WantedBy=multi-user.target
EOF

systemctl enable omqb.service
systemctl start omqb.service

echo "User Data finalizado com sucesso"