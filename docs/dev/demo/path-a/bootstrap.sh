#!/usr/bin/env bash
# EC2 user-data bootstrap for Path A (all-in-OpenSearch demo).
# Paste this into the EC2 "User data" field when launching the instance.
#
# Assumes: Amazon Linux 2023, root execution (user-data runs as root).
# Sets up: Docker, a single-node OpenSearch 2.15 with 24GB heap, no security,
# bound to 127.0.0.1:9200. Uses /mnt/ebs for all data (expects a gp3 volume
# at /dev/nvme1n1).

set -euxo pipefail

# ------------------------------------------------------------
# 1. Install Docker
# ------------------------------------------------------------
dnf install -y docker
systemctl enable docker
systemctl start docker
usermod -aG docker ec2-user

# ------------------------------------------------------------
# 2. Prepare EBS volume for OS data
# ------------------------------------------------------------
# Wait for the volume to show up (EBS attach can lag boot).
for _ in $(seq 1 30); do
  [ -b /dev/nvme1n1 ] && break
  sleep 2
done

if ! blkid /dev/nvme1n1 >/dev/null 2>&1; then
  mkfs.xfs /dev/nvme1n1
fi

mkdir -p /mnt/ebs
grep -q "/mnt/ebs" /etc/fstab || \
  echo "/dev/nvme1n1  /mnt/ebs  xfs  defaults,noatime  0  2" >> /etc/fstab
mount -a

mkdir -p /mnt/ebs/os-data /mnt/ebs/work
chown -R 1000:1000 /mnt/ebs/os-data  # OS container runs as uid 1000
chown -R ec2-user:ec2-user /mnt/ebs/work

# ------------------------------------------------------------
# 3. OS kernel settings required by OpenSearch
# ------------------------------------------------------------
cat > /etc/sysctl.d/99-opensearch.conf <<EOF
vm.max_map_count=262144
vm.swappiness=1
EOF
sysctl --system

# ------------------------------------------------------------
# 4. Start OpenSearch container
# ------------------------------------------------------------
docker pull opensearchproject/opensearch:2.15.0

docker rm -f os 2>/dev/null || true
docker run -d \
  --name os \
  --restart unless-stopped \
  -p 127.0.0.1:9200:9200 \
  -p 127.0.0.1:9600:9600 \
  -e "discovery.type=single-node" \
  -e "plugins.security.disabled=true" \
  -e "OPENSEARCH_JAVA_OPTS=-Xms24g -Xmx24g" \
  -e "bootstrap.memory_lock=true" \
  --ulimit memlock=-1:-1 \
  --ulimit nofile=65536:65536 \
  -v /mnt/ebs/os-data:/usr/share/opensearch/data \
  opensearchproject/opensearch:2.15.0

# ------------------------------------------------------------
# 5. Wait for OS to be ready (up to 2 minutes)
# ------------------------------------------------------------
for _ in $(seq 1 60); do
  if curl -sf http://127.0.0.1:9200 >/dev/null 2>&1; then
    echo "OpenSearch is up"
    break
  fi
  sleep 2
done

curl -s http://127.0.0.1:9200 | head -20 || {
  echo "OpenSearch did not start in time; check 'docker logs os'"
  exit 1
}

# ------------------------------------------------------------
# 6. Install the federation SQL plugin (your branch build)
#    Uncomment + update path when you upload the plugin zip.
# ------------------------------------------------------------
# PLUGIN_URL="s3://your-bucket/opensearch-sql-2.15.0.0-SNAPSHOT.zip"
# aws s3 cp "$PLUGIN_URL" /mnt/ebs/work/sql-plugin.zip
# docker cp /mnt/ebs/work/sql-plugin.zip os:/tmp/
# docker exec os /usr/share/opensearch/bin/opensearch-plugin install \
#   --batch file:///tmp/sql-plugin.zip
# docker restart os

echo "bootstrap.sh done. SSH in as ec2-user and run ingest.sh"
