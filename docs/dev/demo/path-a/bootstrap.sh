#!/usr/bin/env bash
# EC2 user-data bootstrap for Path A (all-in-OpenSearch demo).
#
# Two-stage: the EC2 user-data field runs this script as root at launch. It
# installs Docker, formats the data volume, and starts OpenSearch 3.6.0
# WITHOUT the federation plugin. The second stage (install-plugin.sh) runs
# after you scp the plugin zip to the instance.
#
# Why split: the federation SQL plugin zip lives on your laptop (produced by
# `./gradlew :plugin:assemble`) and there is no common way to make user-data
# fetch it without hard-coding an S3 bucket. We stop here, let you upload,
# then run stage 2.
#
# OS version: 3.6.0 matches the `opensearch_version = 3.6.0-SNAPSHOT`
# declared in the feat/ppl-federation branch's root build.gradle. The plugin
# zip built from this branch is named opensearch-sql-3.6.0.0-SNAPSHOT.zip
# and only installs on OS 3.6.x.
#
# Assumes: Amazon Linux 2023, root execution.

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
# 4. Start OpenSearch container (3.6.0, matches feat/ppl-federation)
# ------------------------------------------------------------
docker pull opensearchproject/opensearch:3.6.0

docker rm -f os 2>/dev/null || true
# Security plugin notes for OS 3.6.0:
#   DISABLE_INSTALL_DEMO_CONFIG=true — skip demo cert/config generation,
#     which otherwise requires OPENSEARCH_INITIAL_ADMIN_PASSWORD and quits
#     the container if absent (3.6.0 behavior; different from earlier docs
#     that referenced `plugins.security.disabled`).
#   DISABLE_SECURITY_PLUGIN=true — fully unload the security plugin, so the
#     REST API accepts unauthenticated requests. Demo environment only.
docker run -d \
  --name os \
  --restart unless-stopped \
  -p 127.0.0.1:9200:9200 \
  -p 127.0.0.1:9600:9600 \
  -e "discovery.type=single-node" \
  -e "DISABLE_INSTALL_DEMO_CONFIG=true" \
  -e "DISABLE_SECURITY_PLUGIN=true" \
  -e "OPENSEARCH_JAVA_OPTS=-Xms24g -Xmx24g" \
  -e "bootstrap.memory_lock=true" \
  --ulimit memlock=-1:-1 \
  --ulimit nofile=65536:65536 \
  -v /mnt/ebs/os-data:/usr/share/opensearch/data \
  opensearchproject/opensearch:3.6.0

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

echo ""
echo "================================================================"
echo " bootstrap.sh stage 1 done."
echo ""
echo " NEXT: upload the feat/ppl-federation plugin zip and run stage 2."
echo "   From your laptop (repo root):"
echo "     ./docs/dev/demo/path-a/build-and-upload-plugin.sh <instance-ip>"
echo ""
echo "   Then on the instance:"
echo "     sudo /mnt/ebs/work/path-a/install-plugin.sh"
echo ""
echo " After plugin install: ./ingest.sh"
echo "================================================================"
