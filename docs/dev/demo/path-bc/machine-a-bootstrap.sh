#!/usr/bin/env bash
# EC2 user-data bootstrap for Machine A: OpenSearch node for Path B/C.
#
# Paste this into the EC2 "User data" field when launching Machine A.
# Assumes Amazon Linux 2023, root execution.
#
# Machine A holds only the products index (~3.7M docs, ~4GB indexed) and
# registers Machine B (ClickHouse) as a CLICKHOUSE datasource. All analytic
# aggregations on reviews go to CH via federation.
#
# Plugin installation is a separate step (build-and-upload-plugin.sh +
# install-plugin.sh), identical to path-a.

set -euxo pipefail

# ------------------------------------------------------------
# 1. Install Docker
# ------------------------------------------------------------
dnf install -y docker
systemctl enable docker
systemctl start docker
usermod -aG docker ec2-user

# ------------------------------------------------------------
# 2. Prepare EBS volume
# ------------------------------------------------------------
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
chown -R 1000:1000 /mnt/ebs/os-data
chown -R ec2-user:ec2-user /mnt/ebs/work

# ------------------------------------------------------------
# 3. Kernel settings for OS
# ------------------------------------------------------------
cat > /etc/sysctl.d/99-opensearch.conf <<EOF
vm.max_map_count=262144
vm.swappiness=1
EOF
sysctl --system

# ------------------------------------------------------------
# 4. Start OpenSearch container (3.6.0, same version as path-a so the
#    release-built plugin zip from feat/ppl-federation can install.)
# ------------------------------------------------------------
# Heap 8 GB is enough for 3.7M products. Compared to path-a's 24 GB heap
# (needed for the 67M-row reviews aggregation workload), Machine A can be
# smaller — this is itself part of the federation value prop.
#
# OPENSEARCH_INITIAL_ADMIN_PASSWORD notes: path-a used
# DISABLE_INSTALL_DEMO_CONFIG + DISABLE_SECURITY_PLUGIN. Keep that here so
# Machine A can register a CLICKHOUSE datasource via unauthenticated REST.

docker pull opensearchproject/opensearch:3.6.0

docker rm -f os 2>/dev/null || true
docker run -d \
  --name os \
  --restart unless-stopped \
  -p 127.0.0.1:9200:9200 \
  -p 127.0.0.1:9600:9600 \
  -e "discovery.type=single-node" \
  -e "DISABLE_INSTALL_DEMO_CONFIG=true" \
  -e "DISABLE_SECURITY_PLUGIN=true" \
  -e "OPENSEARCH_JAVA_OPTS=-Xms8g -Xmx8g" \
  -e "bootstrap.memory_lock=true" \
  --ulimit memlock=-1:-1 \
  --ulimit nofile=65536:65536 \
  -v /mnt/ebs/os-data:/usr/share/opensearch/data \
  opensearchproject/opensearch:3.6.0

# ------------------------------------------------------------
# 5. Wait for readiness
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
echo " Machine A bootstrap stage 1 done."
echo ""
echo " NEXT steps:"
echo "   1. From laptop: build-and-upload-plugin.sh <machine-a-ip>"
echo "      (uploads opensearch-sql-3.6.0.0.zip and path-bc scripts)"
echo "   2. On Machine A: sudo /mnt/ebs/work/path-bc/install-plugin.sh"
echo "   3. On Machine A: CH_PRIVATE_IP=<machine-b-private-ip> \\"
echo "                    /mnt/ebs/work/path-bc/register-datasource.sh"
echo "   4. On Machine A: /mnt/ebs/work/path-bc/ingest-products-to-os.sh"
echo "   5. On Machine A: ./query-b.sh  (with IN-list rule disabled)"
echo "                    ./query-c.sh  (with IN-list rule enabled)"
echo "================================================================"
