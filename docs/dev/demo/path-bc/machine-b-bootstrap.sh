#!/usr/bin/env bash
# EC2 user-data bootstrap for Machine B: ClickHouse node for Path B/C.
#
# Paste this into the EC2 "User data" field when launching Machine B.
# Assumes Amazon Linux 2023, root execution.
#
# Network model (VPC-internal, no public exposure):
#   - CH listens on this instance's PRIVATE IPv4 only (auto-discovered via
#     EC2 metadata service). Not on 127.0.0.1, not on 0.0.0.0.
#   - Security Group should allow TCP 8123 (HTTP) only from Machine A's
#     Security Group as source (not an IP CIDR). This makes it impossible
#     to reach CH from outside the VPC regardless of the SG allow-list.
#   - SSH 22 is for you (the operator). Can be 0.0.0.0/0 with key auth.

set -euxo pipefail

# ------------------------------------------------------------
# 1. Install Docker
# ------------------------------------------------------------
dnf install -y docker
systemctl enable docker
systemctl start docker
usermod -aG docker ec2-user

# ------------------------------------------------------------
# 2. Prepare EBS data volume
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

mkdir -p /mnt/ebs/ch-data /mnt/ebs/ch-logs /mnt/ebs/work
# CH docker image runs as clickhouse user (uid 101 on clickhouse/clickhouse-server:24.x)
chown -R 101:101 /mnt/ebs/ch-data /mnt/ebs/ch-logs
chown -R ec2-user:ec2-user /mnt/ebs/work

# ------------------------------------------------------------
# 3. Discover this instance's private IPv4
# ------------------------------------------------------------
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
PRIVATE_IP=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/local-ipv4)

echo "Discovered private IP: $PRIVATE_IP"
echo "$PRIVATE_IP" > /mnt/ebs/work/machine-b-private-ip.txt

# ------------------------------------------------------------
# 4. Write CH server config overriding listen_host
# ------------------------------------------------------------
mkdir -p /mnt/ebs/ch-config.d
cat > /mnt/ebs/ch-config.d/network.xml <<EOF
<clickhouse>
  <!-- Bind only to the VPC-internal private IP. Refuses connections from
       outside the VPC at the socket level, not just via firewall. -->
  <listen_host>$PRIVATE_IP</listen_host>
  <listen_host>127.0.0.1</listen_host>
</clickhouse>
EOF

cat > /mnt/ebs/ch-config.d/query-log.xml <<'EOF'
<clickhouse>
  <query_log>
    <database>system</database>
    <table>query_log</table>
    <flush_interval_milliseconds>1000</flush_interval_milliseconds>
  </query_log>
</clickhouse>
EOF

# Profile config (resource limits + query logging) goes in config.d.
# This is additive to defaults, no override conflict.
cat > /mnt/ebs/ch-config.d/profiles.xml <<'EOF'
<clickhouse>
  <profiles>
    <default>
      <max_memory_usage>0</max_memory_usage>
      <load_balancing>random</load_balancing>
      <log_queries>1</log_queries>
    </default>
  </profiles>
  <quotas>
    <default>
      <interval>
        <duration>3600</duration>
        <queries>0</queries>
        <errors>0</errors>
        <result_rows>0</result_rows>
        <read_rows>0</read_rows>
        <execution_time>0</execution_time>
      </interval>
    </default>
  </quotas>
</clickhouse>
EOF

chown -R 101:101 /mnt/ebs/ch-config.d

# User config must go in users.d (NOT config.d). Two hard-earned lessons:
#
#   1. The clickhouse/clickhouse-server:24.x image ships
#      /etc/clickhouse-server/users.d/default-user.xml which restricts
#      `default` to localhost-only (::1 + 127.0.0.1). That file is
#      alphabetically later than any 00-*.xml we drop in, so its
#      <networks> block WINS even with `replace="replace"` semantics on
#      our file.
#
#   2. CH 24.x treats <ip>::/0</ip> as IPv6-only. A Machine A in IPv4
#      space (172.31.x / 10.x) hits AUTHENTICATION_FAILED even with the
#      right password. Must add <ip>0.0.0.0/0</ip> explicitly.
#
# Solution: use a zz- prefix so our file is loaded LAST, use
# replace="replace" on <networks> so our IP list completely overrides the
# upstream localhost-only list, and include both IPv4 and IPv6 wildcards.
mkdir -p /mnt/ebs/ch-users.d
cat > /mnt/ebs/ch-users.d/zz-demo-override.xml <<'EOF'
<?xml version="1.0"?>
<clickhouse>
  <users>
    <default>
      <password>demopass</password>
      <networks replace="replace">
        <ip>0.0.0.0/0</ip>
        <ip>::/0</ip>
      </networks>
      <profile>default</profile>
      <quota>default</quota>
      <access_management>1</access_management>
    </default>
  </users>
</clickhouse>
EOF

chown -R 101:101 /mnt/ebs/ch-users.d

# ------------------------------------------------------------
# 5. Start ClickHouse container
# ------------------------------------------------------------
docker pull clickhouse/clickhouse-server:24.8

docker rm -f ch 2>/dev/null || true
docker run -d \
  --name ch \
  --restart unless-stopped \
  --ulimit nofile=262144:262144 \
  --network host \
  -v /mnt/ebs/ch-data:/var/lib/clickhouse \
  -v /mnt/ebs/ch-logs:/var/log/clickhouse-server \
  -v /mnt/ebs/ch-config.d:/etc/clickhouse-server/config.d \
  -v /mnt/ebs/ch-users.d:/etc/clickhouse-server/users.d \
  clickhouse/clickhouse-server:24.8

# --network host so the container sees the EC2 private IP directly.
# With bridge networking, listen_host=<private-ip> inside the container
# would fail because the container doesn't have that address.

# ------------------------------------------------------------
# 6. Wait for CH to be ready
# ------------------------------------------------------------
for _ in $(seq 1 60); do
  if curl -sf "http://$PRIVATE_IP:8123/ping" | grep -q Ok; then
    echo "ClickHouse is up on $PRIVATE_IP:8123"
    break
  fi
  sleep 2
done

curl -sf "http://$PRIVATE_IP:8123/ping" || {
  echo "ClickHouse did not start in time; check 'docker logs ch'"
  exit 1
}

echo ""
echo "================================================================"
echo " Machine B bootstrap done."
echo " CH HTTP endpoint:  http://$PRIVATE_IP:8123"
echo " CH private IP:     $PRIVATE_IP (saved to /mnt/ebs/work/machine-b-private-ip.txt)"
echo ""
echo " NEXT on Machine B (same host):"
echo "   scp/rsync ingest-reviews-to-ch.sh + ch-schema.sql here, then:"
echo "     bash /mnt/ebs/work/path-bc/ingest-reviews-to-ch.sh"
echo ""
echo " NEXT on Machine A:"
echo "   Register the CH datasource pointing at http://$PRIVATE_IP:8123"
echo "   (register-datasource.sh reads the IP from an env var)"
echo "================================================================"
