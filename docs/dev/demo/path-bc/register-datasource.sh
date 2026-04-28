#!/usr/bin/env bash
# Run ON MACHINE A: register Machine B's ClickHouse as a CLICKHOUSE
# datasource named `ch`. This is what makes `source=ch.fed.reviews` resolvable
# in PPL.
#
# Required env var:
#   CH_PRIVATE_IP    Machine B's VPC-internal IPv4 (get from `cat
#                    /mnt/ebs/work/machine-b-private-ip.txt` on Machine B,
#                    or from AWS console / `aws ec2 describe-instances`).
#
# Optional:
#   DS_NAME          datasource name (default: ch)
#   CH_DATABASE      CH database to register (default: fed)
#   CH_USER          CH user (default: default, no password)
#   CH_PASSWORD      CH password (default: empty)

set -euo pipefail

if [ -z "${CH_PRIVATE_IP:-}" ]; then
  echo "ERROR: set CH_PRIVATE_IP to Machine B's private IPv4"
  echo "       example: CH_PRIVATE_IP=10.0.1.22 $0"
  exit 1
fi

OS_URL="${OS_URL:-http://127.0.0.1:9200}"
DS_NAME="${DS_NAME:-ch}"
CH_DATABASE="${CH_DATABASE:-fed}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-}"
CH_URL="http://$CH_PRIVATE_IP:8123"

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# ------------------------------------------------------------
# 1. Pre-flight: can Machine A actually reach Machine B's 8123?
# ------------------------------------------------------------
log "Probing CH connectivity from this host to $CH_URL/ping ..."
if ! curl -sf --max-time 5 "$CH_URL/ping" | grep -q Ok; then
  echo "ERROR: cannot reach $CH_URL/ping"
  echo "  Possible causes:"
  echo "    - Machine B CH not running (check 'docker ps' on Machine B)"
  echo "    - CH listen_host is not $CH_PRIVATE_IP"
  echo "    - Security Group for Machine B doesn't allow inbound TCP 8123"
  echo "      from Machine A's Security Group (or IP)"
  echo "    - Wrong private IP passed in CH_PRIVATE_IP"
  exit 1
fi
log "CH reachable."

# ------------------------------------------------------------
# 2. Introspect CH schema (build the expected schema JSON for the
#    datasource registration). We query system.columns.
# ------------------------------------------------------------
log "Fetching CH schema for $CH_DATABASE.reviews..."
CH_COLUMNS=$(curl -sf --data-binary "
  SELECT name, type
  FROM system.columns
  WHERE database = '$CH_DATABASE' AND table = 'reviews'
  FORMAT TSV" "$CH_URL/")

if [ -z "$CH_COLUMNS" ]; then
  echo "ERROR: CH has no table $CH_DATABASE.reviews yet"
  echo "  Run ingest-reviews-to-ch.sh on Machine B first."
  exit 1
fi

# Build schema JSON. Map CH types to expr types used by the plugin.
# See ClickHouseTypeMapper for authoritative mapping; here we use the
# common subset for our table.
python3 > /tmp/schema.json <<PYEOF
import json, sys

# input from shell: TSV "name\ttype"
cols = []
for line in """$CH_COLUMNS""".strip().splitlines():
    name, ch_type = line.split("\t")
    # Strip LowCardinality/Nullable wrappers for mapping lookup
    base = ch_type
    for prefix in ("LowCardinality(", "Nullable("):
        if base.startswith(prefix):
            base = base[len(prefix):-1]

    # Expr type mapping
    mapping = {
        "String": "STRING",
        "Float32": "FLOAT",
        "Float64": "DOUBLE",
        "UInt8": "BYTE",
        "UInt16": "SHORT",
        "UInt32": "INTEGER",
        "UInt64": "LONG",
        "Int8": "BYTE",
        "Int16": "SHORT",
        "Int32": "INTEGER",
        "Int64": "LONG",
    }
    expr_type = mapping.get(base, "STRING")
    cols.append({"name": name, "ch_type": ch_type, "expr_type": expr_type})

schema = {
    "databases": [
        {
            "name": "$CH_DATABASE",
            "tables": [
                {"name": "reviews", "columns": cols}
            ]
        }
    ]
}
print(json.dumps(schema))
PYEOF

SCHEMA_JSON_ESCAPED=$(python3 -c "import json,sys; print(json.dumps(open('/tmp/schema.json').read()))")
# SCHEMA_JSON_ESCAPED is now a JSON-string-literal (with surrounding quotes).
# Strip the outer quotes so we can inline it as a string value in our
# datasource JSON.
SCHEMA_INLINE=$(python3 -c "import json,sys; print(open('/tmp/schema.json').read())")

# ------------------------------------------------------------
# 3. Register datasource
# ------------------------------------------------------------
log "Registering datasource '$DS_NAME' → $CH_URL ..."

# Build the datasource creation body. Schema JSON must be embedded as a
# string value (hence the nested json.dumps).
python3 > /tmp/ds-body.json <<PYEOF
import json
body = {
    "name": "$DS_NAME",
    "connector": "CLICKHOUSE",
    "properties": {
        "clickhouse.uri": "$CH_URL",
        "clickhouse.auth.type": "basic",
        "clickhouse.auth.username": "$CH_USER",
        "clickhouse.auth.password": "$CH_PASSWORD",
        "clickhouse.schema": open("/tmp/schema.json").read()
    }
}
print(json.dumps(body))
PYEOF

# First delete if it already exists (idempotent re-run).
curl -s -X DELETE "$OS_URL/_plugins/_query/_datasources/$DS_NAME" >/dev/null || true

RESP=$(curl -s -X POST "$OS_URL/_plugins/_query/_datasources" \
  -H 'Content-Type: application/json' \
  -d @/tmp/ds-body.json)

echo "Response: $RESP"

if echo "$RESP" | grep -q '"error"'; then
  echo "ERROR: datasource registration failed"
  exit 1
fi

# ------------------------------------------------------------
# 4. Verify: list datasources and resolve the federated table
# ------------------------------------------------------------
log "Listing datasources..."
curl -s "$OS_URL/_plugins/_query/_datasources" | python3 -m json.tool

log "Smoke-test: source=$DS_NAME.$CH_DATABASE.reviews | head 1"
curl -s -X POST "$OS_URL/_plugins/_ppl" \
  -H 'Content-Type: application/json' \
  -d "{\"query\":\"source=$DS_NAME.$CH_DATABASE.reviews | head 1\"}" \
  | python3 -m json.tool

log "register-datasource.sh complete. Next: ingest-products-to-os.sh"
