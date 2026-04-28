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
# Default password matches machine-b-bootstrap.sh's users.xml. Override
# via CH_PASSWORD env var if you changed it.
CH_PASSWORD="${CH_PASSWORD:-demopass}"
# CH is reachable via two URL shapes:
#   CH_HTTP_URL — plain HTTP for curl probes and schema introspection
#   CH_JDBC_URL — what the opensearch-sql CLICKHOUSE connector expects;
#                 the JDBC driver rejects bare http:// URLs with
#                 "Driver ... claims to not accept jdbcUrl".
CH_HTTP_URL="http://$CH_PRIVATE_IP:8123"
# JDBC URL format expected by the plugin. Three required pieces:
#   1. `jdbc:ch://` scheme (not `jdbc:clickhouse://`). clickhouse-jdbc
#      0.6.5 registers its Driver under jdbc:ch (see HikariClickHouseClient).
#   2. `/default` path to specify the initial database.
#   3. `?compress=0` to disable HTTP compression. With compress=1 (driver
#      default), clickhouse-jdbc 0.6.5 tries to wrap the response in a
#      compression input stream even when the server sent uncompressed
#      bytes, blowing up inside ClickHouseCompressionAlgorithm.createInputStream
#      during Hikari's SELECT 1 pool validation — the error surfaces as
#      a 5s HikariCP timeout with 0 connections in the pool.
# This format matches what the integ-test suite (ClickHouseITBase) uses.
CH_JDBC_URL="jdbc:ch://$CH_PRIVATE_IP:8123/default?compress=0"
# Backward-compatible alias for existing references; still points at HTTP
# since the local curl usage predates the JDBC split.
CH_URL="$CH_HTTP_URL"

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
# Feed $CH_COLUMNS through a temp file so Python reads real file content
# instead of relying on bash variable expansion into a Python string
# literal (which breaks on embedded quotes, backslashes, or newlines
# and — with `set -u` / `pipefail` — exits silently).
echo "$CH_COLUMNS" > /tmp/ch-columns.tsv

CH_DATABASE_ENV="$CH_DATABASE" python3 > /tmp/schema.json <<'PYEOF'
import json, os

ch_database = os.environ["CH_DATABASE_ENV"]

cols = []
with open("/tmp/ch-columns.tsv") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        name, ch_type = line.split("\t")
        base = ch_type
        for prefix in ("LowCardinality(", "Nullable("):
            if base.startswith(prefix):
                base = base[len(prefix):-1]
        # CH type -> plugin expr_type. Constrained by the plugin's type
        # whitelist in ClickHouseTypeMapper. Notably:
        #   - UInt8 must map to SHORT or BOOLEAN (not BYTE); here we
        #     pick BOOLEAN because verified_purchase is semantically a
        #     bool in this demo. If the column is a true small integer,
        #     override to SHORT.
        #   - Int8 / UInt16 / UInt32 / UInt64 likewise have constrained
        #     mappings; check ClickHouseTypeMapper if you extend this.
        mapping = {
            "String":  "STRING",
            "Float32": "FLOAT",
            "Float64": "DOUBLE",
            "UInt8":   "BOOLEAN",
            "UInt16":  "INTEGER",
            "UInt32":  "LONG",
            "UInt64":  "LONG",
            "Int8":    "SHORT",
            "Int16":   "SHORT",
            "Int32":   "INTEGER",
            "Int64":   "LONG",
        }
        expr_type = mapping.get(base, "STRING")
        cols.append({"name": name, "ch_type": ch_type, "expr_type": expr_type})

schema = {
    "databases": [
        {
            "name": ch_database,
            "tables": [
                {"name": "reviews", "columns": cols}
            ]
        }
    ]
}
print(json.dumps(schema))
PYEOF

# ------------------------------------------------------------
# 3. Register datasource
# ------------------------------------------------------------
log "Registering datasource '$DS_NAME' → $CH_URL ..."

# Build the datasource creation body. All values come from env vars so the
# Python heredoc can stay single-quoted and not need shell expansion.
# clickhouse.schema must be a string (not a nested object) per the plugin's
# expected shape — hence reading /tmp/schema.json as raw text.
DS_NAME_ENV="$DS_NAME" \
CH_URL_ENV="$CH_JDBC_URL" \
CH_USER_ENV="$CH_USER" \
CH_PASSWORD_ENV="$CH_PASSWORD" \
python3 > /tmp/ds-body.json <<'PYEOF'
import json, os
body = {
    "name":      os.environ["DS_NAME_ENV"],
    "connector": "CLICKHOUSE",
    "properties": {
        "clickhouse.uri":           os.environ["CH_URL_ENV"],
        "clickhouse.auth.type":     "basic",
        "clickhouse.auth.username": os.environ["CH_USER_ENV"],
        "clickhouse.auth.password": os.environ["CH_PASSWORD_ENV"],
        "clickhouse.schema":        open("/tmp/schema.json").read(),
    },
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
