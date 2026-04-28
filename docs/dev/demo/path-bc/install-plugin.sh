#!/usr/bin/env bash
# Path B/C variant of install-plugin.sh. Differs from path-a's script in
# that it ALSO configures the datasource master key — required for Path
# B/C because they actually register a CLICKHOUSE datasource, whereas
# Path A never registers any datasource.
#
# The master key encrypts credentials at rest in the internal datasource
# metadata index. Without it, POST /_plugins/_query/_datasources returns:
#   "Master key is a required config for using create and update datasource APIs"

set -euo pipefail

WORK="${WORK:-/mnt/ebs/work}"
OS_URL="${OS_URL:-http://127.0.0.1:9200}"

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# ------------------------------------------------------------
# 1. Locate plugin zip
# ------------------------------------------------------------
PLUGIN_ZIP="${PLUGIN_ZIP:-}"
if [ -z "$PLUGIN_ZIP" ]; then
  mapfile -t candidates < <(find "$WORK" -maxdepth 1 -name 'opensearch-sql-*.zip' -print)
  if [ "${#candidates[@]}" -eq 0 ]; then
    echo "ERROR: no opensearch-sql-*.zip found in $WORK"
    echo "       Run build-and-upload-plugin.sh from your laptop first."
    exit 1
  fi
  if [ "${#candidates[@]}" -gt 1 ]; then
    echo "ERROR: multiple plugin zips found, set PLUGIN_ZIP explicitly:"
    printf '  %s\n' "${candidates[@]}"
    exit 1
  fi
  PLUGIN_ZIP="${candidates[0]}"
fi
[ -f "$PLUGIN_ZIP" ] || { echo "ERROR: $PLUGIN_ZIP not found"; exit 1; }

log "Using plugin: $PLUGIN_ZIP"

# ------------------------------------------------------------
# 2. Version compat check (identical to path-a)
# ------------------------------------------------------------
ZIP_BASENAME="$(basename "$PLUGIN_ZIP")"
PLUGIN_VERSION="${ZIP_BASENAME#opensearch-sql-}"
PLUGIN_VERSION="${PLUGIN_VERSION%.zip}"
PLUGIN_VERSION_NO_SNAPSHOT="${PLUGIN_VERSION%-SNAPSHOT}"
PLUGIN_OS_VERSION="${PLUGIN_VERSION_NO_SNAPSHOT%.*}"

OS_RUNNING_VERSION=$(curl -s "$OS_URL" | python3 -c 'import json,sys; print(json.load(sys.stdin)["version"]["number"])')
OS_RUNNING_SNAPSHOT=$(curl -s "$OS_URL" | python3 -c 'import json,sys; print(str(json.load(sys.stdin)["version"]["build_snapshot"]).lower())')

log "Plugin zip: $ZIP_BASENAME  (OS version: $PLUGIN_OS_VERSION)"
log "Running OpenSearch: $OS_RUNNING_VERSION (build_snapshot=$OS_RUNNING_SNAPSHOT)"

if [ "$PLUGIN_OS_VERSION" != "$OS_RUNNING_VERSION" ]; then
  echo "ERROR: plugin OS version $PLUGIN_OS_VERSION != running $OS_RUNNING_VERSION"
  exit 1
fi

PLUGIN_IS_SNAPSHOT="false"
[ "$PLUGIN_VERSION" != "$PLUGIN_VERSION_NO_SNAPSHOT" ] && PLUGIN_IS_SNAPSHOT="true"
if [ "$PLUGIN_IS_SNAPSHOT" != "$OS_RUNNING_SNAPSHOT" ]; then
  echo "ERROR: SNAPSHOT mismatch (plugin=$PLUGIN_IS_SNAPSHOT OS=$OS_RUNNING_SNAPSHOT)"
  exit 1
fi

# ------------------------------------------------------------
# 3. Install plugin
# ------------------------------------------------------------
log "Copying plugin into container..."
docker cp "$PLUGIN_ZIP" os:/tmp/sql-plugin.zip

log "Removing any previously installed opensearch-sql plugin..."
docker exec os /usr/share/opensearch/bin/opensearch-plugin remove --purge opensearch-sql || true

log "Installing plugin..."
docker exec os /usr/share/opensearch/bin/opensearch-plugin install --batch file:///tmp/sql-plugin.zip

# ------------------------------------------------------------
# 4. Configure datasource master key (Path B/C specific)
# ------------------------------------------------------------
# The datasource APIs encrypt connector credentials at rest using AES-256,
# which means the master key must be exactly 32 BYTES (raw, not base64).
#
# The plugin reads the string verbatim from opensearch.yml and uses its
# UTF-8 byte length as the AES key length — so 'base64(32 random bytes)'
# produces a 44-character string and the plugin fails with:
#   java.security.InvalidKeyException: Invalid AES key length: 44 bytes
#
# Generate 32 printable chars from random bytes (base64 of 24 bytes yields
# 32 chars without padding, avoiding '+'/'/'/'=' that can break YAML).
MASTER_KEY_FILE="$WORK/os-datasource-master-key.txt"
if [ ! -f "$MASTER_KEY_FILE" ]; then
  log "Generating new datasource master key (32 bytes / 32 ASCII chars)..."
  head -c 24 /dev/urandom | base64 | tr -d '=+/' | cut -c1-32 > "$MASTER_KEY_FILE"
  chmod 600 "$MASTER_KEY_FILE"
fi
MASTER_KEY=$(cat "$MASTER_KEY_FILE")
if [ "$(echo -n "$MASTER_KEY" | wc -c)" -ne 32 ]; then
  echo "ERROR: master key in $MASTER_KEY_FILE is not exactly 32 chars. Regenerate:"
  echo "  rm $MASTER_KEY_FILE && $0"
  exit 1
fi

log "Ensuring plugins.query.datasources.encryption.masterkey is set..."
# Always strip and re-add so a stale (e.g. 44-char base64) key gets replaced.
docker exec os bash -c "
  sed -i '/^plugins.query.datasources.encryption.masterkey/d' /usr/share/opensearch/config/opensearch.yml
  echo 'plugins.query.datasources.encryption.masterkey: $MASTER_KEY' >> /usr/share/opensearch/config/opensearch.yml
"

# ------------------------------------------------------------
# 5. Restart and verify
# ------------------------------------------------------------
log "Restarting OS container..."
docker restart os

log "Waiting for OS to come back up..."
for _ in $(seq 1 90); do
  curl -sf "$OS_URL" >/dev/null 2>&1 && break
  sleep 2
done

curl -sf "$OS_URL" >/dev/null 2>&1 || {
  echo "ERROR: OS did not come back up. Check: docker logs --tail 200 os"
  exit 1
}

log "Installed plugins:"
docker exec os /usr/share/opensearch/bin/opensearch-plugin list

# Sanity: PPL endpoint must respond (not necessarily with data)
log "Smoke-testing PPL endpoint..."
curl -s -X POST "$OS_URL/_plugins/_ppl" \
  -H 'Content-Type: application/json' \
  -d '{"query":"source=.kibana | head 1"}' | head -c 500
echo

# ------------------------------------------------------------
# 6. Record version
# ------------------------------------------------------------
VERSION_FILE="$WORK/plugin-version.txt"
{
  echo "# Plugin version installed for Path B/C benchmark (Machine A)"
  echo "# Installed at: $(date -Is)"
  echo "plugin_zip=$ZIP_BASENAME"
  echo "plugin_zip_sha256=$(sha256sum "$PLUGIN_ZIP" | awk '{print $1}')"
  echo "opensearch_running_version=$OS_RUNNING_VERSION"
  echo "opensearch_build_snapshot=$OS_RUNNING_SNAPSHOT"
  echo "datasource_master_key_sha256=$(echo -n "$MASTER_KEY" | sha256sum | awk '{print $1}')"
  echo
  echo "# Installed plugins:"
  docker exec os /usr/share/opensearch/bin/opensearch-plugin list | sed 's/^/# /'
} > "$VERSION_FILE"

log "Version record: $VERSION_FILE"
log "install-plugin.sh complete. Next: register-datasource.sh"
