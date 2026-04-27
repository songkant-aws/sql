#!/usr/bin/env bash
# Stage 2 of Path A setup: install the feat/ppl-federation SQL plugin into
# the running OpenSearch container and verify it came back healthy.
#
# Run this AFTER build-and-upload-plugin.sh has scp'd the zip to
# /mnt/ebs/work/opensearch-sql-*.zip.
#
# This script is deliberately strict: if the plugin zip is missing, if its
# version doesn't match the running OS, or if OS doesn't come back up after
# restart, it fails loudly. The whole point of Path A is to produce
# comparable benchmark numbers — running on the wrong plugin silently
# poisons the comparison.

set -euo pipefail

WORK="${WORK:-/mnt/ebs/work}"
OS_URL="${OS_URL:-http://127.0.0.1:9200}"

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# ------------------------------------------------------------
# 1. Locate the plugin zip
# ------------------------------------------------------------
PLUGIN_ZIP="${PLUGIN_ZIP:-}"
if [ -z "$PLUGIN_ZIP" ]; then
  # Expect exactly one zip in the work dir.
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

if [ ! -f "$PLUGIN_ZIP" ]; then
  echo "ERROR: $PLUGIN_ZIP does not exist"
  exit 1
fi

log "Using plugin: $PLUGIN_ZIP"

# ------------------------------------------------------------
# 2. Sanity-check version compatibility
# ------------------------------------------------------------
# Plugin zip naming convention is opensearch-sql-<OS>.<PLUGIN_REV>[-SNAPSHOT].zip
# where <OS> is the three-part OS version (e.g. 3.6.0) and <PLUGIN_REV> is
# usually 0. Examples:
#   opensearch-sql-3.6.0.0.zip           (release build, installs into OS 3.6.0)
#   opensearch-sql-3.6.0.0-SNAPSHOT.zip  (SNAPSHOT build, installs into SNAPSHOT OS only)
# We extract the <OS> prefix and compare it to the running OS version.
ZIP_BASENAME="$(basename "$PLUGIN_ZIP")"
# Strip "opensearch-sql-" prefix and ".zip" suffix, then the trailing
# ".<PLUGIN_REV>" or ".<PLUGIN_REV>-SNAPSHOT". What remains is <OS>.
PLUGIN_VERSION="${ZIP_BASENAME#opensearch-sql-}"
PLUGIN_VERSION="${PLUGIN_VERSION%.zip}"
# Drop optional -SNAPSHOT suffix first.
PLUGIN_VERSION_NO_SNAPSHOT="${PLUGIN_VERSION%-SNAPSHOT}"
# Drop the trailing plugin revision (".0" — the fourth dotted field).
PLUGIN_OS_VERSION="${PLUGIN_VERSION_NO_SNAPSHOT%.*}"

OS_RUNNING_VERSION=$(curl -s "$OS_URL" | python3 -c 'import json,sys; print(json.load(sys.stdin)["version"]["number"])')
OS_RUNNING_SNAPSHOT=$(curl -s "$OS_URL" | python3 -c 'import json,sys; print(json.load(sys.stdin)["version"]["build_snapshot"])')

log "Plugin zip: $ZIP_BASENAME"
log "  => OS version extracted: $PLUGIN_OS_VERSION"
log "  => SNAPSHOT build: $([ "$PLUGIN_VERSION" != "$PLUGIN_VERSION_NO_SNAPSHOT" ] && echo yes || echo no)"
log "Running OpenSearch: $OS_RUNNING_VERSION (build_snapshot=$OS_RUNNING_SNAPSHOT)"

if [ "$PLUGIN_OS_VERSION" != "$OS_RUNNING_VERSION" ]; then
  echo "ERROR: plugin was built for OS $PLUGIN_OS_VERSION, but the running"
  echo "       container is OS $OS_RUNNING_VERSION. Plugin install will fail"
  echo "       or silently poison the benchmark."
  echo "       Rebuild with: ./gradlew :opensearch-sql-plugin:bundlePlugin \\"
  echo "                       -Dopensearch.version=${OS_RUNNING_VERSION} \\"
  echo "                       -Dbuild.snapshot=false"
  exit 1
fi

# Also sanity-check SNAPSHOT alignment: a SNAPSHOT plugin won't install
# into a release OS (plugin-descriptor.properties checks this), and a
# release plugin won't install into a SNAPSHOT OS either.
PLUGIN_IS_SNAPSHOT="false"
[ "$PLUGIN_VERSION" != "$PLUGIN_VERSION_NO_SNAPSHOT" ] && PLUGIN_IS_SNAPSHOT="true"
if [ "$PLUGIN_IS_SNAPSHOT" != "$OS_RUNNING_SNAPSHOT" ]; then
  echo "ERROR: SNAPSHOT mismatch. Plugin SNAPSHOT=$PLUGIN_IS_SNAPSHOT,"
  echo "       OpenSearch build_snapshot=$OS_RUNNING_SNAPSHOT."
  echo "       OpenSearch refuses to install a SNAPSHOT plugin into a"
  echo "       release OS (or vice versa). Rebuild with:"
  if [ "$OS_RUNNING_SNAPSHOT" = "False" ] || [ "$OS_RUNNING_SNAPSHOT" = "false" ]; then
    echo "         ./gradlew :opensearch-sql-plugin:bundlePlugin \\"
    echo "             -Dopensearch.version=${OS_RUNNING_VERSION} \\"
    echo "             -Dbuild.snapshot=false"
  else
    echo "         ./gradlew :opensearch-sql-plugin:bundlePlugin"
  fi
  exit 1
fi

# ------------------------------------------------------------
# 3. Install into the running container
# ------------------------------------------------------------
log "Copying plugin into container..."
docker cp "$PLUGIN_ZIP" os:/tmp/sql-plugin.zip

log "Removing any previously installed opensearch-sql plugin..."
docker exec os /usr/share/opensearch/bin/opensearch-plugin remove --purge opensearch-sql \
  || true  # OK if not installed

log "Installing plugin..."
docker exec os /usr/share/opensearch/bin/opensearch-plugin install --batch \
  file:///tmp/sql-plugin.zip

log "Restarting OS container..."
docker restart os

log "Waiting for OS to come back up..."
for _ in $(seq 1 90); do
  if curl -sf "$OS_URL" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! curl -sf "$OS_URL" >/dev/null 2>&1; then
  echo "ERROR: OS did not come back up after plugin install"
  echo "       Check: docker logs --tail 200 os"
  exit 1
fi

# ------------------------------------------------------------
# 4. Verify the plugin is loaded and record the version
# ------------------------------------------------------------
log "Installed plugins:"
docker exec os /usr/share/opensearch/bin/opensearch-plugin list

# Record what we installed, for the benchmark write-up.
VERSION_FILE="$WORK/plugin-version.txt"
{
  echo "# Plugin version installed for Path A benchmark"
  echo "# Installed at: $(date -Is)"
  echo "plugin_zip=$ZIP_BASENAME"
  echo "plugin_zip_sha256=$(sha256sum "$PLUGIN_ZIP" | awk '{print $1}')"
  echo "opensearch_running_version=$OS_RUNNING_VERSION"
  echo
  echo "# Installed plugins:"
  docker exec os /usr/share/opensearch/bin/opensearch-plugin list | sed 's/^/# /'
  echo
  echo "# PPL endpoint smoke-test:"
  curl -s -X POST "$OS_URL/_plugins/_ppl" \
       -H 'Content-Type: application/json' \
       -d '{"query":"source=.opensearch-observability | head 1"}' \
    | sed 's/^/# /' || true
} > "$VERSION_FILE"

log "Version record written to $VERSION_FILE"
cat "$VERSION_FILE"

log "install-plugin.sh complete. Now run ingest.sh."
