#!/usr/bin/env bash
# Build the feat/ppl-federation SQL plugin on your laptop and scp it to the
# Path A EC2 instance. Run this from the repo root.
#
# Usage:
#   ./docs/dev/demo/path-a/build-and-upload-plugin.sh <instance-ip-or-alias>
#
# If you configured ~/.ssh/config with "Host path-a-demo", use:
#   ./docs/dev/demo/path-a/build-and-upload-plugin.sh path-a-demo

set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <ec2-host>"
  echo "  where <ec2-host> is an IP or an SSH alias from ~/.ssh/config"
  exit 1
fi

HOST="$1"
SSH_USER="${SSH_USER:-ec2-user}"
REMOTE_DIR="${REMOTE_DIR:-/mnt/ebs/work}"

# Figure out repo root (this script lives at docs/dev/demo/path-a/).
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
cd "$REPO_ROOT"

# Sanity: we're on the feat/ppl-federation branch (warn if not).
CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [ "$CURRENT_BRANCH" != "feat/ppl-federation" ]; then
  echo "WARNING: current branch is '$CURRENT_BRANCH', not 'feat/ppl-federation'."
  echo "         Path A benchmark must run against the federation branch."
  read -r -p "Continue anyway? [y/N] " ans
  [[ "$ans" =~ ^[Yy]$ ]] || exit 1
fi

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# ------------------------------------------------------------
# 1. Build the plugin zip
# ------------------------------------------------------------
log "Building plugin (this may take a few minutes)..."
./gradlew :plugin:assemble

# ------------------------------------------------------------
# 2. Locate the freshly-built zip
# ------------------------------------------------------------
DIST_DIR="plugin/build/distributions"
mapfile -t candidates < <(ls -t "$DIST_DIR"/opensearch-sql-*.zip 2>/dev/null)
if [ "${#candidates[@]}" -eq 0 ]; then
  echo "ERROR: no zip found under $DIST_DIR after build"
  exit 1
fi
ZIP_PATH="${candidates[0]}"
ZIP_NAME="$(basename "$ZIP_PATH")"

log "Built: $ZIP_PATH"
log "Size:  $(du -h "$ZIP_PATH" | awk '{print $1}')"
log "SHA:   $(shasum -a 256 "$ZIP_PATH" | awk '{print $1}')"

# ------------------------------------------------------------
# 3. Upload to the instance
# ------------------------------------------------------------
log "Uploading to $SSH_USER@$HOST:$REMOTE_DIR/ ..."
scp "$ZIP_PATH" "$SSH_USER@$HOST:$REMOTE_DIR/$ZIP_NAME"

# Also push this directory's scripts in case the operator hasn't rsync'd yet.
log "Uploading path-a scripts..."
rsync -avz --exclude 'ingest-timings.txt' --exclude 'plugin-version.txt' \
  "$SCRIPT_DIR/" "$SSH_USER@$HOST:$REMOTE_DIR/path-a/"

ssh "$SSH_USER@$HOST" "chmod +x $REMOTE_DIR/path-a/*.sh"

cat <<EOF

================================================================
  Upload complete.

  Next step on the instance:
    ssh $SSH_USER@$HOST
    sudo /mnt/ebs/work/path-a/install-plugin.sh
    cd /mnt/ebs/work/path-a
    nohup ./ingest.sh > ingest.log 2>&1 &
    tail -f ingest.log
================================================================
EOF
