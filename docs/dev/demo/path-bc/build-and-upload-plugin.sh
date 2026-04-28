#!/usr/bin/env bash
# Build the feat/ppl-federation SQL plugin on your laptop and upload to
# Machine A (OpenSearch for Path B/C). Same build settings as path-a's
# script — release build targeting OS 3.6.0, matching the docker image.
#
# Usage:
#   SSH_KEY=~/.ssh/<key>.pem \
#     ./docs/dev/demo/path-bc/build-and-upload-plugin.sh <machine-a-ip>

set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <machine-a-host>"
  echo ""
  echo "Env vars:"
  echo "  SSH_KEY   path to .pem if <host> is a raw IP"
  echo "  SSH_USER  remote user (default: ec2-user)"
  exit 1
fi

HOST="$1"
SSH_USER="${SSH_USER:-ec2-user}"
REMOTE_DIR="${REMOTE_DIR:-/mnt/ebs/work}"

SSH_KEY="${SSH_KEY:-}"
if [ -n "$SSH_KEY" ]; then
  SSH_OPT_CMD="ssh -i $SSH_KEY -o StrictHostKeyChecking=accept-new"
  SCP_CMD="scp -i $SSH_KEY -o StrictHostKeyChecking=accept-new"
  RSYNC_OPTS=(-e "$SSH_OPT_CMD")
else
  SSH_OPT_CMD="ssh"
  SCP_CMD="scp"
  RSYNC_OPTS=()
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
cd "$REPO_ROOT"

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [ "$CURRENT_BRANCH" != "feat/ppl-federation" ]; then
  echo "WARNING: current branch is '$CURRENT_BRANCH', not 'feat/ppl-federation'."
  read -r -p "Continue anyway? [y/N] " ans
  [[ "$ans" =~ ^[Yy]$ ]] || exit 1
fi

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

log "Building plugin (release, OS 3.6.0 target)..."
OS_TARGET_VERSION="${OS_TARGET_VERSION:-3.6.0}"
./gradlew :opensearch-sql-plugin:bundlePlugin \
  -Dopensearch.version="$OS_TARGET_VERSION" \
  -Dbuild.snapshot=false

DIST_DIR="plugin/build/distributions"
ZIP_PATH=$(ls -t "$DIST_DIR"/opensearch-sql-*.zip 2>/dev/null | head -n1)
if [ -z "$ZIP_PATH" ]; then
  echo "ERROR: no zip found under $DIST_DIR"
  exit 1
fi
ZIP_NAME="$(basename "$ZIP_PATH")"

log "Built: $ZIP_PATH  ($(du -h "$ZIP_PATH" | awk '{print $1}'))"
log "SHA:   $(shasum -a 256 "$ZIP_PATH" | awk '{print $1}')"

log "Uploading zip to $SSH_USER@$HOST:$REMOTE_DIR/..."
$SCP_CMD "$ZIP_PATH" "$SSH_USER@$HOST:$REMOTE_DIR/$ZIP_NAME"

log "Uploading path-bc scripts to $SSH_USER@$HOST:$REMOTE_DIR/path-bc/..."
rsync -avz ${RSYNC_OPTS[@]+"${RSYNC_OPTS[@]}"} \
  --exclude 'ch-ingest-timings.txt' \
  --exclude 'os-ingest-timings.txt' \
  --exclude 'plugin-version.txt' \
  --exclude 'query-results.txt' \
  "$SCRIPT_DIR/" "$SSH_USER@$HOST:$REMOTE_DIR/path-bc/"

$SSH_OPT_CMD "$SSH_USER@$HOST" "chmod +x $REMOTE_DIR/path-bc/*.sh"

cat <<EOF

================================================================
  Upload complete.

  Next on Machine A:
    ssh $SSH_USER@$HOST
    sudo /mnt/ebs/work/path-bc/install-plugin.sh

  Then register datasource (with Machine B's private IP):
    CH_PRIVATE_IP=<machine-b-ip> \\
      /mnt/ebs/work/path-bc/register-datasource.sh

  Then ingest products:
    /mnt/ebs/work/path-bc/ingest-products-to-os.sh
================================================================
EOF
