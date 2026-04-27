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
  echo ""
  echo "Env vars:"
  echo "  SSH_KEY   path to .pem if <ec2-host> is a raw IP (default: empty,"
  echo "            which lets ssh/scp resolve IdentityFile via ~/.ssh/config)"
  echo "  SSH_USER  remote user (default: ec2-user)"
  echo ""
  echo "Examples:"
  echo "  SSH_KEY=~/.ssh/path-a.pem $0 54.123.45.67"
  echo "  $0 path-a-demo    # uses ~/.ssh/config alias"
  exit 1
fi

HOST="$1"
SSH_USER="${SSH_USER:-ec2-user}"
REMOTE_DIR="${REMOTE_DIR:-/mnt/ebs/work}"

# Let the operator point at an explicit key (raw IP + pem file). If they
# configured an SSH alias in ~/.ssh/config, this can be left empty — ssh/scp
# will pick up IdentityFile from the alias automatically.
#
# Using plain string expansion (not arrays) keeps this compatible with
# macOS's ancient bash 3.2. SSH_OPT carries an -i flag pair; RSYNC_E carries
# a single `-e "ssh -i …"` form. When SSH_KEY is empty, both are empty and
# the commands below run unchanged.
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
# The Gradle project name is 'opensearch-sql-plugin' (see settings.gradle);
# the on-disk directory is 'plugin/'. bundlePlugin is the canonical task
# that produces the installable zip (integ-test also depends on it).
./gradlew :opensearch-sql-plugin:bundlePlugin

# ------------------------------------------------------------
# 2. Locate the freshly-built zip
# ------------------------------------------------------------
DIST_DIR="plugin/build/distributions"
# Pick the newest zip matching the pattern. Avoid `mapfile` (bash 4+) for
# macOS compatibility — macOS ships bash 3.2 by default.
ZIP_PATH=$(ls -t "$DIST_DIR"/opensearch-sql-*.zip 2>/dev/null | head -n1)
if [ -z "$ZIP_PATH" ]; then
  echo "ERROR: no zip found under $DIST_DIR after build"
  exit 1
fi
ZIP_NAME="$(basename "$ZIP_PATH")"

log "Built: $ZIP_PATH"
log "Size:  $(du -h "$ZIP_PATH" | awk '{print $1}')"
log "SHA:   $(shasum -a 256 "$ZIP_PATH" | awk '{print $1}')"

# ------------------------------------------------------------
# 3. Upload to the instance
# ------------------------------------------------------------
log "Uploading to $SSH_USER@$HOST:$REMOTE_DIR/ ..."
$SCP_CMD "$ZIP_PATH" "$SSH_USER@$HOST:$REMOTE_DIR/$ZIP_NAME"

# Also push this directory's scripts in case the operator hasn't rsync'd yet.
log "Uploading path-a scripts..."
# ${RSYNC_OPTS[@]+"${RSYNC_OPTS[@]}"} is the bash-3.2-safe way to expand a
# possibly-empty array under `set -u`.
rsync -avz ${RSYNC_OPTS[@]+"${RSYNC_OPTS[@]}"} \
  --exclude 'ingest-timings.txt' --exclude 'plugin-version.txt' \
  "$SCRIPT_DIR/" "$SSH_USER@$HOST:$REMOTE_DIR/path-a/"

$SSH_OPT_CMD "$SSH_USER@$HOST" "chmod +x $REMOTE_DIR/path-a/*.sh"

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
