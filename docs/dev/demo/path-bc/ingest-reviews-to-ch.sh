#!/usr/bin/env bash
# Run ON MACHINE B: download Home_and_Kitchen reviews gz and bulk-insert
# into ClickHouse. Records per-phase timings.
#
# Preconditions (machine-b-bootstrap.sh already done):
#   - CH container named `ch` running, bound to <private-ip>:8123
#   - /mnt/ebs/work exists, writable by ec2-user
#   - ch-schema.sql lives alongside this script

set -euo pipefail

WORK="${WORK:-/mnt/ebs/work}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCHEMA_SQL="${SCHEMA_SQL:-$SCRIPT_DIR/ch-schema.sql}"
TIMINGS_FILE="${TIMINGS_FILE:-$WORK/ch-ingest-timings.txt}"

PRIVATE_IP=$(cat "$WORK/machine-b-private-ip.txt" 2>/dev/null || \
  curl -s -H "X-aws-ec2-metadata-token: $(curl -s -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')" http://169.254.169.254/latest/meta-data/local-ipv4)
CH_URL="http://$PRIVATE_IP:8123"

REVIEWS_URL="https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Home_and_Kitchen.jsonl.gz"

mkdir -p "$WORK"
cd "$WORK"

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# ------------------------------------------------------------
# Timing infra (same pattern as path-a/ingest.sh)
# ------------------------------------------------------------
declare -A PHASE_STARTS
SCRIPT_START_EPOCH=$(date +%s)

{
  echo "# ClickHouse ingest timings (Path B/C Machine B)"
  echo "# Host:    $(hostname)"
  echo "# CH URL:  $CH_URL"
  echo "# Started: $(date -Is)"
  echo
  printf '%-28s  %-11s  %-11s  %-8s  %s\n' "phase" "start_epoch" "end_epoch" "seconds" "human"
} > "$TIMINGS_FILE"

phase_start() { PHASE_STARTS[$1]=$(date +%s); log "START: $1"; }
phase_end() {
  local name="$1"
  local start=${PHASE_STARTS[$name]:-}
  [ -z "$start" ] && { log "WARN: phase_end for $name with no start"; return; }
  local end elapsed human
  end=$(date +%s); elapsed=$((end - start))
  human=$(printf '%dh %dm %ds' $((elapsed/3600)) $(((elapsed%3600)/60)) $((elapsed%60)))
  log "END  : $name (${elapsed}s = ${human})"
  printf '%-28s  %-11s  %-11s  %-8s  %s\n' "$name" "$start" "$end" "${elapsed}s" "$human" >> "$TIMINGS_FILE"
}

print_summary() {
  local total elapsed human
  total=$(date +%s); elapsed=$((total - SCRIPT_START_EPOCH))
  human=$(printf '%dh %dm %ds' $((elapsed/3600)) $(((elapsed%3600)/60)) $((elapsed%60)))
  { echo; echo "# Total wall-clock: ${elapsed}s = ${human}"; echo "# Finished: $(date -Is)"; } >> "$TIMINGS_FILE"
  log "Timings written to $TIMINGS_FILE"
  cat "$TIMINGS_FILE"
}
trap print_summary EXIT

# ------------------------------------------------------------
# 1. Download reviews gz
# ------------------------------------------------------------
if [ ! -f Home_and_Kitchen.jsonl.gz ]; then
  phase_start "download-reviews"
  curl -L --fail -o Home_and_Kitchen.jsonl.gz "$REVIEWS_URL"
  phase_end   "download-reviews"
else
  log "Home_and_Kitchen.jsonl.gz already present, skipping download"
fi

# ------------------------------------------------------------
# 2. Create schema
# ------------------------------------------------------------
phase_start "create-schema"
curl -sf --data-binary @"$SCHEMA_SQL" "$CH_URL/?database=default" \
  && echo "(schema applied)"
phase_end   "create-schema"

# ------------------------------------------------------------
# 3. Ingest — stream gz → clickhouse-client (not curl)
# ------------------------------------------------------------
# IMPORTANT: do NOT use `curl --data-binary @-` for this. curl buffers the
# full stdin into memory before POSTing, which OOMs on 30+ GB of NDJSON.
# clickhouse-client streams stdin block-by-block, sending blocks of
# max_insert_block_size rows without buffering the whole input.

phase_start "ingest-reviews"
gunzip -c Home_and_Kitchen.jsonl.gz | \
  sudo docker exec -i ch clickhouse-client \
    --query "INSERT INTO fed.reviews FORMAT JSONEachRow" \
    --input_format_skip_unknown_fields 1 \
    --max_insert_block_size 1000000
phase_end   "ingest-reviews"

# ------------------------------------------------------------
# 4. Optimize (merge parts to get better primary-key skipping)
# ------------------------------------------------------------
phase_start "optimize-table"
curl -sf --data-binary "OPTIMIZE TABLE fed.reviews FINAL" "$CH_URL/"
phase_end   "optimize-table"

# ------------------------------------------------------------
# 5. Report final state
# ------------------------------------------------------------
{
  echo
  echo "# Final state:"
  echo "# row count:"
  curl -sf --data-binary "SELECT count() FROM fed.reviews FORMAT TSV" "$CH_URL/" | sed 's/^/# /'
  echo "# table size:"
  curl -sf --data-binary "
    SELECT formatReadableSize(sum(bytes_on_disk)) AS on_disk,
           formatReadableSize(sum(data_compressed_bytes)) AS compressed,
           formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
           sum(rows) AS rows
    FROM system.parts
    WHERE table = 'reviews' AND database = 'fed' AND active
    FORMAT Vertical" "$CH_URL/" | sed 's/^/# /'
  echo "# distinct parent_asin (ground truth for correctness):"
  curl -sf --data-binary "SELECT uniqExact(parent_asin) FROM fed.reviews FORMAT TSV" "$CH_URL/" | sed 's/^/# /'
  echo "# disk usage on /mnt/ebs:"
  df -h /mnt/ebs | sed 's/^/# /'
} >> "$TIMINGS_FILE"

log "ingest-reviews-to-ch.sh complete."
