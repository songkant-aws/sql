#!/usr/bin/env bash
# Path A ingest: downloads Amazon Reviews (Home_and_Kitchen) and loads BOTH
# products and reviews into the single OpenSearch instance on this host.
#
# Records per-phase wall-clock timings to ingest-timings.txt so the "all-in-OS
# ingest cost" number for the demo is captured automatically — no need to
# grep the run log.
#
# Preconditions (bootstrap.sh has already done these):
#   - Docker running with OS container named `os` bound to 127.0.0.1:9200
#   - /mnt/ebs/work exists and is writable by ec2-user
#   - mapping files (products-mapping.json, reviews-mapping.json) are in the
#     same directory as this script, OR pass their absolute paths in env.

set -euo pipefail

WORK="${WORK:-/mnt/ebs/work}"
OS_URL="${OS_URL:-http://127.0.0.1:9200}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PRODUCTS_MAPPING="${PRODUCTS_MAPPING:-$SCRIPT_DIR/products-mapping.json}"
REVIEWS_MAPPING="${REVIEWS_MAPPING:-$SCRIPT_DIR/reviews-mapping.json}"
TIMINGS_FILE="${TIMINGS_FILE:-$WORK/ingest-timings.txt}"

META_URL="https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Home_and_Kitchen.jsonl.gz"
REVIEWS_URL="https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Home_and_Kitchen.jsonl.gz"

mkdir -p "$WORK"
cd "$WORK"

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# ------------------------------------------------------------
# Timing machinery
# ------------------------------------------------------------
# Records phase timings both to stdout (via `log`) and to a structured file
# that gets printed as a final summary. Each phase call:
#   phase_start "download-reviews"
#   ... do work ...
#   phase_end   "download-reviews"
#
# Produces lines in TIMINGS_FILE like:
#   download-reviews  1714186200  1714186410  210s  (3m 30s)
declare -A PHASE_STARTS
SCRIPT_START_EPOCH=$(date +%s)

# Initialize / truncate timings file with a header that records the run.
{
  echo "# Path A ingest timings"
  echo "# Host:    $(hostname)"
  echo "# Started: $(date -Is)"
  echo "# Script:  $0"
  echo
  printf '%-28s  %-11s  %-11s  %-8s  %s\n' "phase" "start_epoch" "end_epoch" "seconds" "human"
} > "$TIMINGS_FILE"

phase_start() {
  local name="$1"
  PHASE_STARTS[$name]=$(date +%s)
  log "START: $name"
}

phase_end() {
  local name="$1"
  local start=${PHASE_STARTS[$name]:-}
  if [ -z "$start" ]; then
    log "WARN: phase_end called for $name with no matching phase_start"
    return
  fi
  local end elapsed human
  end=$(date +%s)
  elapsed=$((end - start))
  human=$(printf '%dh %dm %ds' $((elapsed/3600)) $(((elapsed%3600)/60)) $((elapsed%60)))
  log "END  : $name (${elapsed}s = ${human})"
  printf '%-28s  %-11s  %-11s  %-8s  %s\n' \
    "$name" "$start" "$end" "${elapsed}s" "$human" >> "$TIMINGS_FILE"
}

print_summary() {
  local total elapsed human
  total=$(date +%s)
  elapsed=$((total - SCRIPT_START_EPOCH))
  human=$(printf '%dh %dm %ds' $((elapsed/3600)) $(((elapsed%3600)/60)) $((elapsed%60)))
  {
    echo
    echo "# Total wall-clock: ${elapsed}s = ${human}"
    echo "# Finished: $(date -Is)"
  } >> "$TIMINGS_FILE"
  log "Timings written to $TIMINGS_FILE"
  echo
  cat "$TIMINGS_FILE"
}
trap print_summary EXIT

# ------------------------------------------------------------
# 1. Download (skip if already present)
# ------------------------------------------------------------
if [ ! -f meta_Home_and_Kitchen.jsonl.gz ]; then
  phase_start "download-meta"
  curl -L --fail -o meta_Home_and_Kitchen.jsonl.gz "$META_URL"
  phase_end   "download-meta"
else
  log "meta_Home_and_Kitchen.jsonl.gz already exists, skipping download"
fi

if [ ! -f Home_and_Kitchen.jsonl.gz ]; then
  phase_start "download-reviews"
  curl -L --fail -o Home_and_Kitchen.jsonl.gz "$REVIEWS_URL"
  phase_end   "download-reviews"
else
  log "Home_and_Kitchen.jsonl.gz already exists, skipping download"
fi

# ------------------------------------------------------------
# 2. Create indices
# ------------------------------------------------------------
phase_start "create-indices"
curl -sf -X DELETE "$OS_URL/products" >/dev/null || true
curl -sf -X PUT "$OS_URL/products" \
  -H 'Content-Type: application/json' \
  -d @"$PRODUCTS_MAPPING" | python3 -m json.tool

curl -sf -X DELETE "$OS_URL/reviews" >/dev/null || true
curl -sf -X PUT "$OS_URL/reviews" \
  -H 'Content-Type: application/json' \
  -d @"$REVIEWS_MAPPING" | python3 -m json.tool
phase_end   "create-indices"

# ------------------------------------------------------------
# 3. Ingest — stream gz → jq → bulk. No full decompression on disk.
# ------------------------------------------------------------
# Install jq if missing (Amazon Linux 2023 doesn't ship it by default).
if ! command -v jq >/dev/null 2>&1; then
  sudo dnf install -y jq
fi

# Bulk loader: reads NDJSON from stdin, wraps in _bulk action lines, POSTs
# in batches of BATCH_DOCS docs. Simple, no external deps.
BATCH_DOCS="${BATCH_DOCS:-5000}"

bulk_load() {
  local index="$1"
  local count=0
  local batch_file
  batch_file="$(mktemp)"
  trap 'rm -f "$batch_file"' RETURN

  while IFS= read -r line; do
    printf '{"index":{}}\n%s\n' "$line" >> "$batch_file"
    count=$((count + 1))
    if [ "$count" -ge "$BATCH_DOCS" ]; then
      curl -sf -X POST "$OS_URL/$index/_bulk" \
        -H 'Content-Type: application/x-ndjson' \
        --data-binary "@$batch_file" \
        -o /dev/null
      : > "$batch_file"
      count=0
    fi
  done

  if [ -s "$batch_file" ]; then
    curl -sf -X POST "$OS_URL/$index/_bulk" \
      -H 'Content-Type: application/x-ndjson' \
      --data-binary "@$batch_file" \
      -o /dev/null
  fi
}

phase_start "ingest-products"
gunzip -c meta_Home_and_Kitchen.jsonl.gz | \
  jq -cr '{
    parent_asin,
    title,
    description: (.description // [] | join(" ")),
    features: (.features // [] | join(" ")),
    categories,
    main_category,
    store,
    average_rating,
    rating_number,
    price: (.price | tonumber? // null)
  }' | \
  bulk_load "products"
phase_end   "ingest-products"

phase_start "ingest-reviews"
gunzip -c Home_and_Kitchen.jsonl.gz | \
  jq -cr '{
    parent_asin,
    asin,
    user_id,
    rating,
    helpful_vote,
    timestamp,
    verified_purchase,
    title,
    text
  }' | \
  bulk_load "reviews"
phase_end   "ingest-reviews"

# ------------------------------------------------------------
# 4. Post-ingest finalization — these DO count toward "ingest cost"
#    (a customer who goes all-in on OS pays for them too).
# ------------------------------------------------------------
phase_start "refresh-flush"
curl -sf -X POST "$OS_URL/products,reviews/_refresh" | python3 -m json.tool
curl -sf -X POST "$OS_URL/products,reviews/_flush" | python3 -m json.tool
phase_end   "refresh-flush"

phase_start "restore-refresh-interval"
curl -sf -X PUT "$OS_URL/products,reviews/_settings" \
  -H 'Content-Type: application/json' \
  -d '{"index":{"refresh_interval":"1s"}}' | python3 -m json.tool
phase_end   "restore-refresh-interval"

phase_start "force-merge"
curl -sf -X POST "$OS_URL/products,reviews/_forcemerge?max_num_segments=1" \
  | python3 -m json.tool
phase_end   "force-merge"

# ------------------------------------------------------------
# 5. Final index state (captured to timings file for the record)
# ------------------------------------------------------------
{
  echo
  echo "# Final index state:"
  curl -s "$OS_URL/_cat/indices/products,reviews?v&h=index,docs.count,store.size,pri.store.size" \
    | sed 's/^/# /'
  echo
  echo "# Shard distribution:"
  curl -s "$OS_URL/_cat/shards/products,reviews?v&h=index,shard,prirep,docs,store" \
    | sed 's/^/# /'
  echo
  echo "# Disk usage on /mnt/ebs:"
  df -h /mnt/ebs | sed 's/^/# /'
} >> "$TIMINGS_FILE"

log "ingest.sh complete. Now run query.sh."
