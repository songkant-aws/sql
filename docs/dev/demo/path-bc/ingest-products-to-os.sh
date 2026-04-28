#!/usr/bin/env bash
# Run ON MACHINE A: ingest Home_and_Kitchen product metadata into OS.
# Federation side's OS only holds products — no reviews (those live in CH).
#
# Preconditions:
#   - OS container running on 127.0.0.1:9200 with the feat/ppl-federation
#     plugin installed (install-plugin.sh)
#   - /mnt/ebs/work exists and writable by ec2-user

set -euo pipefail

WORK="${WORK:-/mnt/ebs/work}"
OS_URL="${OS_URL:-http://127.0.0.1:9200}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PRODUCTS_MAPPING="${PRODUCTS_MAPPING:-$SCRIPT_DIR/products-mapping.json}"
TIMINGS_FILE="${TIMINGS_FILE:-$WORK/os-ingest-timings.txt}"

META_URL="https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Home_and_Kitchen.jsonl.gz"

mkdir -p "$WORK"
cd "$WORK"

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# Reuse path-a's timing machinery
declare -A PHASE_STARTS
SCRIPT_START_EPOCH=$(date +%s)
{
  echo "# OpenSearch products ingest (Path B/C Machine A)"
  echo "# Host:    $(hostname)"
  echo "# Started: $(date -Is)"
  echo
  printf '%-28s  %-11s  %-11s  %-8s  %s\n' "phase" "start_epoch" "end_epoch" "seconds" "human"
} > "$TIMINGS_FILE"

phase_start() { PHASE_STARTS[$1]=$(date +%s); log "START: $1"; }
phase_end() {
  local name="$1"
  local start=${PHASE_STARTS[$name]:-}
  [ -z "$start" ] && return
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
  log "Timings: $TIMINGS_FILE"
  cat "$TIMINGS_FILE"
}
trap print_summary EXIT

# ------------------------------------------------------------
# 1. Download
# ------------------------------------------------------------
if [ ! -f meta_Home_and_Kitchen.jsonl.gz ]; then
  phase_start "download-meta"
  curl -L --fail -o meta_Home_and_Kitchen.jsonl.gz "$META_URL"
  phase_end   "download-meta"
fi

# ------------------------------------------------------------
# 2. Create index
# ------------------------------------------------------------
phase_start "create-index"
curl -sf -X DELETE "$OS_URL/products" >/dev/null || true
curl -sf -X PUT "$OS_URL/products" \
  -H 'Content-Type: application/json' \
  -d @"$PRODUCTS_MAPPING" | python3 -m json.tool
phase_end   "create-index"

# ------------------------------------------------------------
# 3. Ingest
# ------------------------------------------------------------
if ! command -v jq >/dev/null 2>&1; then
  sudo dnf install -y jq
fi

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
        --data-binary "@$batch_file" -o /dev/null
      : > "$batch_file"
      count=0
    fi
  done
  [ -s "$batch_file" ] && curl -sf -X POST "$OS_URL/$index/_bulk" \
    -H 'Content-Type: application/x-ndjson' \
    --data-binary "@$batch_file" -o /dev/null
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

# ------------------------------------------------------------
# 4. Finalize
# ------------------------------------------------------------
phase_start "refresh-flush"
curl -sf -X POST "$OS_URL/products/_refresh" >/dev/null
curl -sf -X POST "$OS_URL/products/_flush" >/dev/null
phase_end   "refresh-flush"

phase_start "restore-refresh-interval"
curl -sf -X PUT "$OS_URL/products/_settings" \
  -H 'Content-Type: application/json' \
  -d '{"index":{"refresh_interval":"1s"}}' >/dev/null
phase_end   "restore-refresh-interval"

phase_start "force-merge"
curl -sf -X POST "$OS_URL/products/_forcemerge?max_num_segments=1" >/dev/null
phase_end   "force-merge"

{
  echo
  echo "# Final state:"
  curl -s "$OS_URL/_cat/indices/products?v&h=index,docs.count,store.size" | sed 's/^/# /'
} >> "$TIMINGS_FILE"

log "ingest-products-to-os.sh complete."
