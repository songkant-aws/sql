#!/usr/bin/env bash
# Path A ingest: downloads Amazon Reviews (Home_and_Kitchen) and loads BOTH
# products and reviews into the single OpenSearch instance on this host.
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

META_URL="https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Home_and_Kitchen.jsonl.gz"
REVIEWS_URL="https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Home_and_Kitchen.jsonl.gz"

mkdir -p "$WORK"
cd "$WORK"

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# ------------------------------------------------------------
# 1. Download (skip if already present)
# ------------------------------------------------------------
if [ ! -f meta_Home_and_Kitchen.jsonl.gz ]; then
  log "Downloading metadata ~3 GB..."
  curl -L --fail -o meta_Home_and_Kitchen.jsonl.gz "$META_URL"
fi

if [ ! -f Home_and_Kitchen.jsonl.gz ]; then
  log "Downloading reviews ~8.3 GB..."
  curl -L --fail -o Home_and_Kitchen.jsonl.gz "$REVIEWS_URL"
fi

# ------------------------------------------------------------
# 2. Create indices
# ------------------------------------------------------------
log "Creating products index..."
curl -sf -X DELETE "$OS_URL/products" >/dev/null || true
curl -sf -X PUT "$OS_URL/products" \
  -H 'Content-Type: application/json' \
  -d @"$PRODUCTS_MAPPING" | python3 -m json.tool

log "Creating reviews index..."
curl -sf -X DELETE "$OS_URL/reviews" >/dev/null || true
curl -sf -X PUT "$OS_URL/reviews" \
  -H 'Content-Type: application/json' \
  -d @"$REVIEWS_MAPPING" | python3 -m json.tool

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
  local transform="$2"   # jq expression applied per doc, emits the doc JSON
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

log "Ingesting products (~3.7M docs, ~3-5 min)..."
time gunzip -c meta_Home_and_Kitchen.jsonl.gz | \
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
  bulk_load "products" "cat"

log "Ingesting reviews (~67M docs, EXPECT 1-3 hours)..."
time gunzip -c Home_and_Kitchen.jsonl.gz | \
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
  bulk_load "reviews" "cat"

# ------------------------------------------------------------
# 4. Force merge and re-enable refresh for query workload
# ------------------------------------------------------------
log "Flushing + refreshing..."
curl -sf -X POST "$OS_URL/products,reviews/_refresh" | python3 -m json.tool
curl -sf -X POST "$OS_URL/products,reviews/_flush" | python3 -m json.tool

log "Re-enabling refresh_interval=1s..."
curl -sf -X PUT "$OS_URL/products,reviews/_settings" \
  -H 'Content-Type: application/json' \
  -d '{"index":{"refresh_interval":"1s"}}' | python3 -m json.tool

log "Force-merging to 1 segment per shard (optional, speeds up queries)..."
curl -sf -X POST "$OS_URL/products,reviews/_forcemerge?max_num_segments=1" \
  | python3 -m json.tool

# ------------------------------------------------------------
# 5. Report final counts and sizes
# ------------------------------------------------------------
log "Final state:"
curl -s "$OS_URL/_cat/indices/products,reviews?v"
curl -s "$OS_URL/_cat/shards/products,reviews?v"
echo
log "ingest.sh complete. Now run query.sh."
