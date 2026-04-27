#!/usr/bin/env bash
# Path A benchmark: runs the federation-equivalent PPL query entirely inside OS.
# Captures cold + warm latencies and prints OS-side stats.
#
# Must run after ingest.sh has populated both products and reviews indices.

set -euo pipefail

OS_URL="${OS_URL:-http://127.0.0.1:9200}"
WARMUP_RUNS="${WARMUP_RUNS:-2}"
TIMED_RUNS="${TIMED_RUNS:-5}"

PPL=$(cat <<'EOF'
source=products | where match(title, "stainless steel cookware") | sort -_score | head 50
| inner join left=p right=r on p.parent_asin = r.parent_asin
  [ source=reviews | where timestamp > 1640995200000
    | stats avg(rating) as avg_r, count() as n by parent_asin ]
EOF
)

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

run_ppl_timed() {
  local label="$1"
  local start end elapsed_ms took
  start=$(date +%s%3N)
  response=$(curl -sf -X POST "$OS_URL/_plugins/_ppl" \
    -H 'Content-Type: application/json' \
    -d "$(jq -cn --arg q "$PPL" '{query:$q}')")
  end=$(date +%s%3N)
  elapsed_ms=$((end - start))
  # OS doesn't expose `took` in PPL response directly; rely on wall clock.
  echo "  $label: ${elapsed_ms}ms"
}

log "Cluster health:"
curl -s "$OS_URL/_cluster/health?pretty"

log "Index sizes and doc counts:"
curl -s "$OS_URL/_cat/indices/products,reviews?v&h=index,docs.count,store.size,pri.store.size"

log "Checking PPL plugin availability..."
if ! curl -sf -X POST "$OS_URL/_plugins/_ppl" \
      -H 'Content-Type: application/json' \
      -d '{"query":"source=products | head 1"}' >/dev/null 2>&1; then
  echo "ERROR: PPL endpoint not responding. Is the SQL plugin installed?"
  echo "       See bootstrap.sh step 6 (plugin install)."
  exit 1
fi

log "Warmup runs ($WARMUP_RUNS):"
for i in $(seq 1 "$WARMUP_RUNS"); do
  run_ppl_timed "warmup-$i"
done

log "Timed runs ($TIMED_RUNS):"
for i in $(seq 1 "$TIMED_RUNS"); do
  run_ppl_timed "run-$i"
done

log "Capturing explain output (for the record)..."
curl -s -X POST "$OS_URL/_plugins/_ppl/_explain" \
  -H 'Content-Type: application/json' \
  -d "$(jq -cn --arg q "$PPL" '{query:$q}')" | python3 -m json.tool

log "Slow log / circuit breaker stats:"
curl -s "$OS_URL/_nodes/stats/breaker?pretty" | \
  python3 -c 'import json,sys; d=json.load(sys.stdin); \
    [print(k, v["tripped"], "tripped") for node in d["nodes"].values() \
     for k, v in node["breakers"].items() if v["tripped"] > 0]' || true

log "query.sh complete."
