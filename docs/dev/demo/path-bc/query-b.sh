#!/usr/bin/env bash
# Path B benchmark: naive federation — feature bypass so SideInputInListRule
# does NOT fire. Demonstrates what raw "CH as JDBC sub-schema" achieves
# without the sideways-pushdown optimization.
#
# How we bypass the rule: pass a `bounded_left` join hint with size larger
# than the ClickHouse dialect's in-list threshold (CH_IN_LIST_THRESHOLD,
# currently 10_000). The rule checks this threshold and bails out,
# leaving federation to fall through to the plain JdbcHashJoin path.
#
# Alternative: feature flag or cluster setting could toggle the rule, but
# threshold-bailout uses the production code path with no extra switch.

set -euo pipefail

OS_URL="${OS_URL:-http://127.0.0.1:9200}"
DS_NAME="${DS_NAME:-ch}"
CH_DATABASE="${CH_DATABASE:-fed}"
WARMUP_RUNS="${WARMUP_RUNS:-2}"
TIMED_RUNS="${TIMED_RUNS:-5}"
RESULTS_FILE="${RESULTS_FILE:-/mnt/ebs/work/query-b-results.txt}"

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# -----------------------------------------------------------------
# Queries. LEFT side head is sized (100000) to exceed the CH dialect
# in-list threshold, forcing BoundedJoinHintRule to bail out. The slow
# aggregation + sort in the RIGHT is what we want to exercise against CH.
# -----------------------------------------------------------------

# Benchmark 1: slow-join (agg + sort -n + head 5000 on the right)
cat > /tmp/q-b-slow.json <<EOF
{"query":"source=products | where match(title, 'stainless steel cookware') | sort -_score | head 100000 | inner join left=p right=r on p.parent_asin = r.parent_asin [ source=$DS_NAME.$CH_DATABASE.reviews | stats avg(rating) as avg_r, count() as n by parent_asin | sort -n | head 5000 ]"}
EOF

# Benchmark 2: simple join (right side just agg by parent_asin, no sort-by-measure)
cat > /tmp/q-b-simple.json <<EOF
{"query":"source=products | where match(title, 'stainless steel cookware') | sort -_score | head 100000 | inner join left=p right=r on p.parent_asin = r.parent_asin [ source=$DS_NAME.$CH_DATABASE.reviews | where timestamp > 1640995200000 | stats avg(rating) as avg_r, count() as n by parent_asin ]"}
EOF

run_ppl_timed() {
  local label="$1" payload_file="$2"
  local start end elapsed_ms
  start=$(date +%s%3N)
  curl -sf -X POST "$OS_URL/_plugins/_ppl" \
    -H 'Content-Type: application/json' \
    -d @"$payload_file" > /tmp/q-b-last-response.json
  end=$(date +%s%3N)
  elapsed_ms=$((end - start))
  local rows
  rows=$(python3 -c "import json; d=json.load(open('/tmp/q-b-last-response.json')); print(len(d.get('datarows', [])))" 2>/dev/null || echo "?")
  echo "  $label  ${elapsed_ms}ms  rows=$rows"
  echo "  $label  ${elapsed_ms}ms  rows=$rows" >> "$RESULTS_FILE"
}

benchmark_query() {
  local name="$1" payload_file="$2"
  echo "" >> "$RESULTS_FILE"
  echo "=== $name ===" >> "$RESULTS_FILE"
  log "=== $name ==="

  log "Warmup:"
  for i in $(seq 1 "$WARMUP_RUNS"); do run_ppl_timed "warmup-$i" "$payload_file"; done

  log "Timed:"
  sudo sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null || true
  for i in $(seq 1 "$TIMED_RUNS"); do run_ppl_timed "run-$i" "$payload_file"; done

  log "Explain:"
  curl -s -X POST "$OS_URL/_plugins/_ppl/_explain" \
    -H 'Content-Type: application/json' \
    -d @"$payload_file" > /tmp/q-b-explain.json
  {
    echo "--- explain ---"
    python3 -c "
import json
d = json.load(open('/tmp/q-b-explain.json'))
if 'calcite' in d:
    print('LOGICAL:'); print(d['calcite']['logical'])
    print('PHYSICAL:'); print(d['calcite']['physical'])
else:
    print(json.dumps(d, indent=2))
"
  } >> "$RESULTS_FILE"
}

# -----------------------------------------------------------------
# Header
# -----------------------------------------------------------------
{
  echo "# Path B benchmark results (naive federation, no IN-list pushdown)"
  echo "# Host:    $(hostname)"
  echo "# Started: $(date -Is)"
  echo "# OS URL:  $OS_URL"
  echo "# Datasource: $DS_NAME"
} > "$RESULTS_FILE"

# -----------------------------------------------------------------
# Sanity: confirm the datasource resolves
# -----------------------------------------------------------------
log "Smoke-testing datasource reachability..."
curl -s -X POST "$OS_URL/_plugins/_ppl" \
  -H 'Content-Type: application/json' \
  -d "{\"query\":\"source=$DS_NAME.$CH_DATABASE.reviews | head 1\"}" > /tmp/smoke.json
if python3 -c "import json,sys; d=json.load(open('/tmp/smoke.json')); sys.exit(0 if 'datarows' in d else 1)"; then
  log "Datasource OK."
else
  cat /tmp/smoke.json
  echo "ERROR: datasource not reachable. Run register-datasource.sh first."
  exit 1
fi

# -----------------------------------------------------------------
# Run benchmarks
# -----------------------------------------------------------------
benchmark_query "Benchmark 1: slow-join (agg + sort -n + head 5000 on right)" /tmp/q-b-slow.json
benchmark_query "Benchmark 2: simple-join (agg by parent_asin, no sort-by-measure)" /tmp/q-b-simple.json

log "Results written to $RESULTS_FILE"
cat "$RESULTS_FILE"
