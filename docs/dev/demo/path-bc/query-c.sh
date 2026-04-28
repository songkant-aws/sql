#!/usr/bin/env bash
# Path C benchmark: federation WITH SideInputInListRule enabled.
# Left-side head=50 is within the CH dialect in-list threshold (10_000),
# so BoundedJoinHintRule + SideInputInListRule fire, pushing down the
# left's 50 parent_asin values as a WHERE IN (…) filter on CH.

set -euo pipefail

OS_URL="${OS_URL:-http://127.0.0.1:9200}"
DS_NAME="${DS_NAME:-ch}"
CH_DATABASE="${CH_DATABASE:-fed}"
WARMUP_RUNS="${WARMUP_RUNS:-2}"
TIMED_RUNS="${TIMED_RUNS:-5}"
RESULTS_FILE="${RESULTS_FILE:-/mnt/ebs/work/query-c-results.txt}"

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

# Same query shapes as Path B, but `head 50` on the left keeps cardinality
# under the pushdown threshold, so the rule applies.
cat > /tmp/q-c-slow.json <<EOF
{"query":"source=products | where match(title, 'stainless steel cookware') | sort -_score | head 50 | inner join left=p right=r on p.parent_asin = r.parent_asin [ source=$DS_NAME.$CH_DATABASE.reviews | stats avg(rating) as avg_r, count() as n by parent_asin | sort -n | head 5000 ]"}
EOF

cat > /tmp/q-c-simple.json <<EOF
{"query":"source=products | where match(title, 'stainless steel cookware') | sort -_score | head 50 | inner join left=p right=r on p.parent_asin = r.parent_asin [ source=$DS_NAME.$CH_DATABASE.reviews | where timestamp > 1640995200000 | stats avg(rating) as avg_r, count() as n by parent_asin ]"}
EOF

run_ppl_timed() {
  local label="$1" payload_file="$2"
  local start end elapsed_ms
  start=$(date +%s%3N)
  curl -sf -X POST "$OS_URL/_plugins/_ppl" \
    -H 'Content-Type: application/json' \
    -d @"$payload_file" > /tmp/q-c-last-response.json
  end=$(date +%s%3N)
  elapsed_ms=$((end - start))
  local rows
  rows=$(python3 -c "import json; d=json.load(open('/tmp/q-c-last-response.json')); print(len(d.get('datarows', [])))" 2>/dev/null || echo "?")
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
    -d @"$payload_file" > /tmp/q-c-explain.json

  # Key check: does the physical plan contain JdbcSideInputFilter?
  # If yes, the rule fired (Path C); if no, this is effectively Path B.
  if python3 -c "
import json, sys
d = json.load(open('/tmp/q-c-explain.json'))
plan = d.get('calcite', {}).get('physical', '')
sys.exit(0 if 'JdbcSideInputFilter' in plan else 1)
"; then
    echo "  [rule-fired] JdbcSideInputFilter present in physical plan" >> "$RESULTS_FILE"
    log "  JdbcSideInputFilter fired (Path C behavior confirmed)"
  else
    echo "  [rule-did-not-fire] JdbcSideInputFilter NOT in physical plan" >> "$RESULTS_FILE"
    log "  WARNING: JdbcSideInputFilter did not fire — this is Path B behavior"
  fi

  {
    echo "--- explain ---"
    python3 -c "
import json
d = json.load(open('/tmp/q-c-explain.json'))
if 'calcite' in d:
    print('LOGICAL:'); print(d['calcite']['logical'])
    print('PHYSICAL:'); print(d['calcite']['physical'])
else:
    print(json.dumps(d, indent=2))
"
  } >> "$RESULTS_FILE"
}

{
  echo "# Path C benchmark results (federation + SideInputInListRule)"
  echo "# Host:    $(hostname)"
  echo "# Started: $(date -Is)"
  echo "# OS URL:  $OS_URL"
  echo "# Datasource: $DS_NAME"
} > "$RESULTS_FILE"

# Smoke
log "Smoke-testing datasource reachability..."
curl -s -X POST "$OS_URL/_plugins/_ppl" \
  -H 'Content-Type: application/json' \
  -d "{\"query\":\"source=$DS_NAME.$CH_DATABASE.reviews | head 1\"}" > /tmp/smoke.json
python3 -c "import json,sys; d=json.load(open('/tmp/smoke.json')); sys.exit(0 if 'datarows' in d else 1)" || {
  cat /tmp/smoke.json
  echo "ERROR: datasource not reachable. Run register-datasource.sh first."
  exit 1
}

benchmark_query "Benchmark 1: slow-join" /tmp/q-c-slow.json
benchmark_query "Benchmark 2: simple-join (IN-list pushdown expected)" /tmp/q-c-simple.json

log "Results: $RESULTS_FILE"
cat "$RESULTS_FILE"
