# Path A: all-in-OpenSearch benchmark environment

This is the "everything in OS" baseline for the federation demo.
Same Amazon Reviews Home_and_Kitchen data (3.7M products + 67M reviews) gets
loaded into a single OpenSearch node, so the PPL query runs entirely in OS
(no ClickHouse). Numbers from here get compared against Path B/C (federation).

## EC2 launch parameters

| Parameter | Value |
|---|---|
| **Instance type** | `r6i.2xlarge` (8 vCPU, 64 GB RAM) |
| **AMI** | Amazon Linux 2023 (x86_64) |
| **Root volume** | gp3 30 GB (for OS + Docker) |
| **Data volume** | gp3 **500 GB**, attached as `/dev/sdf` (shows up as `/dev/nvme1n1`) |
| **Security group** | inbound: SSH (22) only, from your IP |
| **Key pair** | your usual SSH key |
| **User data** | paste the contents of `bootstrap.sh` |
| **IAM role** | none required (unless you host the plugin zip in S3; see step 4) |

**Region**: whatever is cheapest for you. `us-east-1` or `us-west-2`.

**Cost**: ~$0.50/h on-demand + ~$0.04/GB/mo EBS. A 2-day benchmark run is ~$25-30.

## Launch order

### 1. Launch the instance with `bootstrap.sh` as user-data

At launch time EC2 runs the script as root. After ~2 minutes, the instance
will have:
- Docker installed and running
- `/dev/nvme1n1` formatted as XFS and mounted at `/mnt/ebs`
- OpenSearch 2.15 container listening on `127.0.0.1:9200` only (NOT exposed
  outside the instance — you reach it via SSH tunnel or `curl` from inside)
- Heap sized to 24 GB (fits in 64 GB RAM box with room for page cache)

Check progress:
```bash
ssh ec2-user@<ip>
sudo tail -f /var/log/cloud-init-output.log
docker logs -f os
```

### 2. (Optional) SSH tunnel if you want to hit OS from your laptop

```bash
ssh -L 9200:127.0.0.1:9200 ec2-user@<ip>
# now on your laptop:
curl http://localhost:9200
```

For the benchmark itself this isn't needed — the scripts run inside the box.

### 3. Copy this directory onto the instance

```bash
# from your laptop:
rsync -av docs/dev/demo/path-a/ ec2-user@<ip>:/mnt/ebs/work/path-a/
```

### 4. Install the federation SQL plugin

Path A doesn't use the federation rules (all data is in OS), but it still
needs the SQL plugin for PPL. Use the stock OS SQL plugin OR your branch
build — either works for Path A since no CH datasource is registered.

**Option A: stock plugin** (simpler, if stock OS 2.15 ships one that can run
the query — check `docker exec os /usr/share/opensearch/bin/opensearch-plugin list`).

**Option B: your branch build**:
```bash
# on the instance:
cd /mnt/ebs/work
# upload opensearch-sql-plugin-*.zip here first (scp, s3 cp, etc.)
docker cp opensearch-sql-plugin-2.15.0.0-SNAPSHOT.zip os:/tmp/
docker exec os /usr/share/opensearch/bin/opensearch-plugin install --batch \
  file:///tmp/opensearch-sql-plugin-2.15.0.0-SNAPSHOT.zip
docker restart os
```

Wait for OS to come back up:
```bash
until curl -sf http://127.0.0.1:9200 >/dev/null; do sleep 1; done
```

### 5. Run ingest

```bash
cd /mnt/ebs/work/path-a
./ingest.sh
```

**Expected timing** (on r6i.2xlarge):
- Download ~11 GB: ~5-15 min depending on UCSD bandwidth
- Products (3.7M docs): ~3-5 min
- Reviews (67M docs): **~1-3 hours** — this is the headline number for the
  "all-in-OS ingest cost" story
- Force merge: ~15-30 min

Per-phase wall-clock times are recorded automatically to
`/mnt/ebs/work/ingest-timings.txt` (override with `TIMINGS_FILE=...`).
The file is overwritten each run. Columns: phase name, start epoch, end
epoch, seconds, human-readable duration. A trailing section records final
index sizes and disk usage. This is the file to paste into demo write-ups.

You can safely run `ingest.sh` under `nohup`/`tmux`/`screen` and disconnect:
```bash
nohup ./ingest.sh > ingest.log 2>&1 &
tail -f ingest.log
# after it finishes, the summary is in:
cat /mnt/ebs/work/ingest-timings.txt
```

### 6. Run the benchmark query

```bash
cd /mnt/ebs/work/path-a
./query.sh 2>&1 | tee query-results.txt
```

This produces cold + warm latencies for the full PPL query.

### 7. Capture the headline numbers

After ingest completes, the key artifacts are:

- **`/mnt/ebs/work/ingest-timings.txt`** — per-phase wall-clock times and
  final index state. Paste directly into the demo write-up.
- **`query-results.txt`** (step 6) — cold + warm query latencies.

Extra ad-hoc probes if you want more detail:

```bash
# OS heap / GC state
curl -s 'http://127.0.0.1:9200/_nodes/stats/jvm?pretty' | \
  python3 -c 'import json,sys; d=json.load(sys.stdin); \
    [print(n["jvm"]["mem"]["heap_used_percent"], "%") for n in d["nodes"].values()]'

# Cumulative indexing stats (total time OS spent on bulk writes)
curl -s 'http://127.0.0.1:9200/_nodes/stats/indices/indexing?pretty' | \
  python3 -c 'import json,sys; d=json.load(sys.stdin); \
    [print(n["indices"]["indexing"]) for n in d["nodes"].values()]'
```

Record for the demo:
- Ingest wall-clock time breakdown (from `ingest-timings.txt`)
- Final `store.size` for reviews (from `ingest-timings.txt` footer)
- Cold-run latency, warm-run median latency (from `query-results.txt`)
- Any circuit breaker trips (query.sh reports these)

## Teardown

```bash
# Stop and remove the instance from AWS console (or via CLI):
aws ec2 terminate-instances --instance-ids i-xxx

# EBS volume gets deleted with the instance if "delete on termination" was set.
```

## Why these choices

- **Instance size**: `r6i.2xlarge` represents a typical production OS data node
  (not an oversized benchmark rig). If Path A struggles here, that's the point
  — "the customer would need a bigger box" is itself the cost story.
- **Port 9200 bound to 127.0.0.1**: you asked for no external ports. All access
  is via SSH. Zero auth needed inside the box.
- **Security plugin disabled**: demo environment; avoids TLS setup. Do not
  copy this config to production.
- **`text` field on reviews is `index: false, doc_values: false`**: reviews'
  body text is not searched in our query, so we don't make Path A pay for a
  full-text inverted index on 67M rows. This keeps the benchmark honest
  (we're comparing fact aggregation, not full-text capability).
- **3 shards per index**: matches the Path B/C shard count so shard fan-out
  overhead is the same across paths.
- **`refresh_interval: -1` during ingest**: standard OS tuning for bulk load;
  restored to `1s` after. Force-merge to 1 segment per shard speeds up the
  benchmark query but reflects a mature, "compacted" index — fair for showing
  steady-state behavior.
