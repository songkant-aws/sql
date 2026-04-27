# Path A: all-in-OpenSearch benchmark environment

This is the "everything in OS" baseline for the federation demo.
Same Amazon Reviews Home_and_Kitchen data (3.7M products + 67M reviews) gets
loaded into a single OpenSearch node, so the PPL query runs entirely in OS
(no ClickHouse). Numbers from here get compared against Path B/C (federation).

**Important**: Path A must run the **same plugin build** as Path B/C — the
one from the `feat/ppl-federation` branch. Running stock OpenSearch's SQL
plugin would produce numbers that are not fairly comparable (the Calcite
engine behavior between branches is different). These scripts enforce this.

## Prerequisites

- An AWS account and SSH key (ED25519 recommended).
- The repo cloned and checked out on the **`feat/ppl-federation`** branch locally.
- Local Java 21 toolchain (so `./gradlew :plugin:assemble` works on your laptop).

## EC2 launch parameters

| Parameter | Value |
|---|---|
| **Instance type** | `r6i.2xlarge` (8 vCPU, 64 GB RAM) |
| **AMI** | Amazon Linux 2023 (x86_64) |
| **Root volume** | gp3 30 GB (for OS + Docker) |
| **Data volume** | gp3 **500 GB**, attached as `/dev/sdf` (shows up as `/dev/nvme1n1`) |
| **Security group** | inbound: SSH (22) only, from your IP |
| **Key pair** | ED25519 key from AWS console; `chmod 400 ~/.ssh/<key>.pem` |
| **User data** | paste the contents of `bootstrap.sh` |
| **IAM role** | none required |

**Region**: whatever is cheapest for you. `us-east-1` or `us-west-2`.

**Cost**: ~$0.50/h on-demand + ~$0.04/GB/mo EBS. A 2-day benchmark run is ~$25-30.

## OpenSearch version

Scripts target **OpenSearch 3.6.0** (docker tag `opensearchproject/opensearch:3.6.0`).
This matches the `opensearch_version = 3.6.0-SNAPSHOT` declared in the
`feat/ppl-federation` branch's root `build.gradle`. The plugin zip produced
by that branch (`opensearch-sql-3.6.0.0-SNAPSHOT.zip`) installs only on OS 3.6.x.

If you bump the branch's OS version, update `bootstrap.sh` accordingly —
`install-plugin.sh` will refuse to install on a mismatched OS.

## Launch order

### 1. Launch the instance with `bootstrap.sh` as user-data

At launch, EC2 runs the script as root. After ~2 minutes the instance has:
- Docker running
- `/dev/nvme1n1` formatted as XFS and mounted at `/mnt/ebs`
- OpenSearch 3.6.0 container on `127.0.0.1:9200` (NOT exposed outside)
- Heap sized to 24 GB

Note: `bootstrap.sh` does **not** install the SQL plugin — that is stage 2
(below) because the plugin zip lives on your laptop.

Watch progress after SSHing in:
```bash
ssh -i ~/.ssh/<key>.pem ec2-user@<ip>
sudo tail -f /var/log/cloud-init-output.log
docker logs -f os
```

### 2. (Optional) SSH tunnel

```bash
ssh -i ~/.ssh/<key>.pem -L 9200:127.0.0.1:9200 ec2-user@<ip>
# on your laptop:
curl http://localhost:9200   # should return OS 3.6.0 banner
```

### 3. Build the plugin locally and upload

From the repo root on your laptop (must be on `feat/ppl-federation`):

```bash
./docs/dev/demo/path-a/build-and-upload-plugin.sh <ec2-ip-or-alias>
```

This script:
- Runs `./gradlew :plugin:assemble`
- Finds the `plugin/build/distributions/opensearch-sql-*.zip`
- `scp`s the zip to `/mnt/ebs/work/` on the instance
- `rsync`s the path-a scripts to `/mnt/ebs/work/path-a/`
- Prints the next command to run on the instance

If your `~/.ssh/config` has an alias (e.g. `Host path-a-demo`), you can pass
that instead of an IP.

### 4. Install the plugin on the instance

```bash
ssh -i ~/.ssh/<key>.pem ec2-user@<ip>
sudo /mnt/ebs/work/path-a/install-plugin.sh
```

`install-plugin.sh` is strict:
- fails if no plugin zip is found at `/mnt/ebs/work/`
- fails if the plugin's OS version doesn't match the running OS 3.6.0
- verifies the plugin loads after container restart
- records the plugin version + SHA256 to `/mnt/ebs/work/plugin-version.txt`
  (this file goes into the benchmark write-up as evidence of what was tested)

### 5. Run ingest

```bash
cd /mnt/ebs/work/path-a
nohup ./ingest.sh > ingest.log 2>&1 &
tail -f ingest.log
```

**Expected timing** (on r6i.2xlarge):
- Download ~11 GB: ~5-15 min depending on UCSD bandwidth
- Products (3.7M docs): ~3-5 min
- Reviews (67M docs): **~1-3 hours** — headline "all-in-OS ingest cost"
- Force merge: ~15-30 min

Per-phase wall-clock times are recorded automatically to
`/mnt/ebs/work/ingest-timings.txt` (override with `TIMINGS_FILE=...`).
The file is overwritten each run. Columns: phase name, start epoch, end
epoch, seconds, human-readable duration. A trailing section records final
index sizes and disk usage. This is the file to paste into demo write-ups.

### 6. Run the benchmark query

```bash
cd /mnt/ebs/work/path-a
./query.sh 2>&1 | tee query-results.txt
```

Produces cold + warm latencies.

### 7. Capture the headline numbers

After ingest completes, the key artifacts are:

- **`/mnt/ebs/work/plugin-version.txt`** — which plugin build was tested.
- **`/mnt/ebs/work/ingest-timings.txt`** — per-phase wall-clock times and
  final index state. Paste directly into the demo write-up.
- **`query-results.txt`** (step 6) — cold + warm query latencies.

Extra ad-hoc probes:

```bash
# OS heap / GC state
curl -s 'http://127.0.0.1:9200/_nodes/stats/jvm?pretty' | \
  python3 -c 'import json,sys; d=json.load(sys.stdin); \
    [print(n["jvm"]["mem"]["heap_used_percent"], "%") for n in d["nodes"].values()]'

# Cumulative indexing stats
curl -s 'http://127.0.0.1:9200/_nodes/stats/indices/indexing?pretty'
```

Record for the demo:
- Ingest wall-clock time breakdown (from `ingest-timings.txt`)
- Final `store.size` for reviews (from `ingest-timings.txt` footer)
- Cold-run latency, warm-run median latency (from `query-results.txt`)
- Any circuit breaker trips (query.sh reports these)
- Plugin SHA256 (from `plugin-version.txt`) — so B/C can be verified to use
  the same binary

## Teardown

```bash
aws ec2 terminate-instances --instance-ids i-xxx
```

EBS volume gets deleted with the instance if "delete on termination" was set.

## Why these choices

- **OS 3.6.0 / feat/ppl-federation plugin**: Path A must run the same
  plugin build as Path B/C for the comparison to be honest. The two-stage
  install enforces this (user-data + explicit `install-plugin.sh`).
- **Instance size `r6i.2xlarge`**: represents a typical production OS data
  node (not an oversized benchmark rig). If Path A struggles here, that's
  the point — "the customer would need a bigger box" is itself the story.
- **Port 9200 bound to 127.0.0.1**: no external ports. SSH tunnel for access.
  Zero auth needed inside the box.
- **Security plugin disabled**: demo environment; avoids TLS setup.
  Do not copy this config to production.
- **`text` field on reviews is `index: false, doc_values: false`**: reviews'
  body text is not searched in our query, so we don't make Path A pay for a
  full-text inverted index on 67M rows. This keeps the benchmark honest
  (we compare fact aggregation, not full-text capability).
- **3 shards per index**: matches the Path B/C shard count so shard fan-out
  overhead is the same across paths.
- **`refresh_interval: -1` during ingest**: standard OS tuning for bulk load;
  restored to `1s` after. Force-merge to 1 segment per shard is timed as
  part of ingest cost — a customer going all-in on OS pays for it too.
