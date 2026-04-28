# Path B / C: federation demo environment

Path B and Path C share the **same two-node deployment**: one Machine A
(OpenSearch holding `products` metadata) and one Machine B (ClickHouse
holding `reviews` as the fact table). They differ only at query time:

- **Path B** (naive federation): the left side's bound exceeds the CH
  dialect's in-list threshold (10,000), so `BoundedJoinHintRule` bails
  out. The right subtree is still pushed down to CH as a single JDBC
  query (aggregation + sort + limit), but no IN-list narrowing happens.
- **Path C** (federation + IN-list pushdown): the left side's bound is
  small (head 50), which triggers both `BoundedJoinHintRule` and
  `SideInputInListRule`. At runtime, the left side is drained first, its
  bounded key set is bound as an array parameter into a
  `WHERE parent_asin IN (…)` filter on the right's CH query, and CH
  scans only the relevant rows.

Both paths use the same plugin build and the same data. Switching between
B and C is purely a PPL query rewrite (`| head 100000` vs `| head 50`).
This keeps the comparison honest: the only variable is whether the rule
fires.

## Numbers we're trying to reproduce

Reference from Path A (all-in-OpenSearch, already measured):

| Query | Path A | Path B (expected) | Path C (expected) |
|---|---|---|---|
| Slow-join (agg + sort -n + head 5000 on right) | **30 s** / 1 row | 1–3 s | 100–500 ms |
| Simple-join (agg by parent_asin on right) | 300 ms / 2 rows (silent truncation) | 500–1000 ms / 2 rows | 100–300 ms / **50 rows** (no cap involved) |

The slow-join shows **engine selection** value (A→B): CH columnar storage
removes the composite-agg pagination bottleneck.

The simple-join shows **IN-list pushdown** value (B→C): CH's primary-key
index skips 99 % of the fact table when we supply `WHERE IN (50 asins)`.

## Prerequisites

- Two EC2 instances in the **same VPC and same subnet** (private IPs
  reachable between them).
- `feat/ppl-federation` branch checked out on your laptop.
- Local Java 21 toolchain (for `./gradlew :opensearch-sql-plugin:bundlePlugin`).
- SSH keypair.

## EC2 launch parameters

Two machines, launched in the same VPC / subnet:

### Machine A — OpenSearch node

| Parameter | Value |
|---|---|
| Instance type | `r6i.xlarge` (4 vCPU, 32 GB RAM) |
| AMI | Amazon Linux 2023 (x86_64) |
| Root volume | gp3 30 GB |
| Data volume | gp3 **100 GB**, attached as `/dev/sdf` (shows up as `/dev/nvme1n1`) |
| Security Group | **SG-A** |
| User data | paste contents of `machine-a-bootstrap.sh` |

**SG-A inbound rules**:
- TCP 22 from your IP (or 0.0.0.0/0 if you need roaming access — SSH key
  auth is strong enough)

Heap set to 8 GB in the bootstrap (Machine A only holds ~4 GB of products
index, no heavy aggregation work).

### Machine B — ClickHouse node

| Parameter | Value |
|---|---|
| Instance type | `r6i.2xlarge` (8 vCPU, 64 GB RAM) |
| AMI | Amazon Linux 2023 (x86_64) |
| Root volume | gp3 30 GB |
| Data volume | gp3 **200 GB**, attached as `/dev/sdf` (shows up as `/dev/nvme1n1`) |
| Security Group | **SG-B** |
| User data | paste contents of `machine-b-bootstrap.sh` |

**SG-B inbound rules**:
- TCP 22 from your IP (for admin)
- TCP 8123 **from security group SG-A** (not from an IP CIDR)

The "source = SG-A" part is the key: AWS Security Groups let you name
another SG as the source, which means "any ENI in SG-A can reach this
port". Machine A's private IP may change (stop/start cycle), but as long
as it's launched in SG-A, the rule holds.

### How to add the SG→SG rule in the AWS console

1. Navigate to Security Groups → select SG-B → Inbound rules → Edit inbound rules
2. Add rule:
   - Type: **Custom TCP**
   - Port range: **8123**
   - Source: **Custom** → paste SG-A's id (e.g. `sg-xxxxxxxx`)
   - Description: `CH HTTP from Machine A SG`
3. Save

**Verify from Machine A** (after both instances are up):
```bash
# On Machine A:
curl -sf http://<machine-b-private-ip>:8123/ping
# Should print: Ok.
```

If not: check SG-B inbound rules, confirm both machines share a VPC, and
that CH is actually listening on Machine B's private IP (not 127.0.0.1).

## Setup walkthrough

### 1. Launch Machine B first (so we have its private IP)

Paste `machine-b-bootstrap.sh` into User data. Wait ~2 minutes. SSH in
and confirm:

```bash
ssh -i ~/.ssh/<key>.pem ec2-user@<machine-b-public-ip>
sudo tail -f /var/log/cloud-init-output.log
# Wait until you see "Machine B bootstrap done."
cat /mnt/ebs/work/machine-b-private-ip.txt
# Save this value — you'll need it in step 5.
```

### 2. Launch Machine A

Paste `machine-a-bootstrap.sh` into User data. Wait for readiness similarly.

### 3. Upload scripts + build plugin from laptop

```bash
# On your laptop, in the repo root:
SSH_KEY=~/.ssh/<key>.pem \
  ./docs/dev/demo/path-bc/build-and-upload-plugin.sh <machine-a-public-ip>
```

This:
- Runs `./gradlew :opensearch-sql-plugin:bundlePlugin -Dopensearch.version=3.6.0 -Dbuild.snapshot=false`
- `scp`s the release zip (`opensearch-sql-3.6.0.0.zip`) to Machine A
- `rsync`s the path-bc scripts to `/mnt/ebs/work/path-bc/`

Also upload the CH ingest scripts to Machine B:

```bash
rsync -avz -e "ssh -i ~/.ssh/<key>.pem" \
  docs/dev/demo/path-bc/{ingest-reviews-to-ch.sh,ch-schema.sql} \
  ec2-user@<machine-b-public-ip>:/mnt/ebs/work/path-bc/
ssh -i ~/.ssh/<key>.pem ec2-user@<machine-b-public-ip> \
  "chmod +x /mnt/ebs/work/path-bc/*.sh"
```

### 4. Install plugin on Machine A

```bash
ssh -i ~/.ssh/<key>.pem ec2-user@<machine-a-public-ip>
sudo /mnt/ebs/work/path-bc/install-plugin.sh
```

This also configures the datasource master key (required for datasource
APIs) into `opensearch.yml` and restarts OS.

### 5. Register the CH datasource (on Machine A)

```bash
CH_PRIVATE_IP=<machine-b-private-ip> \
  /mnt/ebs/work/path-bc/register-datasource.sh
```

The script:
- Probes `http://<ip>:8123/ping` to confirm network path
- Introspects `fed.reviews` schema via `system.columns` on CH (so it
  requires that ingest-reviews-to-ch.sh has already run — see step 6)
- Registers datasource `ch` pointing at `http://<ip>:8123`
- Smoke-tests `source=ch.fed.reviews | head 1`

**If step 6 hasn't run yet, you can do step 5 after step 6.** The order
doesn't matter as long as reviews is populated before you query.

### 6. Ingest reviews into CH (on Machine B)

```bash
ssh -i ~/.ssh/<key>.pem ec2-user@<machine-b-public-ip>
/mnt/ebs/work/path-bc/ingest-reviews-to-ch.sh
```

Expected: ~3–5 minutes for 67 M reviews (CH is ~20× faster than OS for
bulk loads at this row shape).

Final state (check `/mnt/ebs/work/ch-ingest-timings.txt`):
- row count: 67,409,944
- compressed: ~5–10 GB (ZSTD)
- on_disk: ~6–12 GB

### 7. Ingest products into OS (on Machine A)

```bash
ssh -i ~/.ssh/<key>.pem ec2-user@<machine-a-public-ip>
/mnt/ebs/work/path-bc/ingest-products-to-os.sh
```

Expected: ~3–5 minutes for 3.7 M products.

### 8. Run benchmarks

Both query scripts run the same two workloads (slow-join + simple-join)
with warmup + 5 timed runs + explain capture.

```bash
# On Machine A
/mnt/ebs/work/path-bc/query-b.sh
/mnt/ebs/work/path-bc/query-c.sh
```

Artifacts:
- `/mnt/ebs/work/query-b-results.txt` — Path B timings + explain plans
- `/mnt/ebs/work/query-c-results.txt` — Path C timings + explain plans,
  plus `[rule-fired]` or `[rule-did-not-fire]` tag per benchmark

### 9. Capture demo numbers

From the results files, grab:
- Warm median latency per benchmark
- Rows returned per benchmark (correctness check)
- Explain plan fragments showing `JdbcAggregate`, `JdbcSort`, `JdbcSideInputFilter`

These go into the three-path comparison table alongside Path A's numbers.

## Security posture

| Surface | Exposure |
|---|---|
| Machine A 9200 | 127.0.0.1 only (localhost inside EC2) |
| Machine A 22 | via SG-A inbound rule (operator access) |
| Machine B 8123 | private IP only, SG-B allows inbound 8123 from SG-A |
| Machine B 22 | via SG-B inbound rule (operator access) |
| Machine B public | nothing (8123 never bound to 0.0.0.0) |

The datasource credentials (CH username/password) are encrypted at rest
in OS's internal datasource metadata index using the master key generated
by `install-plugin.sh`. Master key lives in `opensearch.yml`; sha256 is
recorded in `plugin-version.txt` for audit.

## Known gotchas (lessons from path-a)

1. **OS 3.6.0 security plugin**: use `DISABLE_INSTALL_DEMO_CONFIG=true` +
   `DISABLE_SECURITY_PLUGIN=true`. `plugins.security.disabled=true` alone
   does not suppress the demo-config installer.
2. **Plugin version**: build with `-Dopensearch.version=3.6.0
   -Dbuild.snapshot=false` so the zip matches the release OS image.
   SNAPSHOT plugins refuse to install into release containers.
3. **macOS bash 3.2**: upload scripts avoid `mapfile` and use
   `${VAR[@]+"${VAR[@]}"}` for possibly-empty array expansion.
4. **Python True/False vs bash true/false**: install-plugin.sh normalizes
   via `str().lower()`.
5. **EBS volume device naming**: Nitro instances use `/dev/nvme1n1`
   (not `/dev/sdf`) inside the OS.
6. **Docker group membership**: after `usermod -aG docker ec2-user`,
   re-SSH or use `sudo docker` until the group takes effect.

## Teardown

```bash
# From your laptop or AWS console:
aws ec2 terminate-instances --instance-ids <machine-a-id> <machine-b-id>
# Delete-on-termination is typically on for EBS; if not, delete volumes too.
```

SG-A and SG-B can be reused for future runs — they don't carry state.
