---
title: "PPL Federation"
subtitle: "Same entry point. Right engine for each workload."
author: "Songkan Tang"
date: "2026-04-28"
geometry: margin=0.9in
fontsize: 11pt
colorlinks: true
linkcolor: blue
---

# The problem

OpenSearch customers stay on OS because it's their **query entry point** —
PPL, Dashboards, BM25, alerting, observability are all wired up there.

When those customers also need **analytics on large fact tables** —
reviews, clickstream, orders — PPL forces them to push all fact data into
OS. That one decision quietly breaks three things.

---

# Three failure modes (Path A, all-in-OS)

Measured on Amazon Reviews 2023 — **3.7 M products, 67 M reviews**.

| Axis | Observation | Number |
|---|---|---:|
| **Ingest cost** | Bulk-loading reviews into OS vs. a columnar engine. | **2h 50m vs 4m 20s** |
| **Storage cost** | OS index vs. columnar compression. | **19.3 GB vs 7.24 GB** |
| **Query latency** | Dashboard query: `group by product, top-K by count`. OS must enumerate all 3.7 M groups. | **~30 s** |
| **Correctness** | PPL `join` subsearch has a 50 000-row safety cap. 3.7 M distinct keys mean **96 % silent data loss**. | **2 rows returned, ~50 expected** |

Customer gets wrong answers in 300 ms, or correct answers in 30 seconds.
Neither works for a dashboard.

---

# Three paths, same PPL query

- **Path A** — all data in OS. Today's default.
- **Path B** — `products` in OS, `reviews` in ClickHouse. PPL routes
  analytical aggregations to CH via JDBC as a single pushdown query
  (`GROUP BY + ORDER BY + LIMIT`).
- **Path C** — Path B plus `SideInputInListRule`: the bounded left
  side's key set is drained at runtime and bound as an array parameter
  into `WHERE parent_asin IN (…)` on the CH query, letting CH's
  primary-key index skip >99 % of the fact table.

**The customer-facing PPL query is identical in all three paths.**
The planner picks the backend and the optimization. No new syntax.

---

# Results

The benchmark query — a typical dashboard drilldown:

```
source=products | where match(title, 'stainless steel cookware')
| sort -_score | head 50
| inner join left=p right=r on p.parent_asin = r.parent_asin
  [ source=ch.fed.reviews | where timestamp > 1640995200000
    | fields parent_asin, rating ]
| stats avg(rating) as avg_r, count() as n by parent_asin
```

| Metric | Path A | Path B | Path C | Improvement (A→C) |
|---|---:|---:|---:|---:|
| Reviews ingest (67 M rows) | **2h 50m** | **4m 20s** | 4m 20s | **~40×** |
| Reviews storage | 19.3 GB (OS) | **7.24 GB** (CH) | 7.24 GB | 2.7× smaller |
| End-to-end latency (warm) | **30 s** | ~600 ms | **~170 ms** | **~176×** |
| ClickHouse engine-only | — | 465 ms | **6–7 ms** | — |
| Fact rows scanned at CH | — | 67 M | **360 K** | 187× less |
| Fact bytes read at CH | — | 1.94 GB | **8.48 MB** | 233× less |

**How to read the table**

Three layers of speedup stack:

1. **Engine selection (A→B)** — columnar storage + hash aggregation
   beats paginated inverted-index aggregation. Same work, one-shot
   instead of 378 pagination round-trips.
2. **IN-list pushdown (B→C)** — the planner drains the bounded
   left-side key set and pushes it as `WHERE parent_asin IN (…)` to CH,
   letting the primary-key index skip >99 % of partitions. CH reads
   360 K rows / 8 MB instead of 67 M rows / 2 GB.
3. **Correctness everywhere below A** — Path A's 50 000-row subsearch
   cap silently drops 96 % of grouped results. Paths B and C do not.

The optimization stacks cleanly: Path C is 3.5× faster than Path B and
~176× faster than Path A, with no change to the customer's PPL.

---

# Architecture

```
  Customer  (PPL · Dashboards · BM25)
       │
       │  source=products | ... | inner join ... [ source=ch.reviews ... ]
       ▼
  ┌──────────── OpenSearch (entry point) ─────────────┐
  │                                                    │
  │  PPL → Calcite planner → Executor                  │
  │         │                                          │
  │         ├── OS Storage (products, small)           │
  │         └── JdbcRel (ClickHouse) — full subtree    │
  │                │                 pushed as one SQL │
  └────────────────┼───────────────────────────────────┘
                   │  JDBC, VPC-private
                   ▼
         ClickHouse (columnar fact table)
           GROUP BY parent_asin
           ORDER BY count DESC LIMIT 5000
           scans 67 M rows in ~465 ms
```

Customer doesn't see a second query language. Federation is a planner
decision.

---

# Business implications

| Concern | Answer |
|---|---|
| Do customers leave OS? | **No.** OS stays the entry point, auth, dashboards, observability. Fact tables just live where they belong. |
| Lock-in to ClickHouse? | No. `PplFederationDialect` is an extension point — Iceberg, Snowflake, other JDBC targets can plug in. |
| Readiness? | Code complete on `feat/ppl-federation`. Benchmarked on Amazon Reviews 2023 (67 M rows). Ready for design review. |

---

# What I need

- Greenlight to propose at the next design review.
- One customer with PPL + 50 M+ fact rows they don't want in OS
  (retail or observability both fit).
- Agreement that **ClickHouse is the reference first datasource** — more
  JDBC targets plug in incrementally on the same rule framework.

---

# Appendix — implementation notes

`SideInputInListRule` fires at Calcite plan time when the left side of
a join has a statically-provable row-count bound (e.g. `| head 50`)
and the right side is a fully-pushable JDBC subtree. At runtime, the
left enumerable is drained, the distinct keys are bound as a
`java.sql.Array` into the generated code, and the ClickHouse JDBC
driver ships the IN-list as either an inlined literal array or a
parameter marker depending on its bind-path. CH's MergeTree with
`ORDER BY (parent_asin, timestamp)` uses the IN-list to skip
partitions at the primary-key index layer — the 360 K rows scanned
for Path C are precisely the rows belonging to the 50 asins emitted
by the OS-side BM25 top-50.

The rule is dialect-agnostic: `PplFederationDialect` advertises
per-dialect capabilities (`supportsArrayInListParam`,
`getInListPushdownThreshold`) so the same framework works for other
JDBC targets (Iceberg, Snowflake, Postgres) once they declare their
in-list support.

---

*Reproduction scripts: `docs/dev/demo/path-a/` and
`docs/dev/demo/path-bc/`. Bug repro: `docs/dev/bug-in-list-pushdown-null-binding.md`.*
