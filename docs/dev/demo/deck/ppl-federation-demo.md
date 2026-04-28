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

OpenSearch customers already have two systems.

- **OS for search** — products, services, logs, traces' searchable metadata.
- **A columnar store for fact** — Snowflake, ClickHouse, Redshift,
  Iceberg. Reviews, clickstream, orders, long-term spans.

The split is a reasonable architecture. **The split stops at the query
layer.** To answer one business question — "average rating on these
cookware products?" — the customer today has to:

1. Search the product catalog in OS / Dashboards
2. Copy the product IDs into a CH SQL client (or Snowflake, or…)
3. Write the join in application code

OS's query entry point — PPL, Dashboards, alerts, BM25 — can't reach
the other half of their data.

---

# Some teams ingest fact into OS anyway. Here's the bill.

Path A: keep everything in OS so one PPL query can touch it all.

Measured on Amazon Reviews 2023 — **3.7 M products, 67 M reviews**.

| Axis | Observation | Number |
|---|---|---:|
| **Ingest cost** | Bulk-loading reviews into OS vs. a columnar engine. | **2h 50m vs 4m 20s** |
| **Storage cost** | OS index vs. columnar compression. | **19.3 GB vs 7.24 GB** |
| **Query latency** | Dashboard query: `group by product, top-K by count`. OS must enumerate all 3.78 M groups. | **~30 s** |

---

# Silent.

And one more cost the customer never sees.

**2 rows returned. ~50 expected. 96 % silent data loss.**

PPL's `join` subsearch has a 50 000-row safety cap; the fact table has
3.78 M distinct join keys. Customer sees "success" in 300 ms. The
dashboard shows the wrong number. Every time.

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

# 6 ms.

That's what ClickHouse takes to aggregate 67 M rows once
`SideInputInListRule` has narrowed the scan to 360 K rows via
`WHERE parent_asin IN (…)`.

- MergeTree primary-key index skips >99 % of partitions
- 1.94 GB → 8.48 MB read per query (233× less)
- **Customer-facing PPL doesn't change.** The planner inserts the
  filter; the runtime binder drains the left side's keys into it.

---

# Same pattern, new market: RAG

The same federation machinery serves the canonical GenAI retrieval
pattern — k-NN top-K on OpenSearch, enrichment on a columnar fact
store:

```
source=docs | where knn(embedding, [query_vec], k=50)
| sort -_score
| inner join left=d right=e on d.doc_id = e.doc_id
  [ source=ch.fed.user_interactions
    | stats sum(engagement) as eng by doc_id ]
```

Three things make this OpenSearch's lane:

- **Unique combination.** k-NN + BM25 + external-fact federation.
  OS is the only engine with all three GA.
- **GA today.** OS k-NN engine has been in production for 3+ years.
  This branch adds the federation half.
- **No one else can do it.** ClickHouse has no mature relevance
  scoring. Vector DBs (Pinecone, Weaviate) have no business-data
  federation.

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

# Possible next steps

- **Engage a design partner** — ideally a retail or observability
  customer with fact data already outside OS.
- Start with **ClickHouse as the reference first datasource** —
  additional JDBC targets plug into the same rule framework
  incrementally.

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
