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

# The opportunity

OpenSearch excels at search, relevance, and observability — that's why
customers stay.

As their workload expands to **fact-table analytics** — reviews,
clickstream, orders, long-term spans — they need the right engine for
that work too. Today, the only paths are:

1. Duplicate the fact data into OS, or
2. Stitch results together in application code.

Either way, customers pay an **ETL tax** that scales with their data.

---

# What customers work around today

Path A: keep everything in OS so one PPL query can touch it all.

Measured on Amazon Reviews 2023 — **3.7 M products, 67 M reviews**.

| Today's tax | What teams experience | Impact |
|---|---|---:|
| **Ingest** | Teams wait through every duplication cycle. | **2h 50m → 4m 20s** |
| **Storage** | Storage cost scales with raw documents, not data. | **19.3 GB → 7.24 GB** |
| **Latency** | Dashboard iteration waits 30 s per cycle on group-by at fact-table scale. | **~30 s** |

---

# Single-engine paths hit a wall at fact-table scale

PPL's `join` subsearch bounds join output to 50 K rows —
correct-by-design, so the engine never OOMs. Fact tables have millions
of distinct join keys.

**At this scale, 2 rows are returned where ~50 are expected.**

Bounded by the 50 K subsearch cap — correct-by-design, but limiting at
fact-table scale. The right fix is not a larger cap. It's a different
engine for the fact side.

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

Each path uses each engine where it excels:

- **Path A**: OS handles search and fact analytics — one engine, two workloads.
- **Path B**: OS does search. CH does analytics. JDBC carries the handoff.
- **Path C**: Planner binds the OS search result into the CH analytics query at runtime.

Three layers of speedup stack:

1. **Engine selection (A→B)** — columnar storage + hash aggregation
   beats paginated inverted-index aggregation at fact-table scale. Same
   work, one-shot instead of 378 pagination round-trips.
2. **IN-list pushdown (B→C)** — the planner drains the bounded
   left-side key set and pushes it as `WHERE parent_asin IN (…)` to CH,
   letting the primary-key index skip >99 % of partitions. CH reads
   360 K rows / 8 MB instead of 67 M rows / 2 GB.
3. **Join semantics everywhere below A** — Path A's 50 K-row subsearch
   cap bounds join output, correct-by-design but limiting at fact-table
   scale. Paths B and C route the work to an engine built for that scale.

The optimization stacks cleanly: Path C is 3.5× faster than Path B and
~176× faster than Path A, with no change to the customer's PPL.

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

**OS's strengths stay strengths.** PPL, BM25, k-NN, Dashboards,
observability integration — all unchanged. This makes the search entry
point also the federation entry point.

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
