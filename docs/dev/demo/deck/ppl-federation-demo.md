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

# Two paths, same PPL query

- **Path A** — all data in OS. Today's default.
- **Path B** — `products` in OS, `reviews` in ClickHouse. PPL routes
  analytical aggregations to CH via JDBC as a single pushdown query
  (`GROUP BY + ORDER BY + LIMIT`).

**The customer-facing PPL query is identical in both paths.**
The planner picks the backend. No new syntax, no new datasource call
from the customer side.

---

# Results

The benchmark query — a typical dashboard drilldown:

```
source=products | where match(title, 'stainless steel cookware')
| sort -_score | head 50
| inner join left=p right=r on p.parent_asin = r.parent_asin
  [ source=reviews
    | stats avg(rating), count() by parent_asin
    | sort -n | head 5000 ]
```

| Metric | Path A (all-in-OS) | Path B (federation) | Improvement |
|---|---:|---:|---:|
| Ingest time (reviews, 67 M rows) | **2h 50m** | **4m 20s** | **~40×** |
| Total end-to-end ingest | 3h 36m | 36m | ~6× |
| Reviews storage footprint | **19.3 GB** (OS) | **7.24 GB** (CH) | 2.7× smaller |
| Slow-join end-to-end (warm) | **30 s** | **~600 ms** | **~50×** |
| Slow-join engine-only (CH-side) | — | **~465 ms** | — |
| Simple-join end-to-end (warm) | 300 ms | ~2.3 s | (Path A wrong) |
| Simple-join rows returned | **2** (96 % loss) | **1197** | correctness |

**How to read the table**

Two things happen at the same time when fact aggregation moves to a
columnar engine:

1. **Engine selection matters more than anything else.** Same aggregation
   work, CH does it in ~465 ms; OS composite aggregation does it in
   ~30 s. That's a **64×** architectural gap driven by column storage +
   hash aggregation vs. paginated inverted-index walk.
2. **The OS-side 50 000-row subsearch cap disappears.** Path A returns
   2 rows out of ~50 expected because the cap silently drops 96 % of
   the aggregated groups. Path B returns 1197 rows — the complete
   answer.

Federation adds ~100-200 ms of JDBC transport + OS-side glue overhead.
That's negligible against a 30 s baseline.

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

# Appendix — in-progress optimization

`SideInputInListRule` pushes the bounded left side's key set as
`WHERE parent_asin IN (?)` to ClickHouse, leveraging CH's primary-key
index to skip >99 % of the fact table for additional 5-10× gains on
bounded-left joins. The optimization is spec'd and implemented; a
runtime binder bug in the current build causes the array parameter to
resolve to `NULL` at execution (documented in
`docs/dev/bug-in-list-pushdown-null-binding.md`). Not a blocker for
the core federation value shown above — the ~50× / correctness story
already stands without it.

---

*Reproduction scripts: `docs/dev/demo/path-a/` and
`docs/dev/demo/path-bc/`. Bug repro: `docs/dev/bug-in-list-pushdown-null-binding.md`.*
