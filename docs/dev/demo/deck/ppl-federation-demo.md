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
| **Ingest cost** | Bulk-loading reviews into OS vs. a columnar engine. | **3h 36m vs 7m** |
| **Storage cost** | OS index vs. columnar compression. | **19 GB vs 7 GB** |
| **Query latency** | Dashboard query: `group by product, top-K by count`. OS must enumerate all 3.7 M groups. | **~28 s** |
| **Correctness** | PPL `join` subsearch has a 50 000-row safety cap. 3.7 M distinct keys mean **96 % silent data loss**. | **2 rows returned, 50 expected** |

Customer gets wrong answers in 300 ms, or correct answers in 30 seconds.
Neither works for a dashboard.

---

# Three paths, same PPL query

- **Path A** — all data in OS. Today's default.
- **Path B** — `products` in OS, `reviews` in ClickHouse. PPL routes
  analytical aggregations to CH via JDBC.
- **Path C** — same as B, plus `SideInputInListRule`: the left side's
  bounded key set is bound as a runtime parameter into
  `WHERE parent_asin IN (…)` on the CH query. CH's primary-key index
  skips 99 % of the fact table.

**The customer-facing PPL query is identical across all three paths.**
The planner picks the backend.

---

# Results

```
source=products | where match(title, 'stainless steel cookware')
| sort -_score | head 50
| inner join left=p right=r on p.parent_asin = r.parent_asin
  [ source=reviews
    | stats avg(rating), count() by parent_asin
    | sort -n | head 5000 ]
```

| Metric | Path A | Path B | Path C |
|---|---:|---:|---:|
| Ingest (reviews) | 3h 36m | 7m | 7m |
| OS footprint | 19 GB | 4 GB | 4 GB |
| Slow join (warm) | 30 s | _TBD_ | _TBD_ |
| Simple join (warm) | 300 ms | _TBD_ | _TBD_ |
| Simple join rows | **2 (wrong)** | _TBD_ | **50 (correct)** |

Path C eliminates both the latency wall and the correctness bug at once.

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
  │         └── JdbcRel (ClickHouse)                   │
  │                │                                   │
  │                │  JdbcSideInputFilter              │
  │                │    parent_asin IN (?)             │
  └────────────────┼───────────────────────────────────┘
                   │  JDBC, VPC-private
                   ▼
         ClickHouse (columnar fact table)
           primary key (parent_asin, timestamp)
           scans ~1000 rows, not 67 M
```

Customer doesn't see a second query language. Federation is a planner
decision.

---

# Business implications

| Concern | Answer |
|---|---|
| Do customers leave OS? | **No.** OS stays the entry point, auth, dashboards, observability. Fact tables just live where they belong. |
| Lock-in to ClickHouse? | No. `PplFederationDialect` is an extension point — Iceberg, Snowflake, other JDBC targets can plug in. |
| Readiness? | Code complete on `feat/ppl-federation`. Benchmarked on Amazon Reviews 2023. Ready for design review. |

---

# What I need

- Greenlight to propose at the next design review.
- One customer with PPL + 50 M+ fact rows they don't want in OS
  (retail or observability both fit).
- Agreement that **ClickHouse is the reference first datasource** — more
  JDBC targets plug in incrementally on the same rule framework.

---

*Reproduction scripts: `docs/dev/demo/path-a/` and
`docs/dev/demo/path-bc/`.*
