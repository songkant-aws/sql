---
title: "PPL Federation — Project Report"
author: "Songkan Tang"
date: "2026-04-28"
geometry: margin=0.8in
fontsize: 10pt
colorlinks: true
linkcolor: blue
---

# PPL Federation — Project Report

**Branch:** `feat/ppl-federation`  · **Scope:** OpenSearch SQL plugin  ·
**Status:** code complete, benchmarked end-to-end on AWS EC2.

## 1. What this project does

PPL Federation extends OpenSearch's Piped Processing Language (PPL)
so a single query can span OpenSearch and an external analytical
engine — initially ClickHouse, over JDBC. The customer writes one
PPL statement; the planner decides which subtree runs on OpenSearch
(search, relevance, k-NN), which runs on ClickHouse (fact-table
aggregation, long-range scans), and how the two sides compose. No
application-level stitching, no ETL between the two stores.

The delivered pieces are:

1. A ClickHouse `StorageEngine` + `Table` implementation registered
   as a catalog (e.g. `ch.fed.reviews`), so `source=ch.fed.reviews`
   is valid PPL.
2. A set of Calcite planner rules — most importantly
   `SideInputInListRule` — that recognize a bounded search-side
   result and rewrite the plan to push it into the ClickHouse side
   as a runtime-bound `WHERE parent_asin IN (?0)` filter.
3. Plumbing for the Calcite executor (`JdbcSideInputFilter`,
   `BoundedJoinHintRule`) so the rewrite works under any join
   physical operator: `EnumerableHashJoin`, `EnumerableMergeJoin`,
   or `EnumerableNestedLoopJoin`.
4. Integration tests against a real ClickHouse container, including
   the end-to-end IN-list pushdown verified via ClickHouse's
   `system.query_log`.

## 2. Problem being solved

OpenSearch customers routinely outgrow a single engine:
observability shops add orders/clickstream, retail shops add reviews
and long-range analytics, every customer eventually wants RAG over
their own fact tables. The current options are:

- **Duplicate fact data into OpenSearch** — pays an **ETL tax that
  scales with the data**. Re-ingest is hours, storage inflates
  because inverted index targets strings, and aggregations on
  millions of distinct keys pay a per-bucket cost that OpenSearch's
  composite-aggregation path was never designed for.
- **Stitch results in application code** — pushes join semantics
  into every caller, loses the declarative query plan, and doubles
  the surface area the customer has to operate.

PPL Federation replaces both with **same entry point, right engine
for each workload**. Measured on Amazon Reviews 2023 (3.7 M
products + 67 M reviews, `r6i.xlarge` + `r6i.2xlarge`, OS 3.6.0 +
CH 24.8):

| Metric                         | Path A (OS-only) | Path C (federation) | Delta |
|--------------------------------|-----------------:|--------------------:|------:|
| Fact ingest (67 M)             | 2 h 50 m         | 4 m 20 s            | 40×   |
| Fact storage                   | 19.3 GB          | 7.24 GB             | 2.7×  |
| End-to-end warm median         | 30 s             | ~170 ms             | 176×  |
| Rows scanned at fact engine    | 67 M             | 360 K               | 187×  |
| Correctness (top-K rows)       | 2 of expected 50 | complete (1197)     | —     |

The last row matters as much as the latency. PPL's join subsearch
bounds output to 50 000 rows — correct-by-design, because that cap
is what prevents the engine from running out of memory on unbounded
joins. At fact-table scale that protection becomes a ceiling: two
rows come back where fifty would answer the business question.
Federation removes the ceiling by moving the fact side to an engine
built for that shape of work.

## 3. Requirements analysis

**Functional requirements.**

- *Single entry point:* the customer writes PPL, not a DSL per
  backend. `source=products | ... | inner join [ source=ch.fed.reviews
  | ... ]` is a valid query.
- *Planner-driven backend selection:* search, relevance, and k-NN
  stay on OpenSearch; fact scans and aggregations run on ClickHouse.
  The rewrite is visible in `explain` but invisible in the PPL text.
- *Runtime-bound IN-list pushdown:* for queries whose search side
  is bounded (e.g. `head 50`), the actual key values are drained at
  execution time and inlined into the remote SQL, so MergeTree's
  primary-key index can skip partitions.
- *JDBC-dialect agnostic:* Iceberg, Snowflake, and other JDBC
  targets plug in by implementing the dialect + catalog, without
  touching planner rules.

**Non-functional requirements.**

- *Correctness over cleverness.* If a rule cannot safely push a
  predicate, the planner must fall through to a non-pushdown plan
  rather than returning wrong rows. Every rule is gated on bounded
  inputs and verified types.
- *No regressions for OS-only queries.* A query that never references
  a federation catalog must produce a plan indistinguishable from
  the pre-federation plan. The rule set is additive.
- *Operable without a sidecar.* The JDBC driver runs inside the
  OpenSearch JVM; datasource credentials use the standard encrypted
  datasource settings (master-key-backed). There is no separate
  service to run or monitor.
- *Observable.* Every remote query is logged in ClickHouse's
  `system.query_log`, and `explain` on the PPL side shows
  `JdbcFilter(ARRAY_IN($0, ?0))` when the pushdown fires — so the
  operator can confirm the plan is doing what they expect.

**Out of scope for this cut.** Write-path federation (INSERT /
UPDATE across engines); cross-engine transactions; automatic
placement recommendations based on workload statistics.

## 4. How AI accelerated the work

AI (Claude Code, running this repository in an interactive session)
was used as an **engineering pair**, not a code generator. Three
places where the speedup was tangible:

- **Benchmark design and scripting.** The demo required two AWS
  nodes, a reproducible Amazon Reviews ingest on both OS 3.6.0 and
  ClickHouse 24.8, and end-to-end latency measurements. Claude
  drafted the provisioning scripts (EC2 instance specs, security
  groups, cross-node networking, ingest scripts in Python + bash),
  debugged a handful of real issues as they surfaced — a wrong
  JDBC URL prefix, a master-key length bug (44-char base64 vs 32
  ASCII), an `IN (NULL)` edge case, a silent `verified_purchase`
  type mismatch, macOS bash 3.2 incompatibilities in the uploader
  — and kept the scripts idempotent so re-runs were safe.
- **Planner-rule debugging.** When the rule fired but the join got
  swapped to the wrong side, AI helped triangulate the cause by
  reading `SideInputInListRule`, the `explain` output, and the
  CH `system.query_log` side-by-side. That narrowed the issue
  to `Volcano` picking a different physical join, which was then
  handed back to a colleague to widen the rule's operand from a
  specific join class to the generic `Join.class`.
- **Stakeholder-ready communication.** The 4-minute leadership
  pitch went through multiple framings before landing on "same
  entry point, right engine for each workload". AI held the full
  benchmark table + narrative context in view, edited the reveal.js
  deck in place, caught layout regressions (PDF vs browser
  centering, the `html.reveal-print` gate being different from
  `@media print`), and kept the long-form brief, the 4-min
  transcript, and the slide speaker notes consistent across
  revisions.

The consistent pattern: AI compressed the **context-assembly**
cost — pulling the right file, the right rule, the right log line,
the right customer framing, into one place — so engineering
judgment could be applied at the decision points instead of at the
lookup points.

## 5. Status and next steps

Code complete on `feat/ppl-federation`. Benchmarks reproduced
end-to-end (see `docs/dev/demo/deck/ppl-federation-demo.md` for
methodology + raw numbers). Integration tests passing against a
real ClickHouse container. PR ready when requested.

Next steps, in priority order:

1. **Engage a design partner** in retail or observability to
   pressure-test the pattern against a non-synthetic workload.
2. **Add Iceberg as the second JDBC target** — validates the
   dialect-agnostic claim and unlocks a much larger customer
   segment than ClickHouse alone.
3. **Extend the rule set** to cover the remaining shapes
   (left-side unbounded with a small right side, correlated
   subqueries) so more PPL patterns get the pushdown automatically.
