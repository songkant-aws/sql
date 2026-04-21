# PPL → ClickHouse Pushdown Coverage Expansion Design

**Status:** Approved for implementation (2026-04-21)
**Base branch:** `feat/ppl-federation`
**Implementation branch:** `feat/ppl-federation` (continuation — no new branch, matches `feat/clickhouse-datasource` extension rule)

## Goal

Close the gap between PPL queries that OpenSearch natively supports and what gets physically pushed into ClickHouse. Today, many PPL operators that *lower* into standard Calcite `RelNode`s (plain `Project`, `Filter`, `Aggregate`, `Project(RexOver)`) still fall back to the Enumerable calling convention purely because `ClickHouseSqlDialect.supportsFunction` / `supportsAggregateFunction` whitelist is too narrow. The plan: expand the whitelist, rewrite a handful of PPL-specific UDFs/UDAFs to CH-native equivalents, and guard downgrades so no currently-green query regresses.

## Problem Statement

### Current behaviour

- `ClickHouseSqlDialect.supportsFunction` accepts only: `+ − × ÷ %`, comparisons, `CAST`, `COALESCE`, `CASE`, `LIKE`, `SUBSTRING`, `LOWER`, `UPPER`, `LENGTH`, `TRIM`, `CONCAT`, `DATE_TRUNC`, and the five basic aggregates.
- `ClickHouseSqlDialect.supportsAggregateFunction` accepts only `COUNT / SUM / AVG / MIN / MAX`.
- Window functions are *not* structurally blocked (`RexOver` rides inside `LogicalProject`; Calcite's `JdbcProjectRule` gates on `dialect.supportsWindowFunctions() || !project.containsOver()`, and our dialect inherits the default `true`). They fail to push down purely because `ROW_NUMBER / RANK / DENSE_RANK / LAG / LEAD / ...` are not listed in `supportsFunction`.
- PPL-specific UDFs (`SPAN_BUCKET`, `PERCENTILE_APPROX`, `DATE_FORMAT`, `DATE_ADD`, ...) have no mapping to CH SQL, so any plan that references them can only execute in Enumerable.

### Why this is wrong

- Operators like `eval round(x,2)`, `where regex_contains(...)`, `stats stddev_pop(x)`, `stats percentile_approx(x, 0.9)`, `| rare`, `| top`, `| dedup`, `| eventstats`, `| trendline`, `| timechart`, `| bin span=1h`, and pure-projection `| appendcol` all produce logical plans that are *structurally pushable* but run locally because of the whitelist gap.
- The cost delta is not "slightly slower" — it is "ship the full CH table to the JVM and compute there", which defeats the datasource.

### Why this matters now

Feedback from early adopters: the IN-list pushdown that just landed fixes the worst-case correctness bug, but the second-worst issue is "most of my dashboard queries still scan the whole table." Expanding pushdown coverage is the next lever.

## Non-Goals

- New PPL operators or grammar changes.
- Non-ClickHouse JDBC targets (architecture stays extension-friendly; first release tests CH only).
- CBO / statistics-driven choice between pushdown and local.
- Pushdown of `| appendcol` branches that introduce `CorrelatedJoin` / FULL JOIN — those stay in Enumerable this release.
- Pushdown of `| streamstats` reset/correlated branches — same reason.
- Rewriting the Calcite convention system. We keep the existing hybrid HEP-then-Volcano flow.

## High-Level Design

### Three tiers

**Tier 1 — Whitelist expansion (dialect-only, no code-gen change).**
Every CH-native scalar / aggregate / window operator whose Calcite `SqlOperator` already unparses to syntax CH accepts. Pure whitelist edit + `supportsFunction` / `supportsAggregateFunction` additions. No new classes.

**Tier 2 — Functional rewrites (dialect override + HEP rules).**
PPL operators with a semantic equivalent in CH but different SQL-gen. Two sub-strategies:

- **Dialect override (`unparseCall`)** when the rewrite is a 1:1 syntactic substitution — preserves RelNode identity, runs at SQL-gen time.
- **HEP rewrite rule** when the rewrite restructures the logical tree (e.g. `Aggregate(PERCENTILE_APPROX(x,p))` → `Aggregate(CH_QUANTILE_AGG(p)(x))` with `p` lifted into the aggregate function identity).

**Tier 3 — Convention-guarded defensive hardening.**
If plan tests reveal that Calcite's `PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW` rule introduces a `LogicalWindow` alternative that wins cost and breaks pushdown for a currently-green query, add a CH-convention-scoped rule removal. Load-bearing: only activate after empirical evidence, never unconditionally.

### Scope boundary

- **In scope:** operators that lower to `Project / Filter / Aggregate / Sort / Project(RexOver)` and whose argument types are CH-compatible.
- **Out of scope:** operators that lower to `Correlate`, FULL JOIN, `LogicalWindow` with partition-reset semantics, or require CH subquery syntax we have not yet mapped.

## Components

### File inventory

**Modify:**

- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java`
  - Expand `supportsFunction` switch (Tier 1 additions).
  - Expand `supportsAggregateFunction` (Tier 1 aggregate additions + Tier 2 hybrid UDAF whitelist).
  - Delegate non-trivial cases to `ClickHouseDateTimeUnparser` and `ClickHouseAggregateUnparser` via `unparseCall` override.

- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/federation/CalciteFederationRegistration.java`
  - Extend idempotent registration to also register Tier-2 HEP rules through the HEP extension hook.

- `core/src/main/java/org/opensearch/sql/calcite/utils/CalciteToolsHelper.java`
  - Add `registerHepRule(RelOptRule)` + `getExtraHepRules()` hook mirroring the existing `registerVolcanoRule`. Plug extra rules into `HEP_PROGRAM` at build time.

**Create (clickhouse module):**

- `clickhouse/.../pushdown/ClickHouseDateTimeUnparser.java`
  Single source of truth for Tier-2 datetime mapping. Holds
  `Map<SqlOperator, UnparseHandler>` keyed on the PPL / Calcite op. `ClickHouseSqlDialect.supportsFunction` returns `true` iff the operator is in the map; `unparseCall` delegates to the handler. This keeps the whitelist and the SQL-gen in lockstep — you cannot accidentally approve an operator that has no handler.

- `clickhouse/.../pushdown/ClickHouseAggregateUnparser.java`
  Dialect-level unparse handlers for Tier-2 aggregates whose CH name differs from the PPL / Calcite name (`DISTINCT_COUNT_APPROX → uniq`, `ARGMIN → argMin`, `ARGMAX → argMax`). Each handler is a pure syntactic rename; no tree restructuring.

- `clickhouse/.../pushdown/SpanBucketToStdOpRule.java`
  HEP rule (`RelRule<Config>`). Matches `Project` containing a `RexCall` of PPL `SPAN_BUCKET(col, n, unit)` as a scalar in Project.
    - If `unit` is numeric (no time semantics): rewrite to `FLOOR(col / n) * n` (unconditional — standard SQL, semantically equivalent).
    - If `unit` is a time unit (`day / hour / minute / second / week / month / year`) and `n` is a literal: rewrite to `toStartOfInterval(col, INTERVAL n unit)` **only when the Project's convention is CH-bound** (CH-guarded).
  Falls through (no match) for any param that isn't a literal, or unsupported unit.

- `clickhouse/.../pushdown/PplUdafToChAggRule.java`
  HEP rule. Matches `Aggregate` whose `AggregateCall` is `PERCENTILE_APPROX(x, p)` with `p` a `RexLiteral`. Replaces with a custom `SqlAggFunction` (`CH_QUANTILE_AGG`) whose `unparse` emits the curried `quantile(p)(x)` form. CH-convention-guarded.

**Tests (modify):**

- `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/...` — plan tests per new mapping
- `integ-test/src/test/java/org/opensearch/sql/ppl/ClickHousePushdownIT.java` — extend with the 26 existing pushdown assertions plus new coverage per tier
- No changes to `ClickHouseFederationIT` — the four federation ITs stay green as a regression guardrail.

### Data flow

```
PPL query
  │
  ▼
CalciteRelNodeVisitor  ──►  LogicalProject / LogicalFilter / LogicalAggregate /
                            LogicalProject(RexOver)
  │
  ▼
HEP (CalciteToolsHelper.HEP_PROGRAM)
    ├── existing rules (FilterMergeRule, PPLSimplifyDedupRule, BoundedJoinHintRule)
    └── extra rules (from CalciteFederationRegistration):
         ├── SpanBucketToStdOpRule
         └── PplUdafToChAggRule
  │
  ▼
Volcano (JDBC convention chosen for CH-bound subtrees)
    ├── JdbcProjectRule    — gated on dialect.supportsWindowFunctions() || !containsOver()
    ├── JdbcFilterRule     — gated on every RexCall satisfying dialect.supportsFunction(...)
    └── JdbcAggregateRule  — gated on every AggregateCall satisfying dialect.supportsAggregateFunction(...)
  │
  ▼
SqlImplementor.unparse ──► ClickHouseSqlDialect.unparseCall
                            delegates to {DateTime, Aggregate} unparsers when matched
  │
  ▼
ClickHouse SQL over JDBC
```

## Tier-by-tier catalog

### Tier 1 — Whitelist additions

**Scalar (`supportsFunction`):**

| Category | Ops |
|---|---|
| Math | `ABS`, `CEIL`, `FLOOR`, `SQRT`, `EXP`, `LN`, `LOG10`, `POWER`, `ROUND`, `MOD`, `SIGN`, `SIN`, `COS`, `TAN`, `ATAN`, `ATAN2` |
| String | `POSITION`, `CHAR_LENGTH`, `REPLACE`, `REVERSE` |
| Predicate | `IS_TRUE`, `IS_FALSE`, `SEARCH`, `IN` (for cases where it survives the IN-list pushdown gate) |
| Window analytics | `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `NTH_VALUE`, `FIRST_VALUE`, `LAST_VALUE` |

Rationale: Each of these unparses to syntax CH already accepts (`abs()`, `ceil()`, `position()`, `row_number() OVER (...)`, etc.). Zero code-gen changes needed; just lift the veto.

**Aggregate (`supportsAggregateFunction`):**

| Category | Ops |
|---|---|
| Statistical | `STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP` |

CH accepts these names natively.

### Tier 2 — Functional rewrites

**2.1 DateTime (dialect override, via `ClickHouseDateTimeUnparser`).**

Scope: "high-frequency 9" datetime functions most common in dashboard queries.

| PPL / Calcite op | CH target | Notes |
|---|---|---|
| `DATE_FORMAT(expr, fmt)` | `formatDateTime(expr, fmt)` | fmt string left as-is (CH and MySQL share the `%Y/%m/%d` family) |
| `DATE_ADD(expr, INTERVAL n unit)` | `addDays/addHours/...(expr, n)` | unit → CH function name table |
| `DATE_SUB(expr, INTERVAL n unit)` | `subtractDays/...(expr, n)` | symmetric |
| `YEAR(expr)` | `toYear(expr)` | |
| `MONTH(expr)` | `toMonth(expr)` | |
| `DAY(expr)` / `DAYOFMONTH(expr)` | `toDayOfMonth(expr)` | |
| `HOUR(expr)` | `toHour(expr)` | |
| `MINUTE(expr)` | `toMinute(expr)` | |
| `SECOND(expr)` | `toSecond(expr)` | |
| `NOW()` | `now()` | |
| `UNIX_TIMESTAMP(expr)` | `toUnixTimestamp(expr)` | |
| `FROM_UNIXTIME(expr)` | `fromUnixTimestamp(expr)` | |

`DATE_TRUNC` is already in Tier-0 (was in the pre-existing whitelist, CH has `date_trunc`).

**2.2 Span / Bin (HEP rule `SpanBucketToStdOpRule`).**

Two-step scope:

- **Unconditional** (runs regardless of target convention):
  `SPAN_BUCKET(col, n_literal, <numeric sentinel>)` → `FLOOR(col / n) * n`. Semantically equivalent standard SQL. Net-zero for non-CH targets.
- **CH-convention-guarded**:
  `SPAN_BUCKET(col, n_literal, <time unit>)` → `toStartOfInterval(col, INTERVAL n unit)`. Only emitted when the Project is already bound to the JDBC/CH convention; otherwise leave the UDF alone for Enumerable to evaluate.

Guard logic: walk the rel's convention chain; if the nearest `Convention` is not `JdbcConvention` with CH's dialect, no-op.

Unsupported unit → no match (UDF stays → Enumerable handles it → no regression).

**2.3 UDAF hybrid (dialect override + HEP rule).**

Dialect-override path (simple rename — `ClickHouseAggregateUnparser`):

| PPL UDAF | CH target | Strategy |
|---|---|---|
| `DISTINCT_COUNT_APPROX(x)` | `uniq(x)` | Whitelist + unparse rename |

(Note: ARGMIN / ARGMAX are *not currently registered* as PPL UDAFs in this codebase, so they are deferred. Add only when PPL exposes them.)

HEP-rewrite path (curried syntax — `PplUdafToChAggRule`):

| PPL UDAF | CH target | Strategy |
|---|---|---|
| `PERCENTILE_APPROX(x, p)` | `quantile(p)(x)` | Rewrite to custom `CH_QUANTILE_AGG` agg fn whose `unparse` emits `quantile(p)(x)`. Requires `p` as `RexLiteral`. |

Guard for `PplUdafToChAggRule`: CH-convention only (the custom `CH_QUANTILE_AGG` only knows how to unparse to CH).

### Tier 3 — Defensive convention guard

Calcite registers `CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW` by default, which adds a `LogicalWindow` alternative to any `Project(RexOver)`. That alternative is *non-destructive* (the original Project stays in the RelSet), but under some cost metrics Volcano may pick `LogicalWindow` + Enumerable over the JDBC alternative.

**Trigger:** A plan test in Tier-1 or Tier-2 fails with the expected pushdown path because Volcano chose `LogicalWindow`.

**Response:** Register a CH-convention-guarded rule that narrows `PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW` matching — or, if narrower surgery isn't feasible, remove it from the planner *only* inside the CH-bound subtree. Never globally.

**Default:** Do not implement unless tests demand it. YAGNI.

## Error Handling & Graceful Downgrade

The over-arching rule: **every Tier must fail closed to "run in Enumerable"**, not "throw a hard error."

- **Tier 1 failure mode:** impossible — whitelist is strictly additive. If CH rejects the generated SQL at runtime (server version mismatch, edge-case argument type), that is a bug and is caught by IT.
- **Tier 2 dialect-override failure:** `supportsFunction` returns `true` only when the unparse handler is present. If the handler throws during unparse, JDBC code-gen aborts and Volcano falls back to Enumerable through the existing error path.
- **Tier 2 HEP rule failure:** rules match on specific literal / type patterns. Any non-matching instance simply doesn't trigger the rewrite; the PPL UDF survives into Enumerable where it already has a UDF registration. No regression.
- **Tier 3 rule removal:** only scoped to CH convention. Other JDBC dialects and Enumerable paths are unaffected.

**Observability:** We do *not* add new metrics in this release. The existing `query.log` / `explain` output is sufficient evidence for per-query verification, and the IT suite is the landing gate.

## Testing Strategy

### Plan-level tests

For every Tier-1 whitelist addition: a plan test under `clickhouse/src/test/java/...` that builds a PPL query → Calcite plan and asserts the resulting `RelNode` tree is entirely CH-convention (no `EnumerableProject` / `EnumerableFilter` / `EnumerableAggregate` above the JDBC root). One test per operator family, not per operator, to keep the suite lean.

For every Tier-2 rewrite: a plan test asserting
1. The PPL op is present in the raw plan.
2. After HEP + Volcano, the final tree contains the expected CH-native call and the PPL op is gone.
3. The expected SQL string is produced at unparse time.

### Integration tests

Extend `ClickHousePushdownIT.java` with one IT per Tier-1 category (math / string / predicate / window / aggregate) and one IT per Tier-2 family (datetime / span / UDAF-rename / UDAF-rewrite). Each IT:

1. Issues the PPL query.
2. Captures the CH query log (via the existing `ClickHouseQueryLog` harness).
3. Asserts the generated SQL contains the CH-native operator.
4. Asserts the result matches expected.

### Regression guardrails

- All 26 existing pushdown ITs (`ClickHousePushdownIT`) MUST stay green.
- All 4 existing federation ITs (`ClickHouseFederationIT`) MUST stay green.
- Core PPL plan tests MUST stay green (no unintended fallout from `registerHepRule` hook).

### Unit tests

- `ClickHouseDateTimeUnparser` — one test per function, covering nominal + one edge case (e.g., `DATE_ADD` with non-literal interval → handler refuses → falls back).
- `ClickHouseAggregateUnparser` — one test per rename, covering arg order swap for argMin/argMax.
- `SpanBucketToStdOpRule` — matrix: {literal/non-literal} × {numeric/time/unknown unit} = 6 cases.
- `PplUdafToChAggRule` — matrix: {literal p / non-literal p} × {CH convention / non-CH convention} = 4 cases.

## Task Decomposition Overview

The implementation plan (separate file) decomposes into these logical tasks, each TDD-driven, each independently committable:

1. **HEP hook.** Add `registerHepRule` / `getExtraHepRules` to `CalciteToolsHelper`. Integration test: register a no-op rule, verify it's invoked.
2. **Tier-1 scalar math.** Whitelist + plan test + IT.
3. **Tier-1 scalar string.** Whitelist + plan test + IT.
4. **Tier-1 predicate (SEARCH / IS_TRUE / IS_FALSE / REGEXP_CONTAINS).** Whitelist + plan test + IT. Note: `REGEXP_CONTAINS` → CH `match()` requires Tier-2 unparse, so split into 4a (simple predicates) and 4b (regex).
5. **Tier-1 window analytics.** Whitelist + plan test (single-window + multi-window) + IT.
6. **Tier-1 aggregate stat (STDDEV / VAR).** Whitelist + plan test + IT.
7. **Tier-2 DateTimeUnparser.** New class + dialect wiring + per-function unit test + plan test + IT. Landed in sub-tasks per function family (format/add-sub/extract/epoch) to keep commits small.
8. **Tier-2 SpanBucketToStdOpRule** (unconditional numeric branch). HEP rule + unit test + plan test + IT.
9. **Tier-2 SpanBucketToStdOpRule** (CH-guarded time branch). Extend rule + convention guard + unit + plan + IT.
10. **Tier-2 AggregateUnparser (uniq/argMin/argMax).** New class + wiring + unit + plan + IT.
11. **Tier-2 PplUdafToChAggRule (quantile).** New custom `SqlAggFunction` + HEP rule + unit + plan + IT.
12. **Tier-3 conditional defensive guard.** Only if a prior task's plan test shows `LogicalWindow` winning cost over JDBC alternative. Otherwise skipped.
13. **Final regression sweep.** Run full `ClickHousePushdownIT` + `ClickHouseFederationIT`. Update doc.

Each task lives on `feat/ppl-federation`. Each task commits independently with DCO sign-off.

## Trade-offs

- **Whitelist over blacklist.** More verbose to extend, but safer: an unrecognised op defaults to "don't push down," preserving the existing behaviour as the floor.
- **HEP before Volcano, not during.** HEP lets us do purely logical rewrites (FLOOR/times transform) without reasoning about convention. CH-convention-guarded rewrites still fit because the guard reads the convention attribute at match time — HEP sees it set by the time the rule fires (post prior HEP passes, pre Volcano).
- **Hybrid UDAF (dialect override + HEP) instead of uniform HEP.** Dialect override is ~5 lines per rename and needs no new `SqlAggFunction`. Reserving HEP for structurally-different rewrites (curried, multi-call) keeps each tool used for what it's best at.
- **Tier 3 as reactive, not proactive.** `PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW` is not always harmful (it provides an alternative, doesn't replace). Removing it preemptively would create cost-model surprises. Better to add the guard only when a specific test demands it.

## Open Questions

None. All prior open decisions (scope C, pure-projection-only complex branches, hybrid UDAF, two-step HEP scoping) resolved during brainstorming.

## Success Criteria

- Every PPL query in the extended IT suite produces a generated CH SQL that contains the pushed-down operator (verified via `ClickHouseQueryLog`).
- All 26 existing `ClickHousePushdownIT` assertions stay green.
- All 4 existing `ClickHouseFederationIT` assertions stay green.
- Final whole-branch code review returns "Ready to merge."
- Branch pushes cleanly to `origin/feat/ppl-federation`.
