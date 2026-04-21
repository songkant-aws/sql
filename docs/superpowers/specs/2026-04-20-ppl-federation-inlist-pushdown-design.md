# PPL Federation: Sideways IN-List Pushdown Design

**Status:** Approved for implementation (2026-04-20)
**Base branch:** `feat/clickhouse-datasource`
**Implementation branch:** `feat/ppl-federation`

## Goal

Make PPL `join` across OpenSearch (left) and ClickHouse (right) correct and performant by pushing the left side's bounded join-key set down to ClickHouse as a parameterised `WHERE key IN (...)` filter. Federation today is functionally allowed but silently truncates the right side at 50 000 rows (`plugins.ppl.join.subsearch_maxout`), which is both a correctness risk for large fact tables and a missed performance opportunity.

## Problem Statement

### Current behaviour

A PPL query like

```
source=os.docs | where knn(embedding, [...], k=100)
| inner join left=d right=f on d.user_id = f.user_id
    [ source=ch.events | where ts > now()-7d
      | stats sum(gmv) as gmv by user_id ]
```

executes as:

1. Left side streams from OpenSearch via `search_after`, bounded by `QUERY_SIZE_LIMIT=10 000`.
2. Right side is fully executed on ClickHouse (with CH-side aggregation pushdown already in place from `feat/clickhouse-datasource`).
3. Right result is hard-capped at 50 000 rows on ingest (`LogicalSystemLimit(JOIN_SUBSEARCH_MAXOUT)`).
4. Calcite `EnumerableHashJoin` builds a hash table over the (capped) right side; left side probes.

### Why this is wrong

- If the right side's `stats ... by user_id` produces >50 000 groups, **the join silently drops groups**. The user has no way to know.
- The right side scans the *entire* 7-day fact table, aggregates, then ships up to 50 000 rows — wasted CH compute and network.
- This is the case even though the left side has already reduced itself to ≤100 rows via `knn k=100`, and the user is fundamentally only asking for aggregates on those 100 users.

### Why this matters now

RAG-style queries are the canonical OS+CH federation pattern: semantic top-K on OS, factual enrichment on CH. Without sideways information passing, the canonical pattern is broken by default.

## Non-Goals

- Non-ClickHouse JDBC targets (architecture permits extension; first release does not test or guarantee).
- Statistics collection or cost-based optimisation.
- Symmetric optimisation (right-side bounded → IN-list back to left).
- Automatic aggregation insertion on the right when the user forgot it.
- A new `enrich` PPL command (we stay aligned with Splunk SPL `lookup` semantics).
- Changes to grammar, analyzer, or existing system-limit logic.

## High-Level Design

### Pipeline

```
PPL source → AST → Logical plan (existing)
    ↓
HEP phase 1: standard PPL logical rewrites (existing)
    ↓
HEP phase 2: BoundedJoinHintRule  ← new
    ↓
Volcano: standard Jdbc*Rule converts right subtree to JdbcRel (existing)
    ↓
Volcano: SideInputInListRule      ← new
    ↓
Physical plan with JdbcSideInputFilter on right side
    ↓
Runtime: SideInputBinder drains left, binds array parameter, executes
```

### Trigger conditions (all must hold)

1. The logical plan contains a `Join` (any inner/left/right/semi) with an equality condition on one or more key columns.
2. The left subtree has a *statically provable* upper bound `N` on row count (see *Bounded cardinality* below).
3. `N ≤ dialect.getInListPushdownThreshold()` (ClickHouse: 10 000; default: 1 000).
4. The right subtree, after standard Calcite rules, is fully inside a single `JdbcRel` subtree (i.e., was successfully translated to pushdown).
5. The right subtree's root, after IN-list insertion, remains translatable (dialect supports `IN (array_param)`).
6. The right-side JDBC dialect declares `supportsArrayInListParam() == true` (ClickHouse: yes).

If any condition fails, the rule is a no-op and the original plan executes (with the existing 50 000-row safety cap).

### Bounded cardinality detection

`BoundedCardinalityExtractor.extract(RelNode) → Optional<Long>` returns a proven upper bound or empty. Only structurally provable bounds count — never statistics-based estimates.

Recognised patterns:

| Pattern | Bound |
|---|---|
| `Sort(fetch=N)` (from `head N`, `sort ... \| head N`, `top N`) | N |
| `LogicalSystemLimit(fetch=N)` | N |
| `Filter(PK = literal)` | 1 |
| `Filter(PK IN (a,b,c))` | 3 |
| `Aggregate(...)` | *not used in v1* (group-by cardinality unknown) |

Recursion rules:

- `Project`, `Calc`, `Filter (non-PK)` are transparent — recurse into input, bound unchanged.
- `Sort(fetch=N)` caps at N regardless of child.
- `Aggregate`, `Join`, `Union`, `Values`, `TableScan` return empty (no bound).

PK recognition: initially `_id` on OpenSearch tables only. Extension mechanism: `RelOptTable.getStatistic().getKeys()` — if set, honour it. Documented as extension point; no generic schema-level PK annotation in v1.

### New logical rule: `BoundedJoinHintRule` (HEP)

**Match:** `Join(?, ?)` without an existing `bounded_left` hint.

**Action:**
1. Call `BoundedCardinalityExtractor.extract(join.getLeft())`.
2. If empty, no-op.
3. If present, compare against the *right-side* dialect's threshold (resolved by walking the right subtree for a `JdbcRel`). If no JDBC right, no-op.
4. Attach `RelHint("bounded_left", size=N)` to the `Join` node.

Rationale for HEP (not Volcano): the hint is convention-free metadata; we want it set exactly once, deterministically, before convention conversion.

### New physical rule: `SideInputInListRule` (Volcano)

**Match:**
```
EnumerableHashJoin[hint=bounded_left]
  (? ,
   JdbcToEnumerableConverter(jdbcTree))
```

**Action:**
1. Extract the right-side join key column index (`rightKeyIdx`).
2. Walk `jdbcTree` to find the deepest `JdbcTableScan` or `JdbcFilter` on the base table. Insert a `JdbcSideInputFilter(child, keyCol=rightKeyIdx, dynamicParam=?DYN)` as the new child. The surrounding `JdbcAggregate`, `JdbcSort`, etc. sit *above* it and aggregate only matched rows.
3. Rebuild the right subtree with the new filter. The join itself is unchanged.
4. Register a runtime hook on the join so that on execution, the left side is drained and `?DYN` is bound with the distinct key array.

Semantics: `JdbcSideInputFilter` is a `Filter` node that exposes a `JdbcRel` implementation. Its SQL generation emits `WHERE <keyCol> IN (?)` where `?` is a single array-typed `PreparedStatement` parameter.

### New RelNode: `JdbcSideInputFilter`

```java
public final class JdbcSideInputFilter extends Filter implements JdbcRel {
  private final int keyColumnIndex;
  private final RexDynamicParam arrayParam;
  // standard Filter copy/equals/hashCode
  // JdbcRel.implement(JdbcImplementor) → generates "key IN (?)" SQL fragment
}
```

The condition stored in the base `Filter` is `RexCall(SqlStdOperatorTable.IN, RexInputRef(keyColumnIndex), arrayParam)`. `ClickHouseSqlDialect`'s unparse path emits this as CH-native `IN` with array parameter.

### Dialect extension

Add to `SqlDialect` (via a small interface or extension class on our side — we cannot modify Calcite core):

```java
public interface PplFederationDialect {
  long getInListPushdownThreshold();  // default 1_000
  boolean supportsArrayInListParam(); // default false
}
```

`ClickHouseSqlDialect` implements this:
- `getInListPushdownThreshold() = 10_000`
- `supportsArrayInListParam() = true`

Implemented as a side-table lookup (`Map<SqlDialect, PplFederationDialect>`) rather than subclassing, to avoid touching non-CH dialect classes.

### Runtime: `SideInputBinder`

When `SideInputInListRule` fires, it wraps the `JdbcToEnumerableConverter` with a `SideInputJdbcConverter` that:

1. On `bind(DataContext)`:
   - Obtains the left enumerable from the join.
   - Drains it with a bounded buffer (`threshold + 1` rows). If the buffer fills, **bail out**: throw a specific `SideInputBailout` exception that the join catches and falls back to default behaviour.
   - Extracts distinct values of the join key column into a `java.sql.Array`.
   - Sets the dynamic param on the underlying `JdbcEnumerable`.

2. Caches the left buffer and exposes it to the join's build phase so the left is not re-scanned.

The join operator is unchanged; it still sees two enumerables and builds a hash join. The left buffer is simply pre-materialised.

### Bail-out fallback

If `SideInputBailout` is thrown at runtime:
- The current partial execution is discarded.
- Re-plan without the `bounded_left` hint (or simply with the rule disabled for this query).
- Log at INFO: `"SideInput bail: left exceeded threshold N, falling back to standard join"`.
- The existing 50 000 cap on the right still protects correctness of the fallback path.

Implementation: a top-level `try/catch` in the query executor around the first tuple fetch, with a one-shot re-plan. Done with Calcite's `Hintable` — simply remove the hint and re-prepare.

### Warning: non-agg right side

During `SideInputInListRule`, detect whether the right `JdbcRel` subtree contains a `JdbcAggregate` at or below the insertion point. If not, log WARN: `"Federation join right side is not aggregated; fan-out risk. Consider adding | stats ... by <key>"`. Do not block; this is advisory.

## Detailed Component Specs

### File layout (new)

```
core/
  src/main/java/org/opensearch/sql/calcite/planner/logical/rules/
    BoundedCardinalityExtractor.java
    BoundedJoinHintRule.java

  src/test/java/org/opensearch/sql/calcite/planner/logical/rules/
    BoundedCardinalityExtractorTest.java
    BoundedJoinHintRuleTest.java

clickhouse/
  src/main/java/org/opensearch/sql/clickhouse/calcite/federation/
    PplFederationDialect.java
    PplFederationDialectRegistry.java
    JdbcSideInputFilter.java
    JdbcSideInputFilterConverter.java   // registers SQL-generation hook
    SideInputInListRule.java
    SideInputJdbcEnumerable.java        // runtime binding
    SideInputBailout.java

  src/main/java/org/opensearch/sql/clickhouse/calcite/
    ClickHouseSqlDialect.java           // MODIFY: implement PplFederationDialect

  src/test/java/org/opensearch/sql/clickhouse/calcite/federation/
    JdbcSideInputFilterTest.java
    SideInputInListRuleTest.java
    ClickHouseSqlDialectFederationTest.java

integ-test/
  src/test/java/org/opensearch/sql/clickhouse/
    ClickHouseFederationIT.java         // NEW
```

### Wiring

The new rules must be registered in the Calcite planner used by the PPL execution path. Specifically:

1. `BoundedJoinHintRule` added to the HEP program built in `core`'s PPL Calcite configuration (same phase as other `Logical*` rewrites — identify precise hook during Task 3 via reading `QueryPlanFactory` / `CalcitePlanContext`).
2. `SideInputInListRule` added to the Volcano rule set, registered only when the `clickhouse` module is on the classpath. Hook: the existing `ClickHouseSchemaFactory` already contributes rules during schema registration — extend the same path.

### Interface: `PplFederationDialect`

```java
public interface PplFederationDialect {
  /** Maximum IN-list size (in value count) that this dialect handles efficiently. */
  long getInListPushdownThreshold();

  /** Whether the dialect supports a single array-typed PreparedStatement parameter
   *  for `WHERE col IN (?)` semantics. */
  boolean supportsArrayInListParam();

  /** Default, used when no dialect-specific override is registered. */
  PplFederationDialect DEFAULT = new PplFederationDialect() {
    public long getInListPushdownThreshold() { return 1_000L; }
    public boolean supportsArrayInListParam() { return false; }
  };
}
```

Lookup:

```java
public final class PplFederationDialectRegistry {
  public static PplFederationDialect forDialect(SqlDialect d);
}
```

### `JdbcSideInputFilter` contract

```java
public final class JdbcSideInputFilter extends Filter implements JdbcRel {
  public static JdbcSideInputFilter create(
      RelNode input,
      int keyColumnIndex,
      RexDynamicParam arrayParam);

  public int getKeyColumnIndex();
  public RexDynamicParam getArrayParam();

  // Filter.copy — standard
  // JdbcRel.implement — emits `<keyCol> IN (?)` as a SqlNode fragment
}
```

Equality/hashCode: include `keyColumnIndex` and the param index.

### `SideInputInListRule` contract

```java
public final class SideInputInListRule extends RelRule<SideInputInListRule.Config> {
  public interface Config extends RelRule.Config { ... }
  public static final SideInputInListRule INSTANCE;

  @Override public void onMatch(RelOptRuleCall call);
}
```

Matched pattern (pseudo):
```
EnumerableHashJoin [hints contain bounded_left]
  any(left)
  JdbcToEnumerableConverter(any(right) rooted in JdbcRel)
```

### `SideInputJdbcEnumerable` contract

Wraps the default `JdbcEnumerable` produced by `JdbcToEnumerableConverter`. Runtime:

```java
public Enumerator<Object[]> enumerator() {
  // 1. Drain left with threshold+1 bounded buffer
  List<Object[]> leftRows = drainLeft(threshold + 1);
  if (leftRows.size() > threshold) throw new SideInputBailout();

  // 2. Extract distinct keys
  Object[] keys = distinctKeys(leftRows, leftKeyIdx);

  // 3. Bind JDBC array parameter
  jdbcContext.set(paramIdx, toJdbcArray(keys));

  // 4. Execute and return
  return jdbcEnumerable.enumerator();
}
```

The buffered left rows are surfaced back to the join via a thread-local/context slot so the join's build phase reuses them.

### Warning hook

Emit via `org.slf4j.Logger` at WARN in `SideInputInListRule.onMatch` when the right JdbcRel subtree has no `JdbcAggregate`. Not user-visible in the query response — logs only.

## Testing Strategy

### Unit tests

**`BoundedCardinalityExtractorTest`** — one case each:
- `Sort(fetch=100)` on a scan → 100
- `Sort(fetch=100)` on a `Sort(fetch=50)` → 50 (min)
- `Project(Filter(Sort(fetch=100)))` → 100 (transparent)
- `Filter(a=1)` on PK → 1 (requires test table with PK statistic)
- `Filter(a IN (1,2,3))` on PK → 3
- `Aggregate(count() by x)` → empty
- `TableScan` bare → empty

**`BoundedJoinHintRuleTest`**:
- Join with bounded left and CH right → hint added
- Join with bounded left but no JDBC right → no hint
- Join with unbounded left → no hint
- Join with bounded left exceeding threshold → no hint
- Re-application → idempotent

**`SideInputInListRuleTest`**:
- Matched plan → `JdbcSideInputFilter` inserted below JdbcAggregate
- Right side has no JdbcAggregate → warning logged, filter still inserted (at base)
- No hint on join → no match

**`JdbcSideInputFilterTest`**:
- `implement(JdbcImplementor)` produces SqlNode `IN (?)` on key column
- Works through `ClickHouseSqlDialect` unparse

**`ClickHouseSqlDialectFederationTest`**:
- `getInListPushdownThreshold()` = 10 000
- `supportsArrayInListParam()` = true

### Integration tests

`ClickHouseFederationIT` (extends `PPLIntegTestCase` + CH testcontainer, mirrors `ClickHousePushdownIT` pattern):

1. **`testKnnTopKJoinAgg`** — actually executes
   `source=os.docs | head 50 | join user_id [ source=ch.events | stats sum(v) as s by user_id ]`
   and asserts:
   - Correct results
   - `system.query_log` shows `WHERE user_id IN (?)` actually reached CH with exactly the left-side keys

2. **`testHeadNJoinAgg`** — as above with different N

3. **`testBailOutOverThreshold`** — left `head 15000`, exceeds CH threshold at planning → no IN-list pushdown, standard path taken, CH query_log shows no IN filter

4. **`testBailOutAtRuntime`** — synthetic case: planner allowed bounded_left but actual data > threshold at runtime → `SideInputBailout` → correct results on fallback path

5. **`testNonAggRightSideWarning`** — right side has no `stats`; log file captured; correct behaviour with 50 000 cap

6. **`testPkEqualityLeft`** — `where _id='x'` → bound=1 → IN list has 1 value

7. **`testEmptyLeft`** — left returns 0 rows → IN with empty array → right returns nothing (valid; CH handles empty IN)

### Empirical verification (mirroring `ClickHousePushdownIT` convention)

Each IT that asserts pushdown reads `system.query_log` after the PPL query and captures the actual SQL sent to CH. Expected pattern:

```
SELECT `user_id`, sum(`v`) AS `s`
FROM `events`
WHERE `user_id` IN (?)
GROUP BY `user_id`
```

with the `?` parameter array containing exactly the left-side distinct user_ids.

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Calcite rule ordering prevents `SideInputInListRule` from matching | Medium | Feature doesn't trigger | HEP for hint (deterministic); Volcano rule has zero-cost guard; add trace logging |
| `RexDynamicParam` + JDBC Array not supported by CH driver | Low | Feature incorrect | Task 1 is a pure JDBC spike proving bind works end-to-end, before any Calcite code |
| Join key column index calculation wrong under Project reorderings | Medium | Wrong data | `JdbcSideInputFilter` accepts the pre-Project column index and resolves by field *name* via RowType lookup; unit tests cover a renamed-key case |
| Left drain changes memory profile vs. current streaming | Low | Heap pressure | threshold × typical row ≈ 10 000 × 100 B = 1 MB; negligible |
| Interaction with existing `JOIN_SUBSEARCH_MAXOUT` | Medium | Double-limit confusion | Explicitly preserved as the fallback floor; test matrix includes both active simultaneously |
| `SideInputBailout` re-plan cost | Low | Latency spike on edge cases | Only fires when static bound was overly optimistic (rare); still cheaper than a failed 50k-capped join |

## Architectural Invariants

The following invariants must hold for the implementation to be correct. Future maintainers must preserve these when changing related code:

1. **`BoundedCardinalityExtractor` returns only proven upper bounds.** No estimates, no sampling, no metadata-query calls. If uncertain, return empty.

2. **`BoundedJoinHintRule` is HEP-phase; `SideInputInListRule` is Volcano-phase.** Mixing these (e.g., running the side-input rule in HEP) breaks because `JdbcRel` doesn't exist yet pre-conversion.

3. **`JdbcSideInputFilter` must be in JDBC convention.** If it lands above a `JdbcToEnumerableConverter` instead of below, SQL generation skips it and the filter silently doesn't apply.

4. **Dialect threshold is consulted at plan-time, enforced at runtime.** The runtime check must use the same threshold to avoid plan/execute mismatch.

5. **`SideInputBailout` must trigger before *any* left rows are returned to the caller.** Once the hash-join build phase has emitted a row downstream, re-planning is unsafe.

6. **The 50 000-row `JOIN_SUBSEARCH_MAXOUT` cap is retained.** IN-list pushdown is additive; removing the cap is a separate decision.

## Out of Scope for v1 (Noted for v2)

- Statistics-based bounds (extend `BoundedCardinalityExtractor` to consult `RelMetadataQuery`)
- Non-CH JDBC targets (PostgreSQL, MySQL — extension via dialect registry)
- Symmetric IN-list (right bounded → left)
- Broadcast join variant
- Cross-datasource `stats` union (append from multiple sources)

## Pushdown Verification (Empirical)

Captured from ClickHouse `system.query_log` during `ClickHouseFederationIT.testBoundedLeftJoinAggregatedCh` on 2026-04-21.

**PPL:**

```
source=fed_docs
 | head 10
 | inner join left=d right=f on d.user_id = f.user_id
     [ source=ch.fed.fed_events | stats sum(v) as s by user_id ]
```

**Environment:**

- OpenSearch side (`fed_docs`): 3 docs with `user_id ∈ {1, 3, 5}`
- ClickHouse side (`fed.fed_events`): 1000 rows spread across `user_id ∈ {1..10}`
- ClickHouse dialect threshold: 10 000
- JDBC driver: `com.clickhouse:clickhouse-jdbc:0.6.5`

**SQL observed on ClickHouse (from `system.query_log`, single `QueryFinish` entry):**

```sql
SELECT SUM(`v`) AS `s`, `user_id`
FROM `fed`.`fed_events`
WHERE `user_id` IN ([1,3,5])
GROUP BY `user_id`
LIMIT 50000
```

Confirms: IN-list pushdown fires with the three distinct left-side `user_id`s. The `JOIN_SUBSEARCH_MAXOUT=50000` floor is still appended as the outer `LIMIT`.

**Note on array-literal rendering (`IN ([1,3,5])` rather than `IN (?)`):** The prepared statement handed to the `clickhouse-jdbc` driver is genuinely `... WHERE \`user_id\` IN (?)` with a `java.sql.Array` parameter bound via `setArray(int, Array)`. clickhouse-jdbc 0.6.5 is a client-side template engine — its `SqlBasedPreparedStatement` invokes `ClickHouseValues.convertToSqlExpression(Array)` and client-side-interpolates the bound value into the SQL text as `[v1,v2,...]` before shipping to the server. The SQL stored in `system.query_log` is the post-interpolation form. The pushdown is parametric at the JDBC API boundary.

**Bailout case** (from `testBailoutWhenLeftExceedsThreshold`, 15 000 left rows > 10 000 threshold):

```sql
SELECT SUM(`v`) AS `s`, `user_id`
FROM `fed`.`fed_events`
GROUP BY `user_id`
LIMIT 50000
```

No `WHERE user_id IN (...)` clause — the rule correctly no-ops when the static left bound exceeds the dialect threshold, and the query degrades to the pre-feature path.
