# ClickHouse `JdbcConvention` expression fix — M5.1

**Date:** 2026-04-20
**Branch:** `feat/clickhouse-datasource`
**Scope:** Fix the PLANNING_ERROR that causes all six query-path ClickHouse integration tests to fail during Calcite code-gen. Un-`@Ignore` those tests and show them green end-to-end in binary-mode IT.

## 1. Problem

Running any PPL query that resolves to a ClickHouse table currently fails with:

```
PLANNING_ERROR: A method named "unwrap" is not declared in any enclosing
  class nor any supertype, nor through a static import
  at compile step of the optimized query plan for physical execution
```

The optimized plan (from `ClickHouseBasicQueryIT#head_returns_rows`):

```
LogicalSystemLimit(fetch=[10000])
  LogicalProject(event_id=[$0])
    LogicalSort(fetch=[3])
      JdbcTableScan(table=[[ClickHouse, ch_basic, analytics, events]])
```

When Calcite's `JdbcToEnumerableConverter` code-generates the executor, it emits:

```java
ResultSetEnumerable.of(
    (javax.sql.DataSource) "CLICKHOUSE_ch_basic_118959517470781".unwrap(javax.sql.DataSource.class),
    "SELECT ...",
    rowBuilderFactory);
```

— i.e. it calls `.unwrap(DataSource.class)` on the **name** of the convention, because that's what we gave it.

## 2. Root cause

In `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseConvention.java`:

```java
super(ClickHouseSqlDialect.INSTANCE,
      new ConstantExpression(String.class, name),   // ← bug
      name);
```

The second argument to `JdbcConvention` is declared as `Expression expression` and is documented to be "the expression that will be used to generate the data source at runtime". Stock Calcite (`JdbcSchema.create(SchemaPlus parent, String name, DataSource ds, SqlDialect, catalog, schema)`) builds it with:

```java
Schemas.subSchemaExpression(parentSchema, name, JdbcSchema.class)
```

At codegen this becomes `parent.getSubSchema(name).unwrap(JdbcSchema.class)`, and `JdbcToEnumerableConverter` then wraps that in another `.unwrap(DataSource.class)` call. `JdbcSchema` implements `Wrapper` and returns its `DataSource` in `unwrap`.

We pass a bare `ConstantExpression` instead, so codegen generates `"STRING_LITERAL".unwrap(DataSource.class)` — type-illegal Java.

Verified via `javap` on `calcite-core-1.41.0.jar`:
- `JdbcToEnumerableConverter` loads `JdbcConvention.expression` then calls `Schemas.unwrap(expression, DataSource.class)` → emits `<expr>.unwrap(DataSource.class)`.
- `JdbcSchema implements Wrapper` with `public <T> T unwrap(Class<T>)`.

## 3. Constraints

1. **Expression must be built lazily.** When `ClickHouseSchemaFactory.build()` runs, the Calcite `SchemaPlus` tree containing our node does not yet exist (we're producing the `Schema` instance Calcite will mount). `Schemas.subSchemaExpression(parent, name, Class)` needs a live `SchemaPlus parent` — we must defer expression construction until after mounting.

2. **Must preserve per-datasource convention identity.** The existing design (one `JdbcConvention` instance per CH datasource, so planner traits don't collide across datasources) is correct; only the `expression` field needs fixing. Convention name + dialect stay as-is.

3. **Must not break the existing schema registration path.** `ClickHouseSchema` is added to root at `FrameworkConfig` build time (`QueryService.buildFrameworkConfig`); `getSubSchemaMap()` is called lazily by Calcite on first query. The per-datasource Schema instance must be valid from first access — we cannot require a second "install" step.

4. **Must cover both Basic and Pushdown ITs.** All six currently-`@Ignore`d tests must pass. Registration ITs stay passing.

5. **Plan-only unit tests must stay green.** `core` module has `ClickHousePlanSmokeTest` / `ClickHouseSchemaTest` that mock the `CalciteSchemaProvider` and do not build a real Calcite framework. The fix must not break tests that never exercise the codegen path.

## 4. Approaches considered

### Option A (chosen): pass `SchemaPlus` down at construction time, build expression via `Schemas.subSchemaExpression`

Thread the per-datasource `SchemaPlus parent` from the place we know it into `ClickHouseConvention.of(...)`, so the convention's expression is:

```java
Schemas.subSchemaExpression(parentSchema, datasourceName, JdbcSchema.class)
```

At codegen this evaluates to `rootSchema.getSubSchema("ClickHouse").getSubSchema("ch_basic").unwrap(JdbcSchema.class).getDataSource()` — exactly what stock Calcite does.

The "place we know `SchemaPlus`" is `ClickHouseSchema.getSubSchemaMap()` — but it receives `Map<String,Schema>`, not `Map<String, SchemaPlus>`, so it can't observe the parent plus directly. We solve this by making `ClickHouseSchema` **capture its own `SchemaPlus` from the outside**:

- `QueryService.buildFrameworkConfig` already does `SchemaPlus clickhouseSchemaPlus = rootSchema.add("ClickHouse", new ClickHouseSchema(ds));` — the returned `SchemaPlus` is the reference we need.
- Register that reference back into `ClickHouseSchema` (via a `setSchemaPlus(SchemaPlus)` call right after `add`, or — cleaner — via `ClickHouseSchema.install(SchemaPlus root)` that both adds itself and captures the returned plus).
- `getSubSchemaMap()` then passes the plus into each per-datasource schema's construction.

**Pros:** matches the canonical Calcite pattern; no static registry; minimal change surface.
**Cons:** introduces a small mutable field (`SchemaPlus` reference) on `ClickHouseSchema`; requires a one-line convention at the wiring site.

### Option B (rejected): static DataSource registry + explicit `Expressions.call`

Build the convention expression as `Expressions.call(DATA_SOURCE_REGISTRY, "get", Expressions.constant(name))` where `DATA_SOURCE_REGISTRY` is a `Map<String, DataSource>` keyed by convention name. Each `DataSource` is registered/unregistered alongside schema construction.

**Rejected:** introduces global mutable state, harder to unit-test, diverges from Calcite convention. Only considered as a fallback if Option A hits a SchemaPlus-access issue we cannot resolve.

### Option C (rejected): switch to stock `JdbcSchema.create` and drop per-datasource convention

Use Calcite's own `JdbcSchema.create(parentSchema, ds.name, ds, dialect, catalog, schema)`. This would work but loses per-datasource convention separation (all CH datasources share the default JDBC convention), which would complicate future per-datasource rule/cost customization (M6/M7).

**Rejected:** throws away an intentional design choice from M3.

## 5. Design (Option A — detailed)

### 5.1 `ClickHouseConvention` API change

Current:
```java
public static ClickHouseConvention of(String datasourceName) {
  return new ClickHouseConvention("CLICKHOUSE_" + datasourceName + "_" + System.nanoTime());
}
```

New:
```java
public static ClickHouseConvention of(String datasourceName, Expression expression) {
  return new ClickHouseConvention(
      "CLICKHOUSE_" + datasourceName + "_" + System.nanoTime(),
      expression);
}
```

where the constructor passes `expression` through to `super(dialect, expression, name)`.

### 5.2 `ClickHouseSchemaFactory.build` signature change

Current:
```java
public static Schema build(
    String datasourceName, DataSource dataSource, ClickHouseTableSpec.Schema spec)
```

New:
```java
public static Schema build(
    SchemaPlus parentSchema,       // ← new
    String datasourceName,
    DataSource dataSource,
    ClickHouseTableSpec.Schema spec)
```

Inside `build`, the per-datasource convention is now:

```java
Expression expr = Schemas.subSchemaExpression(parentSchema, datasourceName, JdbcSchema.class);
ClickHouseConvention convention = ClickHouseConvention.of(datasourceName, expr);
```

### 5.3 `ClickHouseJdbcSchemaBuilder.build` — use real `JdbcSchema` for sub-schemas

Current: wraps a `JdbcSchema` as `delegate` but returns a bespoke `AbstractSchema`. That means `parentSchema.getSubSchema(datasourceName).unwrap(JdbcSchema.class)` at runtime gets `null` (the bespoke `AbstractSchema` doesn't implement `Wrapper<JdbcSchema>`).

Fix: return a `JdbcSchema` subclass (or configure `JdbcSchema` directly) that holds our pre-validated `JdbcTable` map. Minimum-risk option is a small `AbstractSchema` subclass that overrides `unwrap(Class)`:

```java
@Override public <T> T unwrap(Class<T> clazz) {
  if (clazz.isInstance(delegate)) return clazz.cast(delegate);
  return super.unwrap(clazz);
}
```

so `.unwrap(JdbcSchema.class)` returns our `delegate` `JdbcSchema`, which then returns its `DataSource` on `.unwrap(DataSource.class)`.

**Why not just return `delegate` directly?** Because `delegate` enumerates tables from JDBC metadata at access time, bypassing our declared schema (and our type-validation pass). Keeping the `AbstractSchema` wrapper preserves the curated `tables` map; only the `unwrap` chain needs to bridge through.

### 5.4 `CalciteSchemaProvider.asCalciteSchema` signature change

Current:
```java
public interface CalciteSchemaProvider {
  Schema asCalciteSchema(String datasourceName);
}
```

New:
```java
public interface CalciteSchemaProvider {
  Schema asCalciteSchema(String datasourceName, SchemaPlus parentSchema);
}
```

`ClickHouseStorageEngine.asCalciteSchema` changes accordingly.

### 5.5 `ClickHouseSchema` captures its own `SchemaPlus`

Current:
```java
public class ClickHouseSchema extends AbstractSchema {
  public static final String CLICKHOUSE_SCHEMA_NAME = "ClickHouse";
  private final DataSourceService dataSourceService;

  @Override protected Map<String, Schema> getSubSchemaMap() { /* ... */ }
}
```

New:
```java
public class ClickHouseSchema extends AbstractSchema {
  public static final String CLICKHOUSE_SCHEMA_NAME = "ClickHouse";
  private final DataSourceService dataSourceService;
  private volatile SchemaPlus schemaPlus;    // set by install(); read by getSubSchemaMap()

  public static SchemaPlus install(SchemaPlus rootSchema, DataSourceService ds) {
    ClickHouseSchema node = new ClickHouseSchema(ds);
    SchemaPlus added = rootSchema.add(CLICKHOUSE_SCHEMA_NAME, node);
    node.schemaPlus = added;
    return added;
  }

  @Override protected Map<String, Schema> getSubSchemaMap() {
    if (dataSourceService == null || schemaPlus == null) return Map.of();
    /* ... provider.asCalciteSchema(name, schemaPlus) ... */
  }
}
```

The mutable field is set exactly once, right after `rootSchema.add`, before any query runs. `volatile` covers the happens-before boundary between `buildFrameworkConfig` (main thread) and planning threads.

### 5.6 `QueryService.buildFrameworkConfig` wiring

Current:
```java
rootSchema.add(ClickHouseSchema.CLICKHOUSE_SCHEMA_NAME,
               new ClickHouseSchema(dataSourceService));
```

New:
```java
ClickHouseSchema.install(rootSchema, dataSourceService);
```

## 6. Test strategy

### 6.1 Unit — `ClickHouseConventionTest` (new, in `clickhouse` module)

Verify the convention's expression resolves through a real `SchemaPlus` tree:

```java
SchemaPlus root = CalciteSchema.createRootSchema(true, false).plus();
SchemaPlus ch = root.add("ClickHouse", new AbstractSchema() { ... });
SchemaPlus sub = ch.add("ds1", ClickHouseJdbcSchemaBuilder.build(...));

Expression expr = Schemas.subSchemaExpression(ch, "ds1", JdbcSchema.class);
// When compiled+executed against `root`, the expression should resolve to a
// real DataSource matching the one we registered.
Object got = Expressions.evaluate(Schemas.unwrap(expr, DataSource.class), ...);
assertThat(got, instanceOf(DataSource.class));
assertSame(got, originalDs);
```

If `Expressions.evaluate` isn't straightforward, assert instead that `ch.getSubSchema("ds1").unwrap(JdbcSchema.class)` is non-null and that its `getDataSource()` matches ours.

### 6.2 Unit — update existing mocks

- `core/src/test/java/org/opensearch/sql/calcite/ClickHouseSchemaTest.java`: update stub to match new `asCalciteSchema(name, parent)` signature.
- `core/src/test/java/org/opensearch/sql/calcite/ClickHousePlanSmokeTest.java`: same.
- `clickhouse/src/test/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngineTest.java`: pass a fake `SchemaPlus` (can be `CalciteSchema.createRootSchema(true).plus()`).

### 6.3 Integration — remove `@Ignore`

Un-`@Ignore`:
- `ClickHouseBasicQueryIT.head_returns_rows`
- `ClickHouseBasicQueryIT.filter_and_project_end_to_end`
- `ClickHousePushdownIT.filter_returns_only_matching_rows`
- `ClickHousePushdownIT.project_drops_unwanted_columns`
- `ClickHousePushdownIT.sort_and_limit_return_top_n_descending`
- `ClickHousePushdownIT.explain_shows_jdbc_convention_nodes`

Acceptance: binary-mode IT run

```
./gradlew :integ-test:integTest -DuseClickhouseBinary=true -DignorePrometheus=true --tests "org.opensearch.sql.clickhouse.*"
```

reports `tests=9, skipped=0, failures=0, errors=0`, BUILD SUCCESSFUL.

### 6.4 Non-regression

- `./gradlew :core:test :clickhouse:test` stays green.
- Registration ITs stay green.
- Gradle spotless stays clean.

## 7. Change surface summary

| File | Kind of change |
|---|---|
| `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseConvention.java` | Add `Expression` parameter to `of()` / constructor; drop `ConstantExpression`. |
| `clickhouse/src/main/java/org/apache/calcite/adapter/jdbc/ClickHouseJdbcSchemaBuilder.java` | Return a wrapper schema that overrides `unwrap(Class)` to expose the inner `JdbcSchema`. |
| `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseSchemaFactory.java` | Accept `SchemaPlus parentSchema`; build convention expression via `Schemas.subSchemaExpression`. |
| `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngine.java` | `asCalciteSchema` signature adds `SchemaPlus parentSchema`. |
| `core/src/main/java/org/opensearch/sql/storage/CalciteSchemaProvider.java` | Interface signature adds `SchemaPlus parentSchema`. |
| `core/src/main/java/org/opensearch/sql/calcite/ClickHouseSchema.java` | Add `install(SchemaPlus, DataSourceService)` static helper; capture own `SchemaPlus`; thread it into `asCalciteSchema` calls. |
| `core/src/main/java/org/opensearch/sql/executor/QueryService.java` | Replace `rootSchema.add(...)` with `ClickHouseSchema.install(rootSchema, ds)`. |
| `core/src/test/java/.../ClickHouseSchemaTest.java` and `ClickHousePlanSmokeTest.java` | Update mocks for new `asCalciteSchema` signature. |
| `clickhouse/src/test/java/.../ClickHouseStorageEngineTest.java` | Pass a fake `SchemaPlus`. |
| `clickhouse/src/test/java/.../ClickHouseConventionTest.java` | **New.** Assert convention expression resolves through a live SchemaPlus tree. |
| `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHouseBasicQueryIT.java` | Remove `@Ignore` from 2 tests. |
| `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHousePushdownIT.java` | Remove `@Ignore` from 4 tests. |

## 8. Risks

- **`Schemas.subSchemaExpression` returns an Expression that evaluates through `parentSchema.getSubSchema(name)`** — if at codegen time the CH schema hasn't actually been added under root (e.g., datasource registered after framework config built), the expression resolves to null and we re-fail on `.unwrap(...)`. Mitigation: `ClickHouseSchema.getSubSchemaMap()` is called lazily during planning — datasource visibility at plan time is the same as visibility through `DataSourceService`, which is already handled. Guard: the volatile-null check on `schemaPlus` short-circuits and returns an empty map before a query can reach codegen against a missing parent.
- **`parentSchema` reference held across queries** — we're keeping a reference to the per-query `SchemaPlus` root in `ClickHouseSchema`. Calcite creates a new root per `FrameworkConfig` build; our `ClickHouseSchema` is also re-created per build (inside `buildFrameworkConfig`), so lifetimes match. Verified by reading `QueryService.buildFrameworkConfig`: both are per-call.
- **`unwrap(Class)` wrapper subtlety** — overriding `unwrap` on a schema wrapper has to chain to `super.unwrap` for unknown classes; otherwise we accidentally hide Calcite's own unwrap machinery. The design snippet in §5.3 preserves this.
- **Daemon-state bug already identified in IT harness** — on same daemon, the 2nd `startClickhouse` invocation fails with a `buildProcess` method-missing on the SpawnProcessTask decorator. Orthogonal to this fix; tracked separately.

## 9. Out of scope

- M5 aggregate/fallback rules (separate plan).
- M5 rate-limit / error handling polish.
- Pre-existing Gradle daemon `buildProcess`-missing intermittent on repeat `startClickhouse`.
- Calcite dialect extension (still the M3-era whitelist).

## Architecture invariants for future maintainers

Three coupled invariants must all hold for Calcite's JDBC convention to
push a query through ClickHouse at runtime. Breaking any one causes NPEs
or `Can't unwrap` errors deep in generated bytecode (`Baz.bind`, `Janino`).

### 1. SchemaPlus tree must be shared between planner and runtime
Calcite's `JdbcToEnumerableConverter` emits code that walks
`DataContext.ROOT.getRootSchema().getSubSchema("ClickHouse").getSubSchema(dsName)`
at runtime. `DataContext.ROOT.getRootSchema()` is the SchemaPlus passed to
`OpenSearchDriver.connect`, NOT the one on `FrameworkConfig`. If they differ,
runtime lookup sees an empty tree. Enforcement: `CalciteToolsHelper.connect`
now requires the same `rootSchema` that `QueryService.buildFrameworkConfig`
built, threaded through `CalcitePlanContext`.

### 2. Every AbstractSchema wrapper in the chain must implement `org.apache.calcite.schema.Wrapper`
`SchemaPlusImpl.unwrap(Class)` delegates to the wrapped schema's `unwrap` ONLY
if the wrapped schema `instanceof Wrapper` (SchemaPlusImpl.unwrap step 4).
Anonymous `new AbstractSchema(){}` subclasses do not implement Wrapper —
unwrap returns the wrapper itself via step 1 instead of the inner JdbcSchema.
Enforcement: `OuterWrapper` (per-datasource) and `WrappingSchema`
(per-database), both named inner classes `extends AbstractSchema implements Wrapper`.

### 3. `unwrap(clazz)` must delegate to inner `JdbcSchema.unwrap(clazz)`, not return null
Codegen invokes `unwrap(DataSource.class)` directly on the sub-schema. `JdbcSchema`
handles `DataSource.class`, `Connection.class`, and itself. Custom wrappers that
only handle `Wrapper.class` or `JdbcSchema.class` break the one-step path.
Enforcement: `OuterWrapper.unwrap` and `WrappingSchema.unwrap` delegate to
`delegate.unwrap(clazz)` after `this`-check (standard Calcite idiom — see
`JdbcSchema.Factory.create`).

### Tests that pin these invariants

- `ClickHouseConventionTest.sub_schema_unwraps_to_jdbc_schema_and_data_source`
  — walks chain, asserts same-DataSource, uses second-database assertNotSame
  to pin first-database-only invariant.
- `ClickHousePushdownPlanTest.*` — JdbcRules registration + convention register.
- `ClickHouseSqlDialectTest.unparse_offset_fetch_*` — dialect emits LIMIT/OFFSET
  not ANSI FETCH NEXT (ClickHouse Code 628 guard).
- `QueryServiceTest.framework_config_registers_clickhouse_schema_under_root`
  — rootSchema plumbing from install to FrameworkConfig.

### If these tests start failing after Calcite upgrade

1. Run the SchemaPlusImpl.unwrap probe: check whether step ordering still
   matches Calcite 1.41.0 (isInstance(this) → outerCalciteSchema → wrappedSchema
   → `wrapped instanceof Wrapper` → unwrapOrThrow).
2. Check `JdbcToEnumerableConverter.implement` — if it switches from
   `Schemas.unwrap(expr, DataSource.class)` to something else, update
   `OuterWrapper.unwrap` contract accordingly.
3. If the user-facing fallback is needed, see Task 3 in the plan:
   `Programs.sequence(Programs.standard(), Programs.ofRules(JdbcRules.rules(convention)))`.

## Pushdown verification (empirical)

Evidence collected 2026-04-20 after the Wave 6 ITs landed. Ran `ClickHousePushdownIT`
binary-mode and then queried `system.query_log` on the same ClickHouse instance for
`QueryFinish` rows whose SQL targets `a.t`. The verbatim SQL below is what CH
received over the JDBC wire — i.e., what Calcite's `JdbcConvention` actually
unparsed and sent. Anything Calcite computed above `JdbcToEnumerableConverter`
never reaches this log.

### CH query_log capture (verbatim)

| # | PPL query | CH SQL (verbatim, wire format) |
|---|-----------|--------------------------------|
| 1 | `source=ch_push.a.t \| where id = 42 \| fields id` | ```SELECT `id` FROM `a`.`t` WHERE `id` = 42 LIMIT 10000``` |
| 2 | `source=ch_push.a.t \| head 1 \| fields id` | ```SELECT * FROM (SELECT `id` FROM `a`.`t` LIMIT 1) AS `t1` LIMIT 10000``` |
| 3 | `source=ch_push.a.t \| sort - id \| head 3 \| fields id` | ```SELECT * FROM (SELECT `id` FROM `a`.`t` ORDER BY `id` DESC LIMIT 3) AS `t1` ORDER BY `id` DESC LIMIT 10000``` |
| 4 | `source=ch_push.a.t \| stats count() as c, avg(v) as av, sum(v) as sv, min(v) as mn, max(v) as mx` | ```SELECT COUNT(*) AS `c`, AVG(`v`) AS `av`, SUM(`v`) AS `sv`, MIN(`v`) AS `mn`, MAX(`v`) AS `mx` FROM `a`.`t` LIMIT 10000``` |
| 5 | `source=ch_push.a.t \| stats avg(v) as av by id` | ```SELECT AVG(`v`) AS `av`, `id` FROM `a`.`t` GROUP BY `id` LIMIT 10000``` |
| 6 | `source=ch_push.a.t \| where id > 50 \| stats count() as c` | ```SELECT COUNT(*) AS `c` FROM `a`.`t` WHERE `id` > 50 LIMIT 10000``` |

The outer `LIMIT 10000` on every statement is Calcite's `LogicalSystemLimit` —
the PPL engine's safety cap on an un-bounded `source`. It rides through JDBC as
an outer `JdbcSort(fetch=[10000])`; that is expected and not a bug.

### Per-operator pushdown verdict

| Operator | Query | Evidence in CH SQL | Jdbc\* plan | Verdict |
|----------|-------|--------------------|-------------|---------|
| Filter | `\| where id = 42` | `WHERE `id` = 42` present in SELECT sent to CH | `JdbcFilter` | **Pushed** |
| Projection | `\| fields id` (rows 1 and 2) | CH receives `SELECT `id`` with no `v` column | `JdbcProject` | **Pushed** |
| Limit (`\| head N`) | `\| head 1` / `\| head 3` | CH receives `LIMIT 1` / `LIMIT 3` inside the inner SELECT | `JdbcSort(fetch=[N])` | **Pushed** |
| Sort | `\| sort - id` | CH receives `ORDER BY `id` DESC` inside the inner SELECT | `JdbcSort(sort0=[$0], dir0=[DESC-nulls-last])` | **Pushed** |
| Global `count()` | row 4 | `COUNT(*)` in the SELECT sent to CH | `JdbcAggregate(group=[{}], c=[COUNT()])` | **Pushed** |
| Global `avg(v)` | row 4 | `AVG(`v`)` in the SELECT sent to CH (not split into SUM/COUNT) | `JdbcAggregate(... av=[AVG($1)])` | **Pushed** |
| Global `sum(v)` | row 4 | `SUM(`v`)` in the SELECT sent to CH | `JdbcAggregate(... sv=[SUM($1)])` | **Pushed** |
| Global `min(v)` | row 4 | `MIN(`v`)` in the SELECT sent to CH | `JdbcAggregate(... mn=[MIN($1)])` | **Pushed** |
| Global `max(v)` | row 4 | `MAX(`v`)` in the SELECT sent to CH | `JdbcAggregate(... mx=[MAX($1)])` | **Pushed** |
| Group-by (`stats avg by id`) | row 5 | `GROUP BY `id`` + `AVG(`v`)` in one SELECT | `JdbcAggregate(group=[{0}], av=[AVG($1)])` | **Pushed** |
| Filter + `count()` | row 6 | `WHERE `id` > 50` + `COUNT(*)` in a single SELECT | `JdbcAggregate(group=[{}], c=[COUNT()])` over `JdbcFilter(condition=[>($0, 50)])` | **Pushed (both)** |

All operators tested (filter, project, sort+limit, and every aggregation form
including group-by and filter+aggregate) round-trip to ClickHouse. Calcite is
not computing them above the JDBC boundary.

**Surprise worth pinning**: Calcite does **not** decompose `AVG` into `SUM/COUNT`
over the JDBC boundary for the ClickHouse dialect. The logical plan lists
`AVG($0)` and the physical plan keeps it as `JdbcAggregate(... av=[AVG($1)])`,
and the JDBC unparser emits a literal `AVG(`v`)` to ClickHouse — CH then
computes the average natively. The
`ClickHousePushdownIT.query_log_confirms_aggregation_pushdown` test pins this
with an explicit `containsString("AVG(`v`)")` assertion so any future Calcite
upgrade that flips to a SUM/COUNT split will trip red and force a conscious
decision.

### EXPLAIN physical plan (corroborating)

The PPL `_explain` endpoint emits Calcite's physical plan as a string. The
`Jdbc*` class names confirm, node-by-node, that each operator is part of the
JDBC sub-plan (below `JdbcToEnumerableConverter`):

```
# | where id > 10
physical:
  JdbcToEnumerableConverter
    JdbcSort(fetch=[10000])
      JdbcFilter(condition=[>($0, 10)])
        JdbcTableScan(table=[[ClickHouse, ch_push, a, t]])

# | fields id
physical:
  JdbcToEnumerableConverter
    JdbcSort(fetch=[10000])
      JdbcProject(id=[$0])
        JdbcTableScan(table=[[ClickHouse, ch_push, a, t]])

# | sort - id | head 3
physical:
  JdbcToEnumerableConverter
    JdbcSort(sort0=[$0], dir0=[DESC-nulls-last], fetch=[10000])
      JdbcSort(sort0=[$0], dir0=[DESC-nulls-last], fetch=[3])
        JdbcTableScan(table=[[ClickHouse, ch_push, a, t]])

# | stats count() as c, avg(v) as av, sum(v) as sv, min(v) as mn, max(v) as mx
physical:
  JdbcToEnumerableConverter
    JdbcSort(fetch=[10000])
      JdbcAggregate(group=[{}], c=[COUNT()], av=[AVG($1)], sv=[SUM($1)], mn=[MIN($1)], mx=[MAX($1)])
        JdbcTableScan(table=[[ClickHouse, ch_push, a, t]])

# | stats avg(v) as av by id
physical:
  JdbcToEnumerableConverter
    JdbcSort(fetch=[10000])
      JdbcProject(av=[$1], id=[$0])
        JdbcAggregate(group=[{0}], av=[AVG($1)])
          JdbcTableScan(table=[[ClickHouse, ch_push, a, t]])

# | where id > 50 | stats count() as c
physical:
  JdbcToEnumerableConverter
    JdbcSort(fetch=[10000])
      JdbcAggregate(group=[{}], c=[COUNT()])
        JdbcFilter(condition=[>($0, 50)])
          JdbcTableScan(table=[[ClickHouse, ch_push, a, t]])
```

No `EnumerableFilter`, `EnumerableProject`, `EnumerableSort`, or
`EnumerableAggregate` appears inside any plan — the only Enumerable node is
the converter at the root, which is structural.

### IT pins (permanent regression coverage)

The following IT methods assert these guarantees so any regression in JDBC-rule
registration or dialect unparsing trips red:

- `ClickHousePushdownIT.explain_shows_jdbc_convention_nodes` —
  asserts `JdbcToEnumerableConverter`, `JdbcTableScan`, `JdbcFilter` on
  `| where id > 10`.
- `ClickHousePushdownIT.explain_project_ridden_as_jdbc_project` —
  asserts `JdbcProject` on `| fields id`.
- `ClickHousePushdownIT.explain_sort_and_limit_ridden_as_jdbc_sort` —
  asserts `JdbcSort`, `DESC`, `fetch=[3]` on `| sort - id | head 3`.
- `ClickHousePushdownIT.query_log_confirms_filter_project_sort_pushdown` —
  runs each PPL query, queries `system.query_log`, asserts the CH-side
  SQL contains `WHERE`, `ORDER BY ... DESC`, `LIMIT N`, and that `v` is
  never selected when `| fields id` is in the pipeline.
- `ClickHousePushdownIT.explain_global_aggregation_ridden_as_jdbc_aggregate` —
  asserts `JdbcAggregate(group=[{}]` with `COUNT()`, `AVG(`, `SUM(`, `MIN(`,
  `MAX(` all on the JDBC side and no `EnumerableAggregate` anywhere, on
  `| stats count() as c, avg(v) as av, sum(v) as sv, min(v) as mn, max(v) as mx`.
- `ClickHousePushdownIT.explain_group_by_aggregation_ridden_as_jdbc_aggregate` —
  asserts `JdbcAggregate(group=[{0}]` on `| stats avg(v) by id`, pinning that
  column-0 (`id`) is the group key below the JDBC boundary.
- `ClickHousePushdownIT.explain_filter_plus_aggregation_ridden_as_jdbc_aggregate_over_jdbc_filter` —
  asserts `JdbcAggregate(group=[{}]` above `JdbcFilter(condition=[>($0, 50)])`
  on `| where id > 50 | stats count() as c`.
- `ClickHousePushdownIT.global_aggregation_returns_correct_row` —
  asserts count=100, avg=75.75, sum=7575.0, min=1.5, max=150.0 on the seeded
  1..100 / `id*1.5` dataset, so numerical correctness is pinned end-to-end.
- `ClickHousePushdownIT.group_by_aggregation_returns_one_row_per_group` —
  asserts 100 rows returned (one per distinct `id`).
- `ClickHousePushdownIT.query_log_confirms_aggregation_pushdown` —
  runs the three aggregation PPL queries, queries `system.query_log`, asserts
  the CH-side SQL contains `AVG(`v`)` (not a SUM/COUNT split), `GROUP BY `id``
  for the group-by form, and a single `WHERE `id` > 50 ... COUNT(*)` SELECT
  for the filter+count form.
