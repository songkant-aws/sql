# ClickHouse `JdbcConvention` Expression Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the `PLANNING_ERROR: A method named "unwrap" is not declared` that blocks all six ClickHouse query-path integration tests by wiring a proper `JdbcConvention` expression (via `Schemas.subSchemaExpression`).

**Architecture:** Thread the per-query Calcite `SchemaPlus` parent down from `QueryService.buildFrameworkConfig` (the sole place it's available) through `ClickHouseSchema` (captures its own `SchemaPlus` via a new `install(...)` helper) into `ClickHouseSchemaFactory.build(...)` → `ClickHouseConvention.of(...)`. The convention's expression becomes `Schemas.subSchemaExpression(parentSchema, datasourceName, JdbcSchema.class)`; the per-datasource wrapper schema overrides `unwrap(JdbcSchema.class)` to expose the inner `JdbcSchema`, so Calcite's codegen `<expr>.unwrap(DataSource.class)` lands on a real `Wrapper`.

**Tech Stack:** Java 21, Apache Calcite 1.41.0, Gradle, JUnit 5 (unit) + JUnit 4 (IT, per OpenSearch convention), Hikari/ClickHouse JDBC 0.6.5. Worktree: `/home/songkant/workspace/sql/.worktrees/clickhouse-datasource` on branch `feat/clickhouse-datasource`.

**Verification driver (binary-mode IT):**
```
JAVA_HOME=/home/songkant/jdks/amazon-corretto-21.0.10.7.1-linux-x64 \
  ./gradlew :integ-test:integTest \
  -DuseClickhouseBinary=true \
  -DignorePrometheus=true \
  --tests "org.opensearch.sql.clickhouse.*"
```
Expected at plan completion: `tests=9, skipped=0, failures=0`.

**Daemon-state caveat (pre-existing, not to be fixed here):** The second `startClickhouse` invocation in the same Gradle daemon fails with `Could not find method buildProcess()`. Always `./gradlew --stop` before each IT run in this plan.

---

## File Structure

### New files

| Path | Responsibility |
|---|---|
| `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseConventionTest.java` | Unit test: convention expression resolves through a live `SchemaPlus` tree to a real `DataSource`. |

### Modified files

| Path | Change |
|---|---|
| `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseConvention.java` | Constructor + `of()` take `Expression expression`; drop the `ConstantExpression` bug. |
| `clickhouse/src/main/java/org/apache/calcite/adapter/jdbc/ClickHouseJdbcSchemaBuilder.java` | Returned wrapper schema overrides `unwrap(Class)` to expose the inner `JdbcSchema`. |
| `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseSchemaFactory.java` | `build()` takes `SchemaPlus parentSchema`; builds expression via `Schemas.subSchemaExpression`. |
| `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngine.java` | `asCalciteSchema(String, SchemaPlus)` signature. |
| `core/src/main/java/org/opensearch/sql/storage/CalciteSchemaProvider.java` | Interface: `asCalciteSchema(String, SchemaPlus)`. |
| `core/src/main/java/org/opensearch/sql/calcite/ClickHouseSchema.java` | Static `install(SchemaPlus root, DataSourceService ds)`; capture own `SchemaPlus` in volatile field; `getSubSchemaMap` threads it into provider calls. |
| `core/src/main/java/org/opensearch/sql/executor/QueryService.java` | Use `ClickHouseSchema.install(rootSchema, dataSourceService)` instead of raw `rootSchema.add(...)`. |
| `core/src/test/java/org/opensearch/sql/calcite/ClickHouseSchemaTest.java` | Update stubbing to match new `asCalciteSchema` signature. |
| `core/src/test/java/org/opensearch/sql/calcite/ClickHousePlanSmokeTest.java` | Same. Also use `install(...)` where appropriate. |
| `clickhouse/src/test/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngineTest.java` | Pass a fake `SchemaPlus` to `asCalciteSchema`. |
| `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHouseBasicQueryIT.java` | Remove `@Ignore` from 2 tests. |
| `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHousePushdownIT.java` | Remove `@Ignore` from 4 tests. |

---

## Task 1: Update `CalciteSchemaProvider` interface

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/storage/CalciteSchemaProvider.java`

- [ ] **Step 1.1: Update the interface signature**

Replace the full file with:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Storage engines that expose a Calcite Schema (for per-query sub-schema registration) implement
 * this interface. The Calcite engine queries this via {@code instanceof} when building the planner
 * root schema. Keeps {@code core} free of dependencies on connector-specific modules.
 *
 * <p>{@code parentSchema} is the {@link SchemaPlus} under which the returned sub-schema will be
 * mounted (e.g. the {@code "ClickHouse"} schema). Implementations that build a per-datasource
 * Calcite {@link org.apache.calcite.adapter.jdbc.JdbcConvention} need this reference to construct
 * the convention's expression via {@link org.apache.calcite.schema.Schemas#subSchemaExpression}.
 */
public interface CalciteSchemaProvider {
  Schema asCalciteSchema(String datasourceName, SchemaPlus parentSchema);
}
```

- [ ] **Step 1.2: Attempt compile — expect failures in callers**

Run: `JAVA_HOME=/home/songkant/jdks/amazon-corretto-21.0.10.7.1-linux-x64 ./gradlew :core:compileJava`

Expected: FAIL in `ClickHouseSchema.java` line ~48 (caller uses old single-arg signature). This is intentional — subsequent tasks fix callers.

- [ ] **Step 1.3: Do NOT commit yet**

This change is incomplete without the caller updates in Tasks 2–4. We'll commit after Task 4 when `:core:compileJava` is green again.

---

## Task 2: Update `ClickHouseConvention` to accept a real `Expression`

**Files:**
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseConvention.java`

- [ ] **Step 2.1: Rewrite the convention class**

Replace the full file with:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.calcite;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.linq4j.tree.Expression;

/**
 * Per-datasource {@link JdbcConvention}. The {@code expression} passed to the parent constructor
 * must be a Calcite linq4j tree that resolves to the per-datasource {@code JdbcSchema} at codegen
 * time; Calcite's {@code JdbcToEnumerableConverter} wraps it in {@code
 * Schemas.unwrap(expr, DataSource.class)} when emitting the executor. A bare {@code
 * ConstantExpression(String.class, name)} (as used originally in M3) generates {@code
 * "literal".unwrap(DataSource.class)} Java code, which fails to compile — see
 * {@code docs/superpowers/specs/2026-04-20-clickhouse-convention-expression-fix-design.md}.
 *
 * <p>Callers should construct the expression via {@link
 * org.apache.calcite.schema.Schemas#subSchemaExpression} against the {@link
 * org.apache.calcite.schema.SchemaPlus} under which the per-datasource sub-schema is mounted.
 */
public class ClickHouseConvention extends JdbcConvention {
  private ClickHouseConvention(String name, Expression expression) {
    super(ClickHouseSqlDialect.INSTANCE, expression, name);
  }

  public static ClickHouseConvention of(String datasourceName, Expression expression) {
    return new ClickHouseConvention(
        "CLICKHOUSE_" + datasourceName + "_" + System.nanoTime(), expression);
  }
}
```

- [ ] **Step 2.2: Attempt compile — expect caller failure in `ClickHouseSchemaFactory`**

Run: `JAVA_HOME=/home/songkant/jdks/amazon-corretto-21.0.10.7.1-linux-x64 ./gradlew :clickhouse:compileJava`

Expected: FAIL at `ClickHouseSchemaFactory.java:27` (calls old single-arg `of`). Next task fixes this.

- [ ] **Step 2.3: Do NOT commit yet**

Same rationale — wait for Task 4.

---

## Task 3: Update `ClickHouseJdbcSchemaBuilder` to chain `unwrap`

**Files:**
- Modify: `clickhouse/src/main/java/org/apache/calcite/adapter/jdbc/ClickHouseJdbcSchemaBuilder.java`

**Verified behavior of Calcite 1.41.0's `SchemaPlus.unwrap(Class)` (probed 2026-04-20):**

`SchemaPlusImpl.unwrap(Class clazz)` does exactly five checks, in order:
1. `isInstance(this)` — the SchemaPlus itself;
2. `isInstance(outerCalciteSchema)`;
3. `isInstance(wrappedSchema)` — the raw Schema handed to `rootSchema.add(name, schema)`;
4. `wrappedSchema instanceof org.apache.calcite.schema.Wrapper` → delegate to `wrappedSchema.unwrapOrThrow(clazz)`;
5. `throw new ClassCastException`.

Crucially, `org.apache.calcite.schema.Schema` in 1.41.0 has **no `unwrap` method** (only the optional `Wrapper` interface does). `AbstractSchema` does NOT implement `Wrapper`. So overriding `unwrap` on a bare `AbstractSchema` anonymous subclass is dead code — `SchemaPlusImpl.unwrap` never invokes it. We must make the returned schema `implements org.apache.calcite.schema.Wrapper` so the step-4 branch activates.

Java anonymous classes cannot `implements` extra interfaces at the `new` site, so we use a **named inner class** inside the builder.

- [ ] **Step 3.1: Rewrite the builder**

Replace the full file with:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.calcite.adapter.jdbc;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlDialect;
import org.opensearch.sql.clickhouse.config.ClickHouseColumnSpec;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;
import org.opensearch.sql.clickhouse.type.ClickHouseTypeMapper;

/**
 * Builds a Calcite {@link Schema} whose tables are {@link JdbcTable} instances. Lives in the
 * {@code org.apache.calcite.adapter.jdbc} package so it can invoke the package-private {@code
 * JdbcTable} constructor.
 *
 * <p>The returned schema {@code implements} {@link Wrapper} so Calcite's {@code SchemaPlusImpl
 * .unwrap(Class)} — which only delegates to the wrapped schema's {@code unwrap} when the wrapped
 * schema is a {@link Wrapper} — routes {@code parentSchema.getSubSchema(name).unwrap(JdbcSchema
 * .class)} into our override, yielding the inner {@link JdbcSchema}. Calcite's codegen then does
 * a second {@code .unwrap(DataSource.class)} on the {@link JdbcSchema} (itself a {@link Wrapper})
 * and reaches the real {@link javax.sql.DataSource}.
 */
public final class ClickHouseJdbcSchemaBuilder {
  private ClickHouseJdbcSchemaBuilder() {}

  public static AbstractSchema build(
      DataSource ds,
      SqlDialect dialect,
      JdbcConvention convention,
      String catalog,
      ClickHouseTableSpec.Schema ignored, // row type is read lazily via metadata
      List<ClickHouseTableSpec> tableSpecs) {
    final JdbcSchema delegate = new JdbcSchema(ds, dialect, convention, catalog, null);
    final Map<String, Table> tables = new LinkedHashMap<>();
    for (ClickHouseTableSpec tbl : tableSpecs) {
      // Validate every (ch_type, expr_type) pair — throw early on unsupported columns.
      for (ClickHouseColumnSpec col : tbl.getColumns()) {
        ClickHouseTypeMapper.resolve(col.getChType(), col.getExprType());
      }
      tables.put(
          tbl.getName(),
          new JdbcTable(delegate, catalog, null, tbl.getName(), Schema.TableType.TABLE));
    }
    return new WrappingSchema(tables, delegate);
  }

  /**
   * Named inner class so the returned schema can both extend {@link AbstractSchema} (for the
   * curated table map) AND implement {@link Wrapper} (so Calcite's {@code SchemaPlusImpl.unwrap}
   * delegates to our {@link #unwrap} override). Anonymous classes cannot implement extra
   * interfaces, which is why this is hoisted out.
   */
  private static final class WrappingSchema extends AbstractSchema implements Wrapper {
    private final Map<String, Table> tables;
    private final JdbcSchema delegate;

    WrappingSchema(Map<String, Table> tables, JdbcSchema delegate) {
      this.tables = tables;
      this.delegate = delegate;
    }

    @Override
    protected Map<String, Table> getTableMap() {
      return tables;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
      // Entry point reached via SchemaPlusImpl.unwrap → Wrapper.unwrapOrThrow → this.unwrap.
      // Expose the inner JdbcSchema so the next .unwrap(DataSource.class) step (on the
      // JdbcSchema itself, which also implements Wrapper) can reach the real DataSource.
      if (clazz.isInstance(delegate)) {
        return clazz.cast(delegate);
      }
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      return null;
    }
  }
}
```

- [ ] **Step 3.2: Skip compile here**

Still broken until Task 4 — don't waste a gradle cycle.

- [ ] **Step 3.3: Do NOT commit yet**

---

## Task 4: Update `ClickHouseSchemaFactory` to take `SchemaPlus` and build real expression

**Files:**
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseSchemaFactory.java`
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngine.java`

- [ ] **Step 4.1: Rewrite `ClickHouseSchemaFactory`**

Replace the full file with:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.calcite.adapter.jdbc.ClickHouseJdbcSchemaBuilder;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.sql.clickhouse.calcite.ClickHouseConvention;
import org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;

import org.apache.calcite.schema.Wrapper;
// ... (plus existing imports)

public final class ClickHouseSchemaFactory {
  private ClickHouseSchemaFactory() {}

  /**
   * Build a Calcite Schema whose sub-schemas correspond to CH databases, each sub-schema holding
   * JdbcTable instances backed by the given DataSource and a per-datasource ClickHouseConvention.
   *
   * <p>{@code parentSchema} is the {@link SchemaPlus} under which the returned schema will be
   * mounted (by {@code ClickHouseSchema.getSubSchemaMap}); it is used to construct the convention's
   * runtime expression via {@link Schemas#subSchemaExpression}. The convention's expression is
   * {@code Schemas.subSchemaExpression(parentSchema, datasourceName, JdbcSchema.class)}. Calcite
   * codegen resolves that expression via {@code parentSchema.getSubSchema(datasourceName)
   * .unwrap(JdbcSchema.class)}, which is handled by {@code SchemaPlusImpl.unwrap} — that delegate
   * succeeds only if the sub-schema we return here {@code implements Wrapper}. So the
   * per-datasource outer schema must be a {@link Wrapper} that exposes one of its inner
   * {@link JdbcSchema} delegates (all inner schemas wrap the same {@link DataSource}, so any one
   * suffices for the codegen {@code .unwrap(DataSource.class)} step that follows).
   */
  public static Schema build(
      SchemaPlus parentSchema,
      String datasourceName,
      DataSource dataSource,
      ClickHouseTableSpec.Schema spec) {
    Expression expression =
        Schemas.subSchemaExpression(parentSchema, datasourceName, JdbcSchema.class);
    ClickHouseConvention convention = ClickHouseConvention.of(datasourceName, expression);
    final Map<String, Schema> subs = new LinkedHashMap<>();
    // Hold a reference to any one of the per-database JdbcSchemas so the outer schema can
    // expose it via unwrap(JdbcSchema.class). All per-db schemas share the same DataSource,
    // so which one we pick doesn't matter to codegen's subsequent .unwrap(DataSource.class).
    JdbcSchema anyDelegate = null;
    for (ClickHouseTableSpec.Database db : spec.getDatabases()) {
      AbstractSchema perDbWrapper =
          ClickHouseJdbcSchemaBuilder.build(
              dataSource,
              ClickHouseSqlDialect.INSTANCE,
              convention,
              db.getName(),
              spec,
              db.getTables());
      subs.put(db.getName(), perDbWrapper);
      if (anyDelegate == null) {
        // perDbWrapper is a WrappingSchema; its Wrapper.unwrap returns the inner JdbcSchema.
        anyDelegate = perDbWrapper.unwrap(JdbcSchema.class);
      }
    }
    return new OuterWrapper(subs, anyDelegate);
  }

  /**
   * Outer per-datasource wrapper. {@code implements Wrapper} so Calcite's {@code
   * SchemaPlusImpl.unwrap(JdbcSchema.class)} — called by the convention's runtime expression —
   * delegates here and gets a real {@link JdbcSchema} back. Without the {@code Wrapper} marker,
   * Calcite throws {@code ClassCastException: not a class ... JdbcSchema}.
   */
  private static final class OuterWrapper extends AbstractSchema implements Wrapper {
    private final Map<String, Schema> subs;
    private final JdbcSchema delegate; // any one of the per-db JdbcSchemas; all share ds.

    OuterWrapper(Map<String, Schema> subs, JdbcSchema delegate) {
      this.subs = subs;
      this.delegate = delegate;
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
      return subs;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
      if (delegate != null && clazz.isInstance(delegate)) {
        return clazz.cast(delegate);
      }
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      return null;
    }
  }
}
```

- [ ] **Step 4.2: Rewrite `ClickHouseStorageEngine.asCalciteSchema`**

In `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngine.java`, replace the `asCalciteSchema` method (lines 48-50):

Old:
```java
  public org.apache.calcite.schema.Schema asCalciteSchema(String datasourceName) {
    return ClickHouseSchemaFactory.build(datasourceName, client.getDataSource(), config.getSchema());
  }
```

New:
```java
  @Override
  public org.apache.calcite.schema.Schema asCalciteSchema(
      String datasourceName, org.apache.calcite.schema.SchemaPlus parentSchema) {
    return ClickHouseSchemaFactory.build(
        parentSchema, datasourceName, client.getDataSource(), config.getSchema());
  }
```

- [ ] **Step 4.3: Compile `:clickhouse`**

Run: `JAVA_HOME=/home/songkant/jdks/amazon-corretto-21.0.10.7.1-linux-x64 ./gradlew :clickhouse:compileJava`

Expected: SUCCESS. If any compile error remains, re-read the relevant file and fix before continuing.

- [ ] **Step 4.4: Update `ClickHouseSchema` to install + capture `SchemaPlus`**

Replace the full file `core/src/main/java/org/opensearch/sql/calcite/ClickHouseSchema.java` with:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.CalciteSchemaProvider;

/**
 * Calcite {@link AbstractSchema} that exposes one Calcite sub-schema per registered ClickHouse
 * datasource. Sub-schema map is resolved lazily on each access; individual sub-schemas are obtained
 * from each CH datasource's {@code StorageEngine} via {@link CalciteSchemaProvider} so {@code core}
 * stays decoupled from the {@code clickhouse} module.
 *
 * <p>A null {@code dataSourceService} is tolerated (returns an empty map) so plan-only test
 * harnesses that build a {@code FrameworkConfig} without a live service still succeed.
 *
 * <p>Use {@link #install} rather than {@code rootSchema.add(...)} directly — the schema instance
 * needs a reference to its own {@link SchemaPlus} parent in order to construct the per-datasource
 * {@code JdbcConvention} expression.
 */
@Getter
public class ClickHouseSchema extends AbstractSchema {
  public static final String CLICKHOUSE_SCHEMA_NAME = "ClickHouse";

  private final DataSourceService dataSourceService;
  /**
   * Set by {@link #install}; read by {@link #getSubSchemaMap}. {@code volatile} covers the
   * happens-before between {@code install()} (called from the main thread inside {@code
   * QueryService.buildFrameworkConfig}) and the planner threads that later dereference it.
   */
  private volatile SchemaPlus schemaPlus;

  public ClickHouseSchema(DataSourceService dataSourceService) {
    this.dataSourceService = dataSourceService;
  }

  /**
   * Create a {@link ClickHouseSchema}, mount it under {@code rootSchema} as {@link
   * #CLICKHOUSE_SCHEMA_NAME}, and capture the returned {@link SchemaPlus} so sub-schema
   * construction can build per-datasource convention expressions against it.
   *
   * @return the mounted {@link SchemaPlus} (convenience for callers).
   */
  public static SchemaPlus install(SchemaPlus rootSchema, DataSourceService dataSourceService) {
    ClickHouseSchema node = new ClickHouseSchema(dataSourceService);
    SchemaPlus added = rootSchema.add(CLICKHOUSE_SCHEMA_NAME, node);
    node.schemaPlus = added;
    return added;
  }

  @Override
  protected Map<String, Schema> getSubSchemaMap() {
    if (dataSourceService == null || schemaPlus == null) {
      return Map.of();
    }
    Map<String, Schema> result = new LinkedHashMap<>();
    for (DataSourceMetadata md : dataSourceService.getDataSourceMetadata(true)) {
      if (!DataSourceType.CLICKHOUSE.equals(md.getConnector())) {
        continue;
      }
      DataSource ds = dataSourceService.getDataSource(md.getName());
      if (ds.getStorageEngine() instanceof CalciteSchemaProvider provider) {
        result.put(md.getName(), provider.asCalciteSchema(md.getName(), schemaPlus));
      }
    }
    return result;
  }
}
```

Note: we dropped `@AllArgsConstructor` because we now need a mutable field. A hand-written single-arg constructor keeps existing callers working.

- [ ] **Step 4.5: Update `QueryService.buildFrameworkConfig`**

In `core/src/main/java/org/opensearch/sql/executor/QueryService.java`, replace lines 364-365:

Old:
```java
    rootSchema.add(
        ClickHouseSchema.CLICKHOUSE_SCHEMA_NAME, new ClickHouseSchema(dataSourceService));
```

New:
```java
    ClickHouseSchema.install(rootSchema, dataSourceService);
```

- [ ] **Step 4.6: Compile `:core`**

Run: `JAVA_HOME=/home/songkant/jdks/amazon-corretto-21.0.10.7.1-linux-x64 ./gradlew :core:compileJava`

Expected: SUCCESS.

- [ ] **Step 4.7: Compile test classes and observe expected test failures**

Run: `JAVA_HOME=/home/songkant/jdks/amazon-corretto-21.0.10.7.1-linux-x64 ./gradlew :core:compileTestJava :clickhouse:compileTestJava`

Expected: FAIL — test mocks still use old single-arg `asCalciteSchema`. Tasks 5 and 6 fix them.

---

## Task 5: Update `core` unit tests for new signature

**Files:**
- Modify: `core/src/test/java/org/opensearch/sql/calcite/ClickHouseSchemaTest.java`
- Modify: `core/src/test/java/org/opensearch/sql/calcite/ClickHousePlanSmokeTest.java`

- [ ] **Step 5.1: Fix `ClickHouseSchemaTest`**

Three changes:

1. Import `org.apache.calcite.jdbc.CalciteSchema` and `org.apache.calcite.schema.SchemaPlus`.
2. The `sub_schema_map_contains_one_entry_per_registered_ch_datasource` test must use `install(...)` so `schemaPlus` is non-null.
3. Stub `asCalciteSchema(String, SchemaPlus)` instead of single-arg.

Replace the file with:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Set;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.CalciteSchemaProvider;
import org.opensearch.sql.storage.StorageEngine;

public class ClickHouseSchemaTest {

  @Test
  public void null_data_source_service_returns_empty_sub_schema_map() {
    ClickHouseSchema schema = new ClickHouseSchema(null);
    assertNotNull(schema.getSubSchemaMap());
    assertTrue(schema.getSubSchemaMap().isEmpty());
  }

  @Test
  public void no_registered_ch_datasources_returns_empty_sub_schema_map() {
    DataSourceService dss = mock(DataSourceService.class);
    DataSourceMetadata openSearchMd = mock(DataSourceMetadata.class);
    when(openSearchMd.getConnector()).thenReturn(DataSourceType.OPENSEARCH);
    when(dss.getDataSourceMetadata(true)).thenReturn(Set.of(openSearchMd));

    SchemaPlus root = CalciteSchema.createRootSchema(true, false).plus();
    ClickHouseSchema.install(root, dss);
    ClickHouseSchema schema = (ClickHouseSchema) root.getSubSchema(ClickHouseSchema.CLICKHOUSE_SCHEMA_NAME).unwrap(Schema.class);
    // Sub-schema lookup must go through Calcite's SchemaPlus wrapper.
    assertTrue(schema.getSubSchemaMap().isEmpty());
  }

  @Test
  public void sub_schema_map_contains_one_entry_per_registered_ch_datasource() {
    DataSourceService dss = mock(DataSourceService.class);
    DataSourceMetadata chMd = mock(DataSourceMetadata.class);
    when(chMd.getName()).thenReturn("my_ch");
    when(chMd.getConnector()).thenReturn(DataSourceType.CLICKHOUSE);
    when(dss.getDataSourceMetadata(true)).thenReturn(Set.of(chMd));

    Schema chSubSchema = mock(Schema.class);
    StorageEngine engine =
        mock(StorageEngine.class, withSettings().extraInterfaces(CalciteSchemaProvider.class));
    // Match any SchemaPlus — the instance is produced inside install().
    when(((CalciteSchemaProvider) engine).asCalciteSchema(eq("my_ch"), any(SchemaPlus.class)))
        .thenReturn(chSubSchema);

    DataSource ds = mock(DataSource.class);
    when(ds.getStorageEngine()).thenReturn(engine);
    when(dss.getDataSource("my_ch")).thenReturn(ds);

    SchemaPlus root = CalciteSchema.createRootSchema(true, false).plus();
    ClickHouseSchema.install(root, dss);
    ClickHouseSchema schema =
        (ClickHouseSchema)
            root.getSubSchema(ClickHouseSchema.CLICKHOUSE_SCHEMA_NAME).unwrap(Schema.class);

    assertEquals(1, schema.getSubSchemaMap().size());
    assertTrue(schema.getSubSchemaMap().containsKey("my_ch"));
    assertEquals(chSubSchema, schema.getSubSchemaMap().get("my_ch"));
  }
}
```

**Note on the `unwrap(Schema.class)` call**: Calcite's `SchemaPlus` extends `Schema` and implements `Wrapper`, so `schemaPlus.unwrap(Schema.class)` returns the underlying `ClickHouseSchema` the test needs. If this line fails at runtime (different Calcite behavior), we can alternatively keep a direct reference: assign `ClickHouseSchema node = new ClickHouseSchema(dss)` manually, then call `SchemaPlus added = root.add(...)` and `node.setSchemaPlus(added)` via a package-private setter. Keep the test simple first.

- [ ] **Step 5.2: Fix `ClickHousePlanSmokeTest`**

Replace the file with:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Map;
import java.util.Set;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.CalciteSchemaProvider;
import org.opensearch.sql.storage.StorageEngine;

public class ClickHousePlanSmokeTest {

  @Test
  public void ch_sub_schema_is_visible_to_rel_builder() {
    // 1. Mock DataSourceService so ClickHouseSchema discovers one CH datasource "my_ch".
    DataSourceService dss = mock(DataSourceService.class);
    DataSourceMetadata md = mock(DataSourceMetadata.class);
    when(md.getName()).thenReturn("my_ch");
    when(md.getConnector()).thenReturn(DataSourceType.CLICKHOUSE);
    when(dss.getDataSourceMetadata(true)).thenReturn(Set.of(md));

    // 2. Build a synthetic CH sub-schema with one database "analytics" and one table "events".
    SchemaPlus chSchemaRoot = CalciteSchema.createRootSchema(true, false).plus();
    chSchemaRoot.add(
        "analytics",
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of(
                "events",
                new AbstractTable() {
                  @Override
                  public RelDataType getRowType(RelDataTypeFactory tf) {
                    return tf.builder().add("id", tf.createSqlType(SqlTypeName.BIGINT)).build();
                  }
                });
          }
        });

    // 3. Wire the synthetic sub-schema into a mocked CalciteSchemaProvider-bearing StorageEngine.
    StorageEngine engine =
        mock(StorageEngine.class, withSettings().extraInterfaces(CalciteSchemaProvider.class));
    when(((CalciteSchemaProvider) engine).asCalciteSchema(eq("my_ch"), any(SchemaPlus.class)))
        .thenReturn(chSchemaRoot);

    DataSource ds = mock(DataSource.class);
    when(ds.getStorageEngine()).thenReturn(engine);
    when(dss.getDataSource("my_ch")).thenReturn(ds);

    // 4. Register ClickHouseSchema under a planning root via install() so schemaPlus is captured.
    SchemaPlus root = CalciteSchema.createRootSchema(true, false).plus();
    ClickHouseSchema.install(root, dss);

    FrameworkConfig cfg = Frameworks.newConfigBuilder().defaultSchema(root).build();
    RelBuilder rb = RelBuilder.create(cfg);

    // 5. RelBuilder.scan("ClickHouse","my_ch","analytics","events") must resolve the table.
    RelNode scan =
        rb.scan(ClickHouseSchema.CLICKHOUSE_SCHEMA_NAME, "my_ch", "analytics", "events").build();

    String plan = RelOptUtil.toString(scan);
    assertTrue(plan.contains("events"), () -> "Plan should reference events table:\n" + plan);
  }

  @Test
  public void schema_instance_is_ignored_when_data_source_service_is_null() {
    // Direct install of a ClickHouseSchema with no backing service is a no-op (empty sub-schema
    // map). Ensures the class is safe to instantiate in Framework setup.
    SchemaPlus root = CalciteSchema.createRootSchema(true, false).plus();
    ClickHouseSchema.install(root, null);
    // No assertion needed — absence of exception is the signal.
  }
}
```

- [ ] **Step 5.3: Run `:core:test`**

Run: `JAVA_HOME=/home/songkant/jdks/amazon-corretto-21.0.10.7.1-linux-x64 ./gradlew :core:test --tests "org.opensearch.sql.calcite.ClickHouseSchemaTest" --tests "org.opensearch.sql.calcite.ClickHousePlanSmokeTest"`

Expected: all 5 tests PASS.

If `null_data_source_service_returns_empty_sub_schema_map` or `no_registered_ch_datasources_returns_empty_sub_schema_map` fail with a `NullPointerException` on `unwrap(Schema.class)`, it means `SchemaPlus.unwrap(Schema.class)` does not return the underlying schema in this Calcite version. Fallback: change the assertion to construct the schema, install, and hold the original reference:

```java
ClickHouseSchema schema = new ClickHouseSchema(dss);
SchemaPlus root = CalciteSchema.createRootSchema(true, false).plus();
// Exercise install's wiring manually so we keep the reference:
SchemaPlus added = root.add(ClickHouseSchema.CLICKHOUSE_SCHEMA_NAME, schema);
schema.setSchemaPlus(added);  // requires adding a package-private setter if not present
assertTrue(schema.getSubSchemaMap().isEmpty());
```

If you take the fallback, also add a package-private setter in `ClickHouseSchema`:

```java
/** Test-only wiring; production code goes through {@link #install}. */
void setSchemaPlus(SchemaPlus schemaPlus) {
  this.schemaPlus = schemaPlus;
}
```

---

## Task 6: Update `ClickHouseStorageEngineTest`

**Files:**
- Modify: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngineTest.java`

- [ ] **Step 6.1: Update `asCalciteSchema_returns_non_null_schema_with_declared_subschema`**

Only the `asCalciteSchema` invocation needs to change (pass a `SchemaPlus`). Other tests in the file are unaffected.

In the test `asCalciteSchema_returns_non_null_schema_with_declared_subschema`, replace the body's last 3 lines (around line 77–81):

Old:
```java
    ClickHouseStorageEngine engine = new ClickHouseStorageEngine(cfg, client);
    org.apache.calcite.schema.Schema s = engine.asCalciteSchema("my_ch");
    assertNotNull(s);
    assertNotNull(s.getSubSchema("analytics"));
    assertNotNull(s.getSubSchema("analytics").getTable("events"));
```

New:
```java
    ClickHouseStorageEngine engine = new ClickHouseStorageEngine(cfg, client);
    org.apache.calcite.schema.SchemaPlus parent =
        org.apache.calcite.jdbc.CalciteSchema.createRootSchema(true, false).plus();
    org.apache.calcite.schema.Schema s = engine.asCalciteSchema("my_ch", parent);
    assertNotNull(s);
    assertNotNull(s.getSubSchema("analytics"));
    assertNotNull(s.getSubSchema("analytics").getTable("events"));
```

- [ ] **Step 6.2: Run `:clickhouse:test`**

Run: `JAVA_HOME=/home/songkant/jdks/amazon-corretto-21.0.10.7.1-linux-x64 ./gradlew :clickhouse:test`

Expected: all tests PASS.

- [ ] **Step 6.3: Run `:core:test` in full**

Run: `JAVA_HOME=/home/songkant/jdks/amazon-corretto-21.0.10.7.1-linux-x64 ./gradlew :core:test`

Expected: all tests PASS (no regression in unrelated tests).

- [ ] **Step 6.4: Commit the full refactor as one logical change**

```bash
cd /home/songkant/workspace/sql/.worktrees/clickhouse-datasource
JAVA_HOME=/home/songkant/jdks/amazon-corretto-21.0.10.7.1-linux-x64 ./gradlew spotlessApply
git add \
  core/src/main/java/org/opensearch/sql/storage/CalciteSchemaProvider.java \
  core/src/main/java/org/opensearch/sql/calcite/ClickHouseSchema.java \
  core/src/main/java/org/opensearch/sql/executor/QueryService.java \
  core/src/test/java/org/opensearch/sql/calcite/ClickHouseSchemaTest.java \
  core/src/test/java/org/opensearch/sql/calcite/ClickHousePlanSmokeTest.java \
  clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseConvention.java \
  clickhouse/src/main/java/org/apache/calcite/adapter/jdbc/ClickHouseJdbcSchemaBuilder.java \
  clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseSchemaFactory.java \
  clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngine.java \
  clickhouse/src/test/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngineTest.java
git commit -m "fix(clickhouse): build JdbcConvention expression via Schemas.subSchemaExpression

The convention's expression must be a Calcite linq4j tree that resolves to
the per-datasource JdbcSchema at codegen time (JdbcToEnumerableConverter
wraps it in Schemas.unwrap(expr, DataSource.class) when emitting the
executor). We were passing a bare ConstantExpression(String.class, name),
which generated \"literal\".unwrap(DataSource.class) Java — type-illegal,
hence the PLANNING_ERROR 'A method named \"unwrap\" is not declared'.

Thread the per-query SchemaPlus parent from QueryService down through a
new ClickHouseSchema.install(root, ds) helper (captures the returned
SchemaPlus in a volatile field) into ClickHouseSchemaFactory, build the
expression with Schemas.subSchemaExpression(parent, name, JdbcSchema.class),
and have the per-datasource wrapper schema override unwrap(Class) to expose
its inner JdbcSchema so .unwrap(DataSource.class) lands on a real Wrapper.

Tests updated to stub the new asCalciteSchema(String, SchemaPlus) signature
and to install ClickHouseSchema through the new helper.

Design: docs/superpowers/specs/2026-04-20-clickhouse-convention-expression-fix-design.md"
```

---

## Task 7: Add the new `ClickHouseConventionTest` (unit-level proof the expression resolves)

**Files:**
- Create: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseConventionTest.java`

This is the missing "does this actually work?" safety net — a unit test that proves, without spinning up an OpenSearch cluster, that the expression built by `ClickHouseSchemaFactory` resolves through a real `SchemaPlus` tree to the original `DataSource` we registered.

- [ ] **Step 7.1: Write the test**

Create `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseConventionTest.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.calcite;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import java.util.List;
import javax.sql.DataSource;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.config.ClickHouseColumnSpec;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;
import org.opensearch.sql.clickhouse.storage.ClickHouseSchemaFactory;

/**
 * Unit-level proof that the convention's expression resolves through a live {@link SchemaPlus}
 * tree back to the {@link DataSource} we registered. Reproduces the chain that Calcite's {@code
 * JdbcToEnumerableConverter} uses at codegen time: {@code
 * parent.getSubSchema(name).unwrap(JdbcSchema.class).unwrap(DataSource.class)}.
 */
public class ClickHouseConventionTest {

  @Test
  public void sub_schema_unwraps_to_jdbc_schema_and_data_source() {
    DataSource originalDs = mock(DataSource.class);

    // Build a minimal CH schema: one db "db1" with one table "t1".
    ClickHouseTableSpec.Schema spec =
        new ClickHouseTableSpec.Schema(
            List.of(
                new ClickHouseTableSpec.Database(
                    "db1",
                    List.of(
                        new ClickHouseTableSpec(
                            "t1",
                            List.of(new ClickHouseColumnSpec("c1", "Int64", "LONG")))))));

    // Mount under a synthetic Calcite root, mirroring QueryService.buildFrameworkConfig +
    // ClickHouseSchema.install + provider.asCalciteSchema(name, parent).
    SchemaPlus root = CalciteSchema.createRootSchema(true, false).plus();
    SchemaPlus chParent = root.add("ClickHouse", new org.apache.calcite.schema.impl.AbstractSchema() {});
    Schema perDatasource = ClickHouseSchemaFactory.build(chParent, "ds1", originalDs, spec);
    SchemaPlus subPlus = chParent.add("ds1", perDatasource);

    // Walk the chain Calcite's codegen walks.
    SchemaPlus resolved = chParent.getSubSchema("ds1");
    assertNotNull(resolved, "parent.getSubSchema(name) returned null");

    JdbcSchema asJdbc = resolved.unwrap(JdbcSchema.class);
    assertNotNull(asJdbc, "wrapper did not expose inner JdbcSchema via unwrap(JdbcSchema.class)");

    DataSource got = asJdbc.unwrap(DataSource.class);
    assertNotNull(got, "JdbcSchema.unwrap(DataSource.class) returned null");
    assertSame(originalDs, got, "resolved DataSource differs from the one we registered");
  }
}
```

- [ ] **Step 7.2: Run the new test**

Run: `JAVA_HOME=/home/songkant/jdks/amazon-corretto-21.0.10.7.1-linux-x64 ./gradlew :clickhouse:test --tests "org.opensearch.sql.clickhouse.calcite.ClickHouseConventionTest"`

Expected: PASS. If `resolved` is null, the wrapper's `getSubSchemaMap` doesn't return the sub-schema under the key we expect — double-check §5.2 / §5.3 in the design; usually means `ClickHouseSchemaFactory.build` returned a schema that didn't populate `getSubSchemaMap` correctly. If `asJdbc` is null, the wrapper's `unwrap` override is wrong — re-check Task 3.

- [ ] **Step 7.3: Commit**

```bash
JAVA_HOME=/home/songkant/jdks/amazon-corretto-21.0.10.7.1-linux-x64 ./gradlew spotlessApply
git add clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseConventionTest.java
git commit -m "test(clickhouse): unit-level proof that convention expression resolves

Walks parent.getSubSchema(name).unwrap(JdbcSchema.class).unwrap(DataSource.class)
against a real SchemaPlus tree and asserts the resolved DataSource is the one
originally passed to ClickHouseSchemaFactory.build. Closes the gap between
compile-time correctness and Calcite runtime expectations without needing an
OpenSearch cluster."
```

---

## Task 8: Remove `@Ignore` from the six query ITs

**Files:**
- Modify: `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHouseBasicQueryIT.java`
- Modify: `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHousePushdownIT.java`

- [ ] **Step 8.1: Un-`@Ignore` BasicQuery tests**

In `ClickHouseBasicQueryIT.java`, delete the `@Ignore("Pending Calcite convention fix: ...")` annotation blocks immediately above `head_returns_rows` and `filter_and_project_end_to_end` (both currently at the lines shown below — double-check the file, then remove only the `@Ignore(...)` lines, keeping `@Test`).

Currently:
```java
  @Test
  @Ignore(
      "Pending Calcite convention fix: JdbcTableScan is not converted to"
          + " JdbcToEnumerableConverter, so physical compilation fails with"
          + " `No method named \"unwrap\"`. Tracked in M5.")
  public void head_returns_rows() throws Exception {
```

After:
```java
  @Test
  public void head_returns_rows() throws Exception {
```

Same edit for `filter_and_project_end_to_end`.

- [ ] **Step 8.2: Drop the now-unused `Ignore` import**

At the top of `ClickHouseBasicQueryIT.java`, remove the line:
```java
import org.junit.Ignore;
```
…if no other `@Ignore` remains in the file.

- [ ] **Step 8.3: Un-`@Ignore` Pushdown tests**

In `ClickHousePushdownIT.java`, remove the same `@Ignore(...)` annotation block above all four tests: `filter_returns_only_matching_rows`, `project_drops_unwanted_columns`, `sort_and_limit_return_top_n_descending`, `explain_shows_jdbc_convention_nodes`.

- [ ] **Step 8.4: Drop unused `Ignore` import**

Remove `import org.junit.Ignore;` from the top of `ClickHousePushdownIT.java`.

- [ ] **Step 8.5: Stop any running Gradle daemon (pre-flight)**

Run: `JAVA_HOME=/home/songkant/jdks/amazon-corretto-21.0.10.7.1-linux-x64 ./gradlew --stop`

Rationale: the in-tree `SpawnProcessTask` decorator fails on the 2nd `startClickhouse` in the same daemon (separate bug). Fresh daemon per IT run avoids that.

- [ ] **Step 8.6: Clean up any orphan ClickHouse processes (defensive)**

Run: `pgrep -af "integ-test/bin/clickhouse/config.xml" | grep -v grep | awk '{print $1}' | xargs -r kill -TERM; sleep 2; rm -f /home/songkant/workspace/sql/.worktrees/clickhouse-datasource/.clickhouse.pid.lock`

The `startClickhouse.doFirst` cleanup will also handle this, but being explicit avoids relying on it mid-plan.

- [ ] **Step 8.7: Run the full CH IT suite**

Run:
```
cd /home/songkant/workspace/sql/.worktrees/clickhouse-datasource && \
JAVA_HOME=/home/songkant/jdks/amazon-corretto-21.0.10.7.1-linux-x64 ./gradlew :integ-test:integTest \
  -DuseClickhouseBinary=true \
  -DignorePrometheus=true \
  --tests "org.opensearch.sql.clickhouse.*"
```

Expected: `BUILD SUCCESSFUL`. Then verify test counts:

```bash
for f in integ-test/build/test-results/integTest/TEST-org.opensearch.sql.clickhouse*.xml; do
  head -3 "$f" | grep -oE 'name="[^"]*" tests="[0-9]+" skipped="[0-9]+" failures="[0-9]+" errors="[0-9]+"'
done
```

Expected output (all three files):
- `ClickHouseBasicQueryIT`: `tests="2" skipped="0" failures="0" errors="0"`
- `ClickHousePushdownIT`: `tests="4" skipped="0" failures="0" errors="0"`
- `ClickHouseRegistrationIT`: `tests="3" skipped="0" failures="0" errors="0"`

If any fail: the new error will be something **other than** `No method named "unwrap"` (that path is fixed). Investigate via `cat integ-test/build/test-results/integTest/TEST-<class>.xml` and `grep -A30 "PLANNING_ERROR\|Exception" integ-test/build/testclusters/integTest-0/logs/integTest.log`.

- [ ] **Step 8.8: Commit the un-ignore**

```bash
JAVA_HOME=/home/songkant/jdks/amazon-corretto-21.0.10.7.1-linux-x64 ./gradlew spotlessApply
git add \
  integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHouseBasicQueryIT.java \
  integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHousePushdownIT.java
git commit -m "test(clickhouse): un-ignore 6 query ITs after convention expression fix

Binary-mode run with all 9 CH ITs: tests=9 skipped=0 failures=0."
```

---

## Task 9: Update design doc with verification outcome

**Files:**
- Modify: `docs/superpowers/specs/2026-04-20-clickhouse-convention-expression-fix-design.md` (on `main` branch, not worktree)

- [ ] **Step 9.1: Switch to main, add a verification note**

```bash
cd /home/songkant/workspace/sql
```

Append to the spec (after §9 "Out of scope") a new §10 section. Use the `Edit` tool to locate the end of the file (the last paragraph ends with "Calcite dialect extension (still the M3-era whitelist).") and append:

```markdown

## 10. Verification (record when implementation lands)

Branch `feat/clickhouse-datasource` commits `<FIRST_COMMIT_HASH>` + `<SECOND_COMMIT_HASH>` + `<THIRD_COMMIT_HASH>` (core refactor + new unit test + un-ignore).

- `./gradlew :core:test :clickhouse:test` — green, no regressions.
- `./gradlew :clickhouse:test --tests "*ClickHouseConventionTest"` — new unit-level proof that `parent.getSubSchema(name).unwrap(JdbcSchema.class).unwrap(DataSource.class)` resolves to the original `DataSource`.
- `./gradlew :integ-test:integTest -DuseClickhouseBinary=true -DignorePrometheus=true --tests "org.opensearch.sql.clickhouse.*"` — tests=9, skipped=0, failures=0.
```

Replace `<FIRST_COMMIT_HASH>`, `<SECOND_COMMIT_HASH>`, `<THIRD_COMMIT_HASH>` with the actual short hashes obtained via:

```bash
cd /home/songkant/workspace/sql/.worktrees/clickhouse-datasource && git log --oneline -4
```

The most recent three `feat/clickhouse-datasource` commits in order (oldest-to-newest among the three) are the ones to reference.

- [ ] **Step 9.2: Commit**

```bash
cd /home/songkant/workspace/sql
git add docs/superpowers/specs/2026-04-20-clickhouse-convention-expression-fix-design.md
git commit -m "docs(clickhouse): record verification of convention expression fix"
```

---

## Task 10: Push both branches

- [ ] **Step 10.1: Push main**

```bash
cd /home/songkant/workspace/sql && git push origin main
```

- [ ] **Step 10.2: Push feature branch**

```bash
cd /home/songkant/workspace/sql/.worktrees/clickhouse-datasource && git push origin feat/clickhouse-datasource
```

Expected: both pushes succeed. The feature branch now has 3 new commits on top of `0a842256d`.

---

## Self-review (completed during plan writing)

- **Spec coverage:** Every section of the spec is covered by a task. §2 Root cause → Task 2. §3 Constraints: lazy expression (Task 4), per-datasource convention preserved (Task 2), no schema re-install (Task 4.4), both IT classes (Task 8), unit tests green (Tasks 5–7). §4 Option A chosen — all subsections of §5 mapped: 5.1→Task 2, 5.2→Task 4, 5.3→Task 3, 5.4→Task 1, 5.5→Task 4.4, 5.6→Task 4.5. §6 Test strategy: 6.1→Task 7, 6.2→Tasks 5–6, 6.3→Task 8, 6.4→Task 8.7 + Tasks 5.3/6.3. §7 Change surface → cross-checked against tasks' "Files" sections.
- **Placeholder scan:** no TBD / TODO / "add appropriate error handling". All commands have expected output. All code blocks show real content.
- **Type consistency:** `asCalciteSchema(String datasourceName, SchemaPlus parentSchema)` used consistently in Tasks 1, 4.2, 5, 6. `ClickHouseSchema.install(SchemaPlus, DataSourceService)` used consistently in Tasks 4.5, 5, 6. `ClickHouseConvention.of(String, Expression)` in Tasks 2, 4.1. `ClickHouseSchemaFactory.build(SchemaPlus, String, DataSource, Schema)` in Tasks 4.1, 4.2, 7.
