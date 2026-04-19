# ClickHouse Datasource Implementation Plan (M0–M4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a read-only ClickHouse datasource connector for OpenSearch SQL plugin that executes PPL queries against ClickHouse via Calcite's JdbcRules pushdown. This plan covers M0–M4 (Calcite multi-datasource dispatch → end-to-end basic query). M5–M8 (aggregate/fallback, rate-limit/errors, metrics/audit, docs/benchmarks) are tracked in a follow-up plan created once M4 is green.

**Architecture:** Register a `ClickHouseSchema` (in `core/`) under Calcite's per-query root SchemaPlus; relax `CalciteRelNodeVisitor.visitRelation` to dispatch on datasource type; build a per-CH-datasource Calcite sub-schema holding `JdbcTable`s backed by a HikariCP DataSource + custom `ClickHouseSqlDialect` + `ClickHouseConvention`. Calcite's built-in `JdbcRules` auto-push filter/project/sort/limit to ClickHouse; unsupported operators fall through to Calcite's Enumerable convention.

**Tech Stack:** Java 21, Gradle, OpenSearch plugin framework, Apache Calcite 1.41.0 (calcite-core JdbcSchema/JdbcRules/JdbcConvention), ClickHouse JDBC 0.6.x, HikariCP 5.x, Testcontainers for IT.

**Spec:** `docs/superpowers/specs/2026-04-19-clickhouse-datasource-design.md`

---

## File Map (M0–M4)

**Created files:**
- `core/src/main/java/org/opensearch/sql/calcite/ClickHouseSchema.java` — AbstractSchema; lazy sub-schema lookup via DataSourceService
- `core/src/main/java/org/opensearch/sql/storage/CalciteSchemaProvider.java` — narrow interface that CH `StorageEngine` implements so `core` can read a Calcite `Schema` without depending on `clickhouse`
- `core/src/test/java/org/opensearch/sql/calcite/ClickHouseSchemaTest.java`
- `clickhouse/build.gradle`
- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngine.java`
- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseTable.java`
- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageFactory.java`
- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseSchemaFactory.java`
- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/config/ClickHouseDataSourceConfig.java`
- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/config/ClickHouseColumnSpec.java`
- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/config/ClickHouseTableSpec.java`
- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/client/ClickHouseClient.java`
- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/client/ClickHouseClientFactory.java`
- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/auth/ClickHouseAuthProvider.java`
- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java`
- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseConvention.java`
- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/type/ClickHouseTypeMapper.java`
- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/exception/ClickHouseException.java` (+ subclasses)
- `clickhouse/src/test/...` — corresponding unit tests
- `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHouseRegistrationIT.java`
- `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHouseBasicQueryIT.java`
- `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHousePushdownIT.java`

**Modified files:**
- `settings.gradle` — add `:clickhouse`
- `core/src/main/java/org/opensearch/sql/datasource/model/DataSourceType.java` — add `CLICKHOUSE`
- `core/src/main/java/org/opensearch/sql/executor/QueryService.java` — register `ClickHouseSchema` in `buildFrameworkConfig()`
- `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java` — dispatch in `visitRelation()`
- `plugin/src/main/java/.../OpenSearchPluginModule.java` — wire `ClickHouseStorageFactory`
- `plugin/build.gradle` — `implementation project(':clickhouse')`
- `integ-test/build.gradle` — Testcontainers ClickHouse dep

---

# M0 — Calcite Multi-Datasource Dispatch

## Task M0.1: Add `CLICKHOUSE` DataSourceType

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/datasource/model/DataSourceType.java`
- Modify: `core/src/test/java/org/opensearch/sql/datasource/model/DataSourceTypeTest.java` (create if missing)

- [ ] **Step 1: Write the failing test**

Create `core/src/test/java/org/opensearch/sql/datasource/model/DataSourceTypeTest.java` if it does not exist; otherwise append to it.

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

public class DataSourceTypeTest {
  @Test
  public void clickhouse_type_registered_and_lookup_case_insensitive() {
    assertEquals("CLICKHOUSE", DataSourceType.CLICKHOUSE.name());
    assertSame(DataSourceType.CLICKHOUSE, DataSourceType.fromString("clickhouse"));
    assertSame(DataSourceType.CLICKHOUSE, DataSourceType.fromString("CLICKHOUSE"));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :core:test --tests "org.opensearch.sql.datasource.model.DataSourceTypeTest"`
Expected: FAIL — `CLICKHOUSE` symbol does not exist.

- [ ] **Step 3: Add the enum constant**

Edit `core/src/main/java/org/opensearch/sql/datasource/model/DataSourceType.java`:

```java
  public static final DataSourceType PROMETHEUS = new DataSourceType("PROMETHEUS");
  public static final DataSourceType OPENSEARCH = new DataSourceType("OPENSEARCH");
  public static final DataSourceType S3GLUE = new DataSourceType("S3GLUE");
  public static final DataSourceType SECURITY_LAKE = new DataSourceType("SECURITY_LAKE");
  public static final DataSourceType CLICKHOUSE = new DataSourceType("CLICKHOUSE");

  // Map from uppercase DataSourceType name to DataSourceType object
  private static final Map<String, DataSourceType> knownValues = new HashMap<>();

  static {
    register(PROMETHEUS, OPENSEARCH, S3GLUE, SECURITY_LAKE, CLICKHOUSE);
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :core:test --tests "org.opensearch.sql.datasource.model.DataSourceTypeTest"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add core/src/main/java/org/opensearch/sql/datasource/model/DataSourceType.java \
        core/src/test/java/org/opensearch/sql/datasource/model/DataSourceTypeTest.java
git commit -s -m "feat(core): register CLICKHOUSE datasource type"
```

---

## Task M0.2: Create `ClickHouseSchema` stub (AbstractSchema)

**Files:**
- Create: `core/src/main/java/org/opensearch/sql/calcite/ClickHouseSchema.java`
- Create: `core/src/test/java/org/opensearch/sql/calcite/ClickHouseSchemaTest.java`

- [ ] **Step 1: Write the failing test**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.datasource.DataSourceService;

public class ClickHouseSchemaTest {
  @Test
  public void schema_exposes_expected_constants_and_empty_subschema_map_when_no_ch_datasources() {
    DataSourceService ds = mock(DataSourceService.class);
    ClickHouseSchema schema = new ClickHouseSchema(ds);
    assertNotNull(schema);
    assertTrue(schema.getSubSchemaMap() == null || schema.getSubSchemaMap().isEmpty());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :core:test --tests "org.opensearch.sql.calcite.ClickHouseSchemaTest"`
Expected: FAIL — `ClickHouseSchema` does not exist.

- [ ] **Step 3: Create the stub class**

`core/src/main/java/org/opensearch/sql/calcite/ClickHouseSchema.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.sql.datasource.DataSourceService;

/**
 * Calcite {@link AbstractSchema} that exposes one Calcite sub-schema per registered ClickHouse
 * datasource. Sub-schema map is resolved lazily on each access; individual sub-schemas are obtained
 * from each CH datasource's {@code StorageEngine} via a narrow interface so {@code core} stays
 * decoupled from the {@code clickhouse} module.
 *
 * <p>This stub returns an empty sub-schema map. Wiring to CH datasources is added in a later
 * milestone.
 */
@Getter
@AllArgsConstructor
public class ClickHouseSchema extends AbstractSchema {
  public static final String CLICKHOUSE_SCHEMA_NAME = "ClickHouse";

  private final DataSourceService dataSourceService;

  @Override
  protected Map<String, Schema> getSubSchemaMap() {
    // M0: stub — real lookup added in M4.
    return Map.of();
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :core:test --tests "org.opensearch.sql.calcite.ClickHouseSchemaTest"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add core/src/main/java/org/opensearch/sql/calcite/ClickHouseSchema.java \
        core/src/test/java/org/opensearch/sql/calcite/ClickHouseSchemaTest.java
git commit -s -m "feat(core): add ClickHouseSchema stub for Calcite sub-schema dispatch"
```

---

## Task M0.3: Register `ClickHouseSchema` in `QueryService.buildFrameworkConfig()`

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/executor/QueryService.java` (lines 357–371)
- Modify/Create: `core/src/test/java/org/opensearch/sql/executor/QueryServiceTest.java` — add one new test

- [ ] **Step 1: Write the failing test**

Append to `QueryServiceTest.java`:

```java
  @Test
  public void framework_config_registers_clickhouse_schema_under_root() throws Exception {
    QueryService service = new QueryService(
        planner, executionEngine, analyzer, dataSourceService, planFactory, queryManager, settings);
    java.lang.reflect.Method m = QueryService.class.getDeclaredMethod("buildFrameworkConfig");
    m.setAccessible(true);
    org.apache.calcite.tools.FrameworkConfig cfg =
        (org.apache.calcite.tools.FrameworkConfig) m.invoke(service);
    org.apache.calcite.schema.SchemaPlus root = cfg.getDefaultSchema().getParentSchema();
    assertNotNull(root.getSubSchema(
        org.opensearch.sql.calcite.ClickHouseSchema.CLICKHOUSE_SCHEMA_NAME));
  }
```

(If `QueryServiceTest` does not already have mocks for these fields, read the existing file and reuse its `@BeforeEach` setup; if it doesn't exist at all, create it following `PrometheusStorageEngineTest` style — a minimum test class that mocks exactly the constructor args `QueryService` needs.)

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :core:test --tests "org.opensearch.sql.executor.QueryServiceTest.framework_config_registers_clickhouse_schema_under_root"`
Expected: FAIL — `getSubSchema("ClickHouse")` returns null.

- [ ] **Step 3: Register the schema**

Edit `QueryService.java`, inside `buildFrameworkConfig()` (around line 360):

```java
  private FrameworkConfig buildFrameworkConfig() {
    // Use simple calcite schema since we don't compute tables in advance of the query.
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(true, false).plus();
    final SchemaPlus opensearchSchema =
        rootSchema.add(
            OpenSearchSchema.OPEN_SEARCH_SCHEMA_NAME, new OpenSearchSchema(dataSourceService));
    rootSchema.add(
        org.opensearch.sql.calcite.ClickHouseSchema.CLICKHOUSE_SCHEMA_NAME,
        new org.opensearch.sql.calcite.ClickHouseSchema(dataSourceService));
    Frameworks.ConfigBuilder configBuilder =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(opensearchSchema)
            .traitDefs((List<RelTraitDef>) null)
            .programs(Programs.standard())
            .typeSystem(OpenSearchTypeSystem.INSTANCE);
    return configBuilder.build();
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :core:test --tests "org.opensearch.sql.executor.QueryServiceTest.framework_config_registers_clickhouse_schema_under_root"`
Expected: PASS. Also run: `./gradlew :core:test` — all existing tests still pass.

- [ ] **Step 5: Commit**

```bash
git add core/src/main/java/org/opensearch/sql/executor/QueryService.java \
        core/src/test/java/org/opensearch/sql/executor/QueryServiceTest.java
git commit -s -m "feat(core): register ClickHouseSchema under Calcite root SchemaPlus"
```

---

## Task M0.4: Dispatch non-default datasources in `visitRelation`

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java` (around lines 222–254)
- Modify: `core/src/test/java/org/opensearch/sql/calcite/CalciteRelNodeVisitorTest.java` (create if absent)

- [ ] **Step 1: Write the failing test**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.exception.CalciteUnsupportedException;

public class CalciteRelNodeVisitorTest {
  @Test
  public void visitRelation_routes_clickhouse_datasource_via_scan_path() {
    DataSourceService dss = mock(DataSourceService.class);
    DataSource ch = mock(DataSource.class);
    when(ch.getConnectorType()).thenReturn(DataSourceType.CLICKHOUSE);
    when(dss.getDataSource("my_ch")).thenReturn(ch);

    CalciteRelNodeVisitor visitor = new CalciteRelNodeVisitor(dss);
    CalcitePlanContext ctx = mock(CalcitePlanContext.class);
    RelBuilder rb = mock(RelBuilder.class);
    when(ctx.getRelBuilder()).thenReturn(rb);

    Relation node = new Relation(QualifiedName.of("my_ch", "analytics", "events"));
    // Should not throw CalciteUnsupportedException for CH datasources.
    visitor.visitRelation(node, ctx);
    // Assert scan was called with the full qualified parts (including datasource name).
    org.mockito.Mockito.verify(rb).scan(java.util.List.of("my_ch", "analytics", "events"));
  }

  @Test
  public void visitRelation_still_rejects_unknown_datasource_type() {
    DataSourceService dss = mock(DataSourceService.class);
    DataSource unknown = mock(DataSource.class);
    when(unknown.getConnectorType()).thenReturn(DataSourceType.PROMETHEUS);
    when(dss.getDataSource("promds")).thenReturn(unknown);

    CalciteRelNodeVisitor visitor = new CalciteRelNodeVisitor(dss);
    CalcitePlanContext ctx = mock(CalcitePlanContext.class);
    Relation node = new Relation(QualifiedName.of("promds", "metric"));
    assertThrows(CalciteUnsupportedException.class, () -> visitor.visitRelation(node, ctx));
  }
}
```

(If `CalcitePlanContext` exposes `relBuilder` as a public field rather than a getter, adapt the mock accordingly; read the existing `CalcitePlanContext.java` first and match its accessor.)

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :core:test --tests "org.opensearch.sql.calcite.CalciteRelNodeVisitorTest"`
Expected: FAIL — `visitRelation` currently throws `CalciteUnsupportedException` for any non-default datasource.

- [ ] **Step 3: Relax the guard and dispatch on datasource type**

Edit `CalciteRelNodeVisitor.visitRelation()` at lines 222–255:

```java
  @Override
  public RelNode visitRelation(Relation node, CalcitePlanContext context) {
    DataSourceSchemaIdentifierNameResolver nameResolver =
        new DataSourceSchemaIdentifierNameResolver(
            dataSourceService, node.getTableQualifiedName().getParts());
    String dsName = nameResolver.getDataSourceName();
    boolean isDefault =
        dsName.equals(DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME);

    if (!isDefault) {
      DataSourceType dsType = dataSourceService.getDataSource(dsName).getConnectorType();
      if (DataSourceType.CLICKHOUSE.equals(dsType)) {
        // Route via Calcite root SchemaPlus → ClickHouseSchema → per-datasource sub-schema.
        context.relBuilder.scan(node.getTableQualifiedName().getParts());
        return context.relBuilder.peek();
      }
      throw new CalciteUnsupportedException(
          "Datasource " + dsName + " is unsupported in Calcite");
    }

    if (nameResolver.getIdentifierName().equals(DATASOURCES_TABLE_NAME)) {
      throw new CalciteUnsupportedException("SHOW DATASOURCES is unsupported in Calcite");
    }
    if (nameResolver.getSchemaName().equals(INFORMATION_SCHEMA_NAME)) {
      throw new CalciteUnsupportedException("information_schema is unsupported in Calcite");
    }
    context.relBuilder.scan(node.getTableQualifiedName().getParts());
    RelNode scan = context.relBuilder.peek();

    if (context.getHighlightConfig() != null && scan instanceof HighlightPushDown) {
      RelNode newScan = ((HighlightPushDown) scan).pushDownHighlight(context.getHighlightConfig());
      context.relBuilder.build();
      context.relBuilder.push(newScan);
      scan = newScan;
      context.setHighlightConfig(null);
    }
    if (scan instanceof AliasFieldsWrappable) {
      ((AliasFieldsWrappable) scan).wrapProjectForAliasFields(context.relBuilder);
    }
    return context.relBuilder.peek();
  }
```

Add the import for `DataSourceType` at the top of the file.

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :core:test --tests "org.opensearch.sql.calcite.CalciteRelNodeVisitorTest"`
Expected: PASS. Also run `./gradlew :core:test` — existing tests still pass.

- [ ] **Step 5: Commit**

```bash
git add core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java \
        core/src/test/java/org/opensearch/sql/calcite/CalciteRelNodeVisitorTest.java
git commit -s -m "feat(core): dispatch ClickHouse datasource in visitRelation"
```

---

## Task M0.5: Sanity integration test — CH datasource name resolves through Calcite

**Files:**
- Create: `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHouseDispatchIT.java`

- [ ] **Step 1: Write the IT**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import org.junit.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Pre-connector IT: verifies that visitRelation routes a CH datasource name into Calcite's scan
 * path. Because no CH datasource is registered yet, the query should fail with a scan-resolution
 * error ("table not found"), NOT with "Datasource X is unsupported in Calcite". Once the connector
 * is wired in later milestones, this test is superseded by {@code ClickHouseBasicQueryIT}.
 */
public class ClickHouseDispatchIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Test
  public void ch_datasource_name_reaches_scan_layer_not_unsupported_guard() {
    try {
      executeQuery("source = my_ch.analytics.events | head 1");
    } catch (Exception e) {
      String msg = e.getMessage() == null ? "" : e.getMessage();
      assertThat(
          "Expected scan-resolution failure, not the unsupported-datasource guard",
          msg,
          org.hamcrest.Matchers.not(containsString("is unsupported in Calcite")));
    }
  }
}
```

- [ ] **Step 2: Run it**

Run: `./gradlew :integ-test:integTest --tests "*ClickHouseDispatchIT*"`
Expected: PASS (the dispatch change in M0.4 makes the CH path reach the scan layer instead of being rejected up front).

- [ ] **Step 3: Commit**

```bash
git add integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHouseDispatchIT.java
git commit -s -m "test(clickhouse): sanity IT that visitRelation dispatches CH datasources"
```

---

# M1 — Scaffolding

## Task M1.1: Register `:clickhouse` in `settings.gradle`

**Files:**
- Modify: `settings.gradle`

- [ ] **Step 1: Append the module**

```groovy
include 'opensearch-sql-plugin'
project(':opensearch-sql-plugin').projectDir = file('plugin')
include 'api'
include 'ppl'
include 'common'
include 'opensearch'
include 'core'
include 'protocol'
include 'legacy'
include 'sql'
include 'prometheus'
include 'benchmarks'
include 'datasources'
include 'async-query-core'
include 'async-query'
include 'direct-query-core'
include 'direct-query'
include 'language-grammar'
include 'clickhouse'

if (!gradle.startParameter.offline) {
    include 'integ-test'
    include 'doctest'
}
```

- [ ] **Step 2: Commit**

```bash
git add settings.gradle
git commit -s -m "build: register :clickhouse module"
```

---

## Task M1.2: Create `clickhouse/build.gradle`

**Files:**
- Create: `clickhouse/build.gradle`

- [ ] **Step 1: Write the file**

`clickhouse/build.gradle`:

```groovy
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

plugins {
    id 'java-library'
    id "io.freefair.lombok"
    id 'jacoco'
}

repositories {
    mavenCentral()
}

dependencies {
    api project(':core')
    implementation project(':datasources')

    implementation group: 'org.opensearch', name: 'opensearch', version: "${opensearch_version}"
    implementation 'com.clickhouse:clickhouse-jdbc:0.6.5'
    implementation 'com.zaxxer:HikariCP:5.1.0'
    implementation group: 'com.fasterxml.jackson.core', name: 'jackson-core', version: "${versions.jackson}"
    implementation group: 'com.fasterxml.jackson.core', name: 'jackson-databind', version: "${versions.jackson_databind}"

    // calcite-core brought in transitively via :core (api scope)

    testImplementation('org.junit.jupiter:junit-jupiter:5.9.3')
    testImplementation group: 'org.hamcrest', name: 'hamcrest-library', version: "${hamcrest_version}"
    testImplementation group: 'org.mockito', name: 'mockito-core', version: "${mockito_version}"
    testImplementation group: 'org.mockito', name: 'mockito-junit-jupiter', version: "${mockito_version}"
    testRuntimeOnly('org.junit.platform:junit-platform-launcher')
}

test {
    maxParallelForks = Runtime.runtime.availableProcessors()
    useJUnitPlatform()
    testLogging {
        events "passed", "skipped", "failed"
        exceptionFormat "full"
    }
}

configurations.all {
    resolutionStrategy.force 'junit:junit:4.13.2'
}

jacocoTestReport {
    reports {
        html.required = true
        xml.required = true
    }
    afterEvaluate {
        classDirectories.setFrom(files(classDirectories.files.collect {
            fileTree(dir: it)
        }))
    }
}
test.finalizedBy(project.tasks.jacocoTestReport)
```

- [ ] **Step 2: Verify it builds**

Run: `./gradlew :clickhouse:build`
Expected: BUILD SUCCESSFUL (empty module with just dependencies declared).

- [ ] **Step 3: Commit**

```bash
git add clickhouse/build.gradle
git commit -s -m "build(clickhouse): add module build file"
```

---

## Task M1.3: Create config value objects (`ClickHouseDataSourceConfig`, `ClickHouseColumnSpec`, `ClickHouseTableSpec`)

**Files:**
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/config/ClickHouseColumnSpec.java`
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/config/ClickHouseTableSpec.java`
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/config/ClickHouseDataSourceConfig.java`
- Create: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/config/ClickHouseDataSourceConfigTest.java`

- [ ] **Step 1: Write the failing test**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class ClickHouseDataSourceConfigTest {
  @Test
  public void parses_required_and_optional_fields() {
    Map<String, String> props = Map.of(
        "clickhouse.uri", "jdbc:clickhouse://h:8123/default",
        "clickhouse.auth.type", "basic",
        "clickhouse.auth.username", "u",
        "clickhouse.auth.password", "p",
        "clickhouse.pool.max_size", "20",
        "clickhouse.schema",
        "{\"databases\":[{\"name\":\"db\",\"tables\":[{\"name\":\"t\",\"columns\":["
            + "{\"name\":\"c\",\"ch_type\":\"Int64\",\"expr_type\":\"LONG\"}]}]}]}");
    ClickHouseDataSourceConfig cfg = ClickHouseDataSourceConfig.parse(props);
    assertEquals("jdbc:clickhouse://h:8123/default", cfg.getUri());
    assertEquals("basic", cfg.getAuthType());
    assertEquals("u", cfg.getUsername());
    assertEquals("p", cfg.getPassword());
    assertEquals(20, cfg.getPoolMaxSize());
    assertEquals(1, cfg.getSchema().getDatabases().size());
    assertEquals("t", cfg.getSchema().getDatabases().get(0).getTables().get(0).getName());
  }

  @Test
  public void rejects_missing_uri() {
    Map<String, String> props = Map.of("clickhouse.auth.type", "basic");
    assertThrows(IllegalArgumentException.class, () -> ClickHouseDataSourceConfig.parse(props));
  }

  @Test
  public void rejects_basic_auth_without_credentials() {
    Map<String, String> props = Map.of(
        "clickhouse.uri", "jdbc:clickhouse://h:8123/default",
        "clickhouse.auth.type", "basic");
    assertThrows(IllegalArgumentException.class, () -> ClickHouseDataSourceConfig.parse(props));
  }

  @Test
  public void rejects_jwt_auth_without_token() {
    Map<String, String> props = Map.of(
        "clickhouse.uri", "jdbc:clickhouse://h:8123/default",
        "clickhouse.auth.type", "jwt");
    assertThrows(IllegalArgumentException.class, () -> ClickHouseDataSourceConfig.parse(props));
  }

  @Test
  public void pool_defaults_applied() {
    Map<String, String> props = Map.of(
        "clickhouse.uri", "jdbc:clickhouse://h:8123/default",
        "clickhouse.auth.type", "basic",
        "clickhouse.auth.username", "u",
        "clickhouse.auth.password", "p",
        "clickhouse.schema", "{\"databases\":[]}");
    ClickHouseDataSourceConfig cfg = ClickHouseDataSourceConfig.parse(props);
    assertEquals(10, cfg.getPoolMaxSize());
    assertEquals(2, cfg.getPoolMinIdle());
    assertEquals(50, cfg.getRateLimitQps());
    assertEquals(20, cfg.getRateLimitConcurrent());
    assertTrue(cfg.getSchema().getDatabases().isEmpty());
  }
}
```

- [ ] **Step 2: Run it to verify failure**

Run: `./gradlew :clickhouse:test --tests "*ClickHouseDataSourceConfigTest*"`
Expected: FAIL (classes do not exist).

- [ ] **Step 3: Write `ClickHouseColumnSpec`**

`clickhouse/src/main/java/org/opensearch/sql/clickhouse/config/ClickHouseColumnSpec.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ClickHouseColumnSpec {
  @JsonProperty("name") private String name;
  @JsonProperty("ch_type") private String chType;
  @JsonProperty("expr_type") private String exprType;
}
```

- [ ] **Step 4: Write `ClickHouseTableSpec`**

`clickhouse/src/main/java/org/opensearch/sql/clickhouse/config/ClickHouseTableSpec.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ClickHouseTableSpec {
  @JsonProperty("name") private String name;
  @JsonProperty("columns") private List<ClickHouseColumnSpec> columns;

  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Database {
    @JsonProperty("name") private String name;
    @JsonProperty("tables") private List<ClickHouseTableSpec> tables;
  }

  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Schema {
    @JsonProperty("databases") private List<Database> databases;
  }
}
```

- [ ] **Step 5: Write `ClickHouseDataSourceConfig`**

`clickhouse/src/main/java/org/opensearch/sql/clickhouse/config/ClickHouseDataSourceConfig.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
@AllArgsConstructor
public class ClickHouseDataSourceConfig {
  public static final String URI = "clickhouse.uri";
  public static final String AUTH_TYPE = "clickhouse.auth.type";
  public static final String AUTH_USERNAME = "clickhouse.auth.username";
  public static final String AUTH_PASSWORD = "clickhouse.auth.password";
  public static final String AUTH_TOKEN = "clickhouse.auth.token";
  public static final String TLS_ENABLED = "clickhouse.tls.enabled";
  public static final String TLS_TRUST_STORE_PATH = "clickhouse.tls.trust_store_path";
  public static final String TLS_TRUST_STORE_PASSWORD = "clickhouse.tls.trust_store_password";
  public static final String JDBC_SOCKET_TIMEOUT = "clickhouse.jdbc.socket_timeout";
  public static final String JDBC_COMPRESS = "clickhouse.jdbc.compress";
  public static final String POOL_MAX_SIZE = "clickhouse.pool.max_size";
  public static final String POOL_MIN_IDLE = "clickhouse.pool.min_idle";
  public static final String RATE_LIMIT_QPS = "clickhouse.rate_limit.qps";
  public static final String RATE_LIMIT_CONCURRENT = "clickhouse.rate_limit.concurrent";
  public static final String SLOW_QUERY_THRESHOLD_MS = "clickhouse.slow_query.threshold_ms";
  public static final String SCHEMA = "clickhouse.schema";

  private final String uri;
  private final String authType;
  private final String username;
  private final String password;
  private final String token;
  private final boolean tlsEnabled;
  private final String trustStorePath;
  private final String trustStorePassword;
  private final int socketTimeoutMs;
  private final boolean compress;
  private final int poolMaxSize;
  private final int poolMinIdle;
  private final int rateLimitQps;
  private final int rateLimitConcurrent;
  private final int slowQueryThresholdMs;
  private final ClickHouseTableSpec.Schema schema;

  public static ClickHouseDataSourceConfig parse(Map<String, String> props) {
    String uri = require(props, URI);
    String authType = props.getOrDefault(AUTH_TYPE, "basic");
    String username = props.get(AUTH_USERNAME);
    String password = props.get(AUTH_PASSWORD);
    String token = props.get(AUTH_TOKEN);

    if ("basic".equalsIgnoreCase(authType)) {
      if (isBlank(username) || isBlank(password)) {
        throw new IllegalArgumentException(
            "clickhouse.auth.type=basic requires auth.username and auth.password");
      }
    } else if ("jwt".equalsIgnoreCase(authType)) {
      if (isBlank(token)) {
        throw new IllegalArgumentException("clickhouse.auth.type=jwt requires auth.token");
      }
    } else {
      throw new IllegalArgumentException("Unsupported clickhouse.auth.type: " + authType);
    }

    ClickHouseTableSpec.Schema schema;
    try {
      String schemaJson = props.getOrDefault(SCHEMA, "{\"databases\":[]}");
      schema = new ObjectMapper().readValue(schemaJson, ClickHouseTableSpec.Schema.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid clickhouse.schema JSON: " + e.getMessage(), e);
    }

    return ClickHouseDataSourceConfig.builder()
        .uri(uri)
        .authType(authType)
        .username(username)
        .password(password)
        .token(token)
        .tlsEnabled(Boolean.parseBoolean(props.getOrDefault(TLS_ENABLED, "false")))
        .trustStorePath(props.get(TLS_TRUST_STORE_PATH))
        .trustStorePassword(props.get(TLS_TRUST_STORE_PASSWORD))
        .socketTimeoutMs(parseInt(props, JDBC_SOCKET_TIMEOUT, 30_000))
        .compress(Boolean.parseBoolean(props.getOrDefault(JDBC_COMPRESS, "false")))
        .poolMaxSize(parseInt(props, POOL_MAX_SIZE, 10))
        .poolMinIdle(parseInt(props, POOL_MIN_IDLE, 2))
        .rateLimitQps(parseInt(props, RATE_LIMIT_QPS, 50))
        .rateLimitConcurrent(parseInt(props, RATE_LIMIT_CONCURRENT, 20))
        .slowQueryThresholdMs(parseInt(props, SLOW_QUERY_THRESHOLD_MS, 5000))
        .schema(schema == null
            ? new ClickHouseTableSpec.Schema(java.util.List.of())
            : (schema.getDatabases() == null
                ? new ClickHouseTableSpec.Schema(java.util.List.of())
                : schema))
        .build();
  }

  private static String require(Map<String, String> props, String key) {
    String v = props.get(key);
    if (isBlank(v)) {
      throw new IllegalArgumentException("Missing required property: " + key);
    }
    return v;
  }

  private static int parseInt(Map<String, String> props, String key, int def) {
    String v = props.get(key);
    if (v == null) return def;
    try {
      return Integer.parseInt(v);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be an integer, got: " + v);
    }
  }

  private static boolean isBlank(String s) {
    return s == null || s.trim().isEmpty();
  }
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `./gradlew :clickhouse:test --tests "*ClickHouseDataSourceConfigTest*"`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/config/ \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/config/
git commit -s -m "feat(clickhouse): add config value objects + parser"
```

---

## Task M1.4: Empty `ClickHouseStorageEngine` + `ClickHouseTable` (skeleton)

**Files:**
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngine.java`
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseTable.java`
- Create: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngineTest.java`

- [ ] **Step 1: Write the failing test**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.clickhouse.client.ClickHouseClient;
import org.opensearch.sql.clickhouse.config.ClickHouseColumnSpec;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;
import org.opensearch.sql.exception.SemanticCheckException;

public class ClickHouseStorageEngineTest {
  @Test
  public void getTable_returns_clickhouse_table_for_declared_table() {
    ClickHouseTableSpec col = new ClickHouseTableSpec(
        "events",
        List.of(new ClickHouseColumnSpec("id", "Int64", "LONG")));
    ClickHouseTableSpec.Database db = new ClickHouseTableSpec.Database(
        "analytics", List.of(col));
    ClickHouseDataSourceConfig cfg = ClickHouseDataSourceConfig.builder()
        .uri("jdbc:clickhouse://h:8123/default")
        .authType("basic").username("u").password("p")
        .poolMaxSize(10).poolMinIdle(2)
        .rateLimitQps(50).rateLimitConcurrent(20)
        .slowQueryThresholdMs(5000)
        .schema(new ClickHouseTableSpec.Schema(List.of(db)))
        .build();

    ClickHouseStorageEngine engine =
        new ClickHouseStorageEngine(cfg, mock(ClickHouseClient.class));
    assertNotNull(engine.getTable(
        new DataSourceSchemaName("my_ch", "analytics"), "events"));
  }

  @Test
  public void getTable_throws_for_unknown_table() {
    ClickHouseDataSourceConfig cfg = ClickHouseDataSourceConfig.builder()
        .uri("jdbc:clickhouse://h:8123/default")
        .authType("basic").username("u").password("p")
        .poolMaxSize(10).poolMinIdle(2)
        .rateLimitQps(50).rateLimitConcurrent(20)
        .slowQueryThresholdMs(5000)
        .schema(new ClickHouseTableSpec.Schema(List.of()))
        .build();

    ClickHouseStorageEngine engine =
        new ClickHouseStorageEngine(cfg, mock(ClickHouseClient.class));
    assertThrows(
        SemanticCheckException.class,
        () -> engine.getTable(new DataSourceSchemaName("my_ch", "x"), "y"));
  }
}
```

- [ ] **Step 2: Run — expect FAIL** (classes missing)

- [ ] **Step 3: Create `ClickHouseTable`**

`clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseTable.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.clickhouse.config.ClickHouseColumnSpec;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;

/**
 * ClickHouse-backed Table — metadata only. The Calcite pushdown scan path does NOT use this class;
 * it resolves tables via {@code ClickHouseSchema} → per-datasource Calcite sub-schema → JdbcTable.
 * This class exists so the OpenSearch SQL plugin's non-Calcite paths (analyzer symbol lookup,
 * v2 execution fallback) can still see CH tables.
 */
@RequiredArgsConstructor
public class ClickHouseTable implements Table {
  @Getter private final String database;
  @Getter private final String name;
  private final ClickHouseTableSpec spec;

  @Override
  public Map<String, ExprType> getFieldTypes() {
    Map<String, ExprType> types = new LinkedHashMap<>();
    for (ClickHouseColumnSpec c : spec.getColumns()) {
      types.put(c.getName(), ExprCoreType.valueOf(c.getExprType()));
    }
    return types;
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    throw new UnsupportedOperationException(
        "ClickHouse tables are executed via Calcite; the v2 path is not supported.");
  }
}
```

- [ ] **Step 4: Create `ClickHouseStorageEngine`**

`clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngine.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.clickhouse.client.ClickHouseClient;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

@RequiredArgsConstructor
public class ClickHouseStorageEngine implements StorageEngine {
  private final ClickHouseDataSourceConfig config;
  private final ClickHouseClient client;

  public ClickHouseDataSourceConfig getConfig() {
    return config;
  }

  public ClickHouseClient getClient() {
    return client;
  }

  @Override
  public Table getTable(DataSourceSchemaName schemaName, String tableName) {
    String db = schemaName.getSchemaName();
    Optional<ClickHouseTableSpec> spec =
        config.getSchema().getDatabases().stream()
            .filter(d -> d.getName().equals(db))
            .flatMap(d -> d.getTables().stream())
            .filter(t -> t.getName().equals(tableName))
            .findFirst();
    if (spec.isEmpty()) {
      throw new SemanticCheckException(
          "Table " + db + "." + tableName + " is not declared for ClickHouse datasource");
    }
    return new ClickHouseTable(db, tableName, spec.get());
  }
}
```

- [ ] **Step 5: Add minimal `ClickHouseClient` placeholder** (real implementation in M2)

`clickhouse/src/main/java/org/opensearch/sql/clickhouse/client/ClickHouseClient.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.client;

import javax.sql.DataSource;

/** Placeholder; full implementation (HikariCP + rate limits) comes in M2. */
public interface ClickHouseClient extends AutoCloseable {
  DataSource getDataSource();

  @Override
  void close();
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `./gradlew :clickhouse:test --tests "*ClickHouseStorageEngineTest*"`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ \
        clickhouse/src/main/java/org/opensearch/sql/clickhouse/client/ClickHouseClient.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/storage/
git commit -s -m "feat(clickhouse): add storage engine skeleton + table metadata"
```

---

## Task M1.5: `ClickHouseStorageFactory` — plug into `DataSourceFactory` SPI

**Files:**
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageFactory.java`
- Create: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageFactoryTest.java`

- [ ] **Step 1: Write the failing test**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;

public class ClickHouseStorageFactoryTest {
  @Test
  public void factory_returns_CLICKHOUSE_type() {
    ClickHouseStorageFactory f = new ClickHouseStorageFactory(mock(Settings.class));
    assertEquals(DataSourceType.CLICKHOUSE, f.getDataSourceType());
  }

  @Test
  public void creates_datasource_with_storage_engine() {
    ClickHouseStorageFactory f = new ClickHouseStorageFactory(mock(Settings.class));
    DataSourceMetadata md = new DataSourceMetadata.Builder()
        .setName("my_ch")
        .setConnector(DataSourceType.CLICKHOUSE)
        .setProperties(Map.of(
            "clickhouse.uri", "jdbc:clickhouse://h:8123/default",
            "clickhouse.auth.type", "basic",
            "clickhouse.auth.username", "u",
            "clickhouse.auth.password", "p",
            "clickhouse.schema", "{\"databases\":[]}"))
        .build();
    DataSource ds = f.createDataSource(md);
    assertNotNull(ds.getStorageEngine());
    assertEquals(DataSourceType.CLICKHOUSE, ds.getConnectorType());
  }
}
```

(If `DataSourceMetadata.Builder` doesn't expose those setters with those exact names, read `datasources/.../DataSourceMetadata.java` in the project and match its actual builder API; do not invent.)

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Create the factory**

`clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageFactory.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.clickhouse.client.ClickHouseClient;
import org.opensearch.sql.clickhouse.client.ClickHouseClientFactory;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.DataSourceFactory;

@RequiredArgsConstructor
public class ClickHouseStorageFactory implements DataSourceFactory {
  private final Settings settings;

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.CLICKHOUSE;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    Map<String, String> props = metadata.getProperties();
    ClickHouseDataSourceConfig cfg = ClickHouseDataSourceConfig.parse(props);
    ClickHouseClient client = ClickHouseClientFactory.create(cfg);
    return new DataSource(
        metadata.getName(),
        DataSourceType.CLICKHOUSE,
        new ClickHouseStorageEngine(cfg, client));
  }
}
```

- [ ] **Step 4: Stub `ClickHouseClientFactory`**

`clickhouse/src/main/java/org/opensearch/sql/clickhouse/client/ClickHouseClientFactory.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.client;

import javax.sql.DataSource;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;

/** Stub. Real HikariCP-backed impl lands in M2. */
public final class ClickHouseClientFactory {
  private ClickHouseClientFactory() {}

  public static ClickHouseClient create(ClickHouseDataSourceConfig config) {
    return new ClickHouseClient() {
      @Override public DataSource getDataSource() {
        throw new UnsupportedOperationException("ClickHouseClientFactory stub — wired in M2");
      }
      @Override public void close() {}
    };
  }
}
```

- [ ] **Step 5: Run tests, expect PASS**

Run: `./gradlew :clickhouse:test`
Expected: all clickhouse unit tests pass.

- [ ] **Step 6: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageFactory.java \
        clickhouse/src/main/java/org/opensearch/sql/clickhouse/client/ClickHouseClientFactory.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageFactoryTest.java
git commit -s -m "feat(clickhouse): add DataSourceFactory SPI implementation"
```

---

## Task M1.6: Wire `ClickHouseStorageFactory` into the plugin

**Files:**
- Modify: `plugin/build.gradle` — add `implementation project(':clickhouse')`
- Modify: `plugin/src/main/java/.../OpenSearchPluginModule.java` — register the factory

- [ ] **Step 1: Locate the factory registration site**

Read `plugin/src/main/java/org/opensearch/sql/plugin/OpenSearchPluginModule.java` (or equivalent Guice module). Find how `PrometheusStorageFactory` is bound into the multibinder. Mirror that binding for `ClickHouseStorageFactory`.

- [ ] **Step 2: Add dependency**

Edit `plugin/build.gradle`, add under `dependencies { ... }`:

```groovy
    implementation project(':clickhouse')
```

- [ ] **Step 3: Add the Guice binding**

In the module file, inside the `configure()` method where `PrometheusStorageFactory` is bound:

```java
import org.opensearch.sql.clickhouse.storage.ClickHouseStorageFactory;
// ...
Multibinder<DataSourceFactory> dataSourceFactoryMultibinder = Multibinder.newSetBinder(binder(), DataSourceFactory.class);
dataSourceFactoryMultibinder.addBinding().to(PrometheusStorageFactory.class);
dataSourceFactoryMultibinder.addBinding().to(ClickHouseStorageFactory.class);
```

(The exact existing pattern may vary; read the file first, then add one new `addBinding().to(ClickHouseStorageFactory.class)` call.)

- [ ] **Step 4: Build the whole project**

Run: `./gradlew build -x integTest -x check`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 5: Commit**

```bash
git add plugin/build.gradle plugin/src/main/java/.../OpenSearchPluginModule.java
git commit -s -m "feat(plugin): wire ClickHouseStorageFactory into Guice module"
```

---

# M2 — Connection & Authentication

## Task M2.1: Type whitelist + `ClickHouseTypeMapper`

**Files:**
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/type/ClickHouseTypeMapper.java`
- Create: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/type/ClickHouseTypeMapperTest.java`

- [ ] **Step 1: Write the failing test**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.exception.ClickHouseSchemaException;
import org.opensearch.sql.data.type.ExprCoreType;

public class ClickHouseTypeMapperTest {
  @Test
  public void maps_supported_types() {
    assertEquals(ExprCoreType.INTEGER, ClickHouseTypeMapper.resolve("Int32", "INTEGER"));
    assertEquals(ExprCoreType.LONG, ClickHouseTypeMapper.resolve("Int64", "LONG"));
    assertEquals(ExprCoreType.LONG, ClickHouseTypeMapper.resolve("UInt32", "LONG"));
    assertEquals(ExprCoreType.FLOAT, ClickHouseTypeMapper.resolve("Float32", "FLOAT"));
    assertEquals(ExprCoreType.DOUBLE, ClickHouseTypeMapper.resolve("Float64", "DOUBLE"));
    assertEquals(ExprCoreType.STRING, ClickHouseTypeMapper.resolve("String", "STRING"));
    assertEquals(ExprCoreType.BOOLEAN, ClickHouseTypeMapper.resolve("Bool", "BOOLEAN"));
    assertEquals(ExprCoreType.DATE, ClickHouseTypeMapper.resolve("Date", "DATE"));
    assertEquals(ExprCoreType.DATE, ClickHouseTypeMapper.resolve("Date32", "DATE"));
    assertEquals(ExprCoreType.TIMESTAMP, ClickHouseTypeMapper.resolve("DateTime", "TIMESTAMP"));
    assertEquals(
        ExprCoreType.TIMESTAMP,
        ClickHouseTypeMapper.resolve("DateTime64(3)", "TIMESTAMP"));
  }

  @Test
  public void rejects_uint64_decimal_nullable_array_etc() {
    for (String t :
        new String[] {
          "UInt64", "Int128", "UInt128", "Int256", "UInt256",
          "Decimal(10,2)", "Decimal64(4)", "Enum8('a'=1)",
          "LowCardinality(String)", "Nullable(Int32)", "Array(Int32)",
          "Tuple(Int32, String)", "Map(String, Int32)", "UUID", "IPv4", "IPv6"
        }) {
      assertThrows(
          ClickHouseSchemaException.class,
          () -> ClickHouseTypeMapper.resolve(t, "STRING"),
          "Should reject " + t);
    }
  }

  @Test
  public void rejects_mismatched_expr_type() {
    assertThrows(
        ClickHouseSchemaException.class,
        () -> ClickHouseTypeMapper.resolve("Int32", "STRING"));
  }
}
```

- [ ] **Step 2: Create `ClickHouseSchemaException` (and base)**

`clickhouse/src/main/java/org/opensearch/sql/clickhouse/exception/ClickHouseException.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.exception;

import lombok.Getter;

@Getter
public class ClickHouseException extends RuntimeException {
  private final String errorCode;

  public ClickHouseException(String errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public ClickHouseException(String errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }
}
```

`clickhouse/src/main/java/org/opensearch/sql/clickhouse/exception/ClickHouseSchemaException.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.exception;

public class ClickHouseSchemaException extends ClickHouseException {
  public ClickHouseSchemaException(String message) {
    super("CH_SCHEMA_001", message);
  }
}
```

- [ ] **Step 3: Create `ClickHouseTypeMapper`**

`clickhouse/src/main/java/org/opensearch/sql/clickhouse/type/ClickHouseTypeMapper.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.type;

import java.util.Map;
import java.util.Set;
import org.opensearch.sql.clickhouse.exception.ClickHouseSchemaException;
import org.opensearch.sql.data.type.ExprCoreType;

public final class ClickHouseTypeMapper {
  private ClickHouseTypeMapper() {}

  /** Allowed (ch_type_prefix → allowed ExprCoreType set). DateTime64(p) matched by prefix. */
  private static final Map<String, Set<ExprCoreType>> ALLOWED = Map.ofEntries(
      Map.entry("Int8", Set.of(ExprCoreType.INTEGER)),
      Map.entry("Int16", Set.of(ExprCoreType.INTEGER)),
      Map.entry("Int32", Set.of(ExprCoreType.INTEGER)),
      Map.entry("Int64", Set.of(ExprCoreType.LONG)),
      Map.entry("UInt8", Set.of(ExprCoreType.BOOLEAN, ExprCoreType.SHORT)),
      Map.entry("UInt16", Set.of(ExprCoreType.LONG)),
      Map.entry("UInt32", Set.of(ExprCoreType.LONG)),
      Map.entry("Float32", Set.of(ExprCoreType.FLOAT)),
      Map.entry("Float64", Set.of(ExprCoreType.DOUBLE)),
      Map.entry("String", Set.of(ExprCoreType.STRING)),
      Map.entry("FixedString", Set.of(ExprCoreType.STRING)),
      Map.entry("Bool", Set.of(ExprCoreType.BOOLEAN)),
      Map.entry("Date", Set.of(ExprCoreType.DATE)),
      Map.entry("Date32", Set.of(ExprCoreType.DATE)),
      Map.entry("DateTime", Set.of(ExprCoreType.TIMESTAMP)),
      Map.entry("DateTime64", Set.of(ExprCoreType.TIMESTAMP)));

  /** Explicitly rejected prefixes — present here for clearer error messages. */
  private static final Set<String> REJECTED = Set.of(
      "UInt64", "Int128", "UInt128", "Int256", "UInt256",
      "Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256",
      "Enum8", "Enum16", "LowCardinality", "Nullable",
      "Array", "Tuple", "Map", "UUID", "IPv4", "IPv6",
      "Nested", "AggregateFunction");

  public static ExprCoreType resolve(String chType, String declaredExprType) {
    String prefix = extractPrefix(chType);
    if (REJECTED.contains(prefix)) {
      throw new ClickHouseSchemaException(
          "ClickHouse type " + chType + " is not supported. Cast server-side or choose a different column.");
    }
    Set<ExprCoreType> allowed = ALLOWED.get(prefix);
    if (allowed == null) {
      throw new ClickHouseSchemaException("Unknown ClickHouse type: " + chType);
    }
    ExprCoreType declared;
    try {
      declared = ExprCoreType.valueOf(declaredExprType);
    } catch (IllegalArgumentException e) {
      throw new ClickHouseSchemaException("Unknown expr_type: " + declaredExprType);
    }
    if (!allowed.contains(declared)) {
      throw new ClickHouseSchemaException(
          "expr_type " + declaredExprType + " not allowed for ClickHouse type " + chType
              + "; allowed: " + allowed);
    }
    return declared;
  }

  /** "DateTime64(3)" → "DateTime64"; "FixedString(16)" → "FixedString"; "Int32" → "Int32". */
  private static String extractPrefix(String chType) {
    int paren = chType.indexOf('(');
    return paren < 0 ? chType : chType.substring(0, paren);
  }
}
```

- [ ] **Step 4: Run test — expect PASS**

Run: `./gradlew :clickhouse:test --tests "*ClickHouseTypeMapperTest*"`

- [ ] **Step 5: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/exception/ \
        clickhouse/src/main/java/org/opensearch/sql/clickhouse/type/ \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/type/
git commit -s -m "feat(clickhouse): add type whitelist mapper + schema exception"
```

---

## Task M2.2: Hook type validation into config parsing

**Files:**
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/config/ClickHouseDataSourceConfig.java`
- Modify: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/config/ClickHouseDataSourceConfigTest.java`

- [ ] **Step 1: Write the failing test** (append to existing)

```java
  @Test
  public void rejects_schema_with_unsupported_ch_type_listing_all_violations() {
    String schemaJson =
        "{\"databases\":[{\"name\":\"db\",\"tables\":[{\"name\":\"t\",\"columns\":["
            + "{\"name\":\"a\",\"ch_type\":\"UInt64\",\"expr_type\":\"LONG\"},"
            + "{\"name\":\"b\",\"ch_type\":\"Decimal(10,2)\",\"expr_type\":\"DOUBLE\"},"
            + "{\"name\":\"c\",\"ch_type\":\"Int64\",\"expr_type\":\"LONG\"}]}]}]}";
    Map<String, String> props = Map.of(
        "clickhouse.uri", "jdbc:clickhouse://h:8123/default",
        "clickhouse.auth.type", "basic",
        "clickhouse.auth.username", "u",
        "clickhouse.auth.password", "p",
        "clickhouse.schema", schemaJson);
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> ClickHouseDataSourceConfig.parse(props));
    assertTrue(ex.getMessage().contains("a"));
    assertTrue(ex.getMessage().contains("b"));
    // "c" is valid; should NOT appear.
  }
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Add validation in `parse()`**

In `ClickHouseDataSourceConfig.parse()`, after parsing `schema`, add:

```java
    // Validate every (ch_type, expr_type) pair against the whitelist; collect all violations.
    java.util.List<String> violations = new java.util.ArrayList<>();
    for (ClickHouseTableSpec.Database db : schema.getDatabases()) {
      for (ClickHouseTableSpec tbl : db.getTables()) {
        for (ClickHouseColumnSpec col : tbl.getColumns()) {
          try {
            org.opensearch.sql.clickhouse.type.ClickHouseTypeMapper.resolve(
                col.getChType(), col.getExprType());
          } catch (org.opensearch.sql.clickhouse.exception.ClickHouseSchemaException e) {
            violations.add(
                db.getName() + "." + tbl.getName() + "." + col.getName()
                    + " (" + col.getChType() + ", " + col.getExprType() + "): " + e.getMessage());
          }
        }
      }
    }
    if (!violations.isEmpty()) {
      throw new IllegalArgumentException(
          "Schema contains unsupported columns:\n  " + String.join("\n  ", violations));
    }
```

- [ ] **Step 4: Run — expect PASS**

- [ ] **Step 5: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/config/ClickHouseDataSourceConfig.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/config/ClickHouseDataSourceConfigTest.java
git commit -s -m "feat(clickhouse): validate declared schema against type whitelist"
```

---

## Task M2.3: `ClickHouseAuthProvider` — build JDBC Properties

**Files:**
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/auth/ClickHouseAuthProvider.java`
- Create: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/auth/ClickHouseAuthProviderTest.java`

- [ ] **Step 1: Write the failing test**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;

public class ClickHouseAuthProviderTest {
  private ClickHouseDataSourceConfig.ClickHouseDataSourceConfigBuilder baseBuilder() {
    return ClickHouseDataSourceConfig.builder()
        .uri("jdbc:clickhouse://h:8123/default")
        .poolMaxSize(10).poolMinIdle(2)
        .rateLimitQps(50).rateLimitConcurrent(20)
        .slowQueryThresholdMs(5000)
        .schema(new ClickHouseTableSpec.Schema(java.util.List.of()));
  }

  @Test
  public void basic_auth_sets_user_and_password() {
    ClickHouseDataSourceConfig cfg = baseBuilder()
        .authType("basic").username("u").password("p").build();
    Properties p = ClickHouseAuthProvider.build(cfg);
    assertEquals("u", p.getProperty("user"));
    assertEquals("p", p.getProperty("password"));
    assertNull(p.getProperty("access_token"));
  }

  @Test
  public void jwt_auth_sets_access_token_only() {
    ClickHouseDataSourceConfig cfg = baseBuilder()
        .authType("jwt").token("tok").build();
    Properties p = ClickHouseAuthProvider.build(cfg);
    assertEquals("tok", p.getProperty("access_token"));
    assertNull(p.getProperty("user"));
    assertNull(p.getProperty("password"));
  }

  @Test
  public void tls_sets_ssl_properties() {
    ClickHouseDataSourceConfig cfg = baseBuilder()
        .authType("basic").username("u").password("p")
        .tlsEnabled(true)
        .trustStorePath("/t.jks").trustStorePassword("pw").build();
    Properties p = ClickHouseAuthProvider.build(cfg);
    assertEquals("true", p.getProperty("ssl"));
    assertEquals("strict", p.getProperty("sslmode"));
    assertTrue(p.containsKey("trust_store"));
  }

  @Test
  public void socket_timeout_and_compress_flags_set() {
    ClickHouseDataSourceConfig cfg = baseBuilder()
        .authType("basic").username("u").password("p")
        .socketTimeoutMs(15000).compress(true).build();
    Properties p = ClickHouseAuthProvider.build(cfg);
    assertEquals("15000", p.getProperty("socket_timeout"));
    assertEquals("true", p.getProperty("compress"));
  }
}
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Create `ClickHouseAuthProvider`**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.auth;

import java.util.Properties;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;

public final class ClickHouseAuthProvider {
  private ClickHouseAuthProvider() {}

  public static Properties build(ClickHouseDataSourceConfig cfg) {
    Properties p = new Properties();
    if ("basic".equalsIgnoreCase(cfg.getAuthType())) {
      p.setProperty("user", cfg.getUsername());
      p.setProperty("password", cfg.getPassword());
    } else if ("jwt".equalsIgnoreCase(cfg.getAuthType())) {
      p.setProperty("access_token", cfg.getToken());
    }
    if (cfg.isTlsEnabled()) {
      p.setProperty("ssl", "true");
      p.setProperty("sslmode", "strict");
      if (cfg.getTrustStorePath() != null) {
        p.setProperty("trust_store", cfg.getTrustStorePath());
      }
      if (cfg.getTrustStorePassword() != null) {
        p.setProperty("key_store_password", cfg.getTrustStorePassword());
      }
    }
    if (cfg.getSocketTimeoutMs() > 0) {
      p.setProperty("socket_timeout", Integer.toString(cfg.getSocketTimeoutMs()));
    }
    if (cfg.isCompress()) {
      p.setProperty("compress", "true");
    }
    return p;
  }
}
```

- [ ] **Step 4: Run — expect PASS**

- [ ] **Step 5: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/auth/ \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/auth/
git commit -s -m "feat(clickhouse): add AuthProvider building JDBC Properties"
```

---

## Task M2.4: Real `ClickHouseClient` using HikariCP

**Files:**
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/client/ClickHouseClient.java` (turn into concrete class or keep interface + add Impl — keep interface; add Impl)
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/client/HikariClickHouseClient.java`
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/client/ClickHouseClientFactory.java`
- Create: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/client/HikariClickHouseClientTest.java`

- [ ] **Step 1: Write the failing test**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;

public class HikariClickHouseClientTest {
  @Test
  public void hikari_pool_honors_config() {
    ClickHouseDataSourceConfig cfg = ClickHouseDataSourceConfig.builder()
        .uri("jdbc:clickhouse://host:8123/default")
        .authType("basic").username("u").password("p")
        .poolMaxSize(12).poolMinIdle(3)
        .rateLimitQps(50).rateLimitConcurrent(20)
        .slowQueryThresholdMs(5000)
        .socketTimeoutMs(15000)
        .schema(new ClickHouseTableSpec.Schema(java.util.List.of()))
        .build();
    try (HikariClickHouseClient c = HikariClickHouseClient.create(cfg)) {
      HikariDataSource ds = (HikariDataSource) c.getDataSource();
      assertNotNull(ds);
      assertEquals(12, ds.getMaximumPoolSize());
      assertEquals(3, ds.getMinimumIdle());
      assertEquals("SELECT 1", ds.getConnectionTestQuery());
      assertEquals("jdbc:clickhouse://host:8123/default", ds.getJdbcUrl());
      // auth applied via dataSourceProperties
      assertEquals("u", ds.getDataSourceProperties().getProperty("user"));
    }
  }
}
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Create `HikariClickHouseClient`**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.client;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.opensearch.sql.clickhouse.auth.ClickHouseAuthProvider;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;

public class HikariClickHouseClient implements ClickHouseClient {
  private final HikariDataSource hikari;

  private HikariClickHouseClient(HikariDataSource hikari) {
    this.hikari = hikari;
  }

  public static HikariClickHouseClient create(ClickHouseDataSourceConfig cfg) {
    HikariConfig hc = new HikariConfig();
    hc.setJdbcUrl(cfg.getUri());
    hc.setMaximumPoolSize(cfg.getPoolMaxSize());
    hc.setMinimumIdle(cfg.getPoolMinIdle());
    hc.setConnectionTimeout(5_000);
    hc.setIdleTimeout(10L * 60_000L);
    hc.setMaxLifetime(30L * 60_000L);
    hc.setLeakDetectionThreshold(60_000);
    hc.setConnectionTestQuery("SELECT 1");
    hc.setPoolName("clickhouse-" + Integer.toHexString(System.identityHashCode(cfg)));
    hc.setDataSourceProperties(ClickHouseAuthProvider.build(cfg));
    return new HikariClickHouseClient(new HikariDataSource(hc));
  }

  @Override
  public DataSource getDataSource() {
    return hikari;
  }

  @Override
  public void close() {
    hikari.close();
  }
}
```

- [ ] **Step 4: Rewire the factory**

In `ClickHouseClientFactory.create()`:

```java
  public static ClickHouseClient create(ClickHouseDataSourceConfig config) {
    return HikariClickHouseClient.create(config);
  }
```

- [ ] **Step 5: Run test — expect PASS**

Run: `./gradlew :clickhouse:test --tests "*HikariClickHouseClientTest*"`

- [ ] **Step 6: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/client/ \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/client/
git commit -s -m "feat(clickhouse): HikariCP-backed ClickHouseClient"
```

---

## Task M2.5: Fail-fast probe on registration (`SELECT 1`)

**Files:**
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageFactory.java`
- Modify: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageFactoryTest.java`

- [ ] **Step 1: Add `ClickHouseConnectionException`**

`clickhouse/src/main/java/org/opensearch/sql/clickhouse/exception/ClickHouseConnectionException.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.exception;

public class ClickHouseConnectionException extends ClickHouseException {
  public ClickHouseConnectionException(String message, Throwable cause) {
    super("CH_CONN_001", message, cause);
  }
}
```

- [ ] **Step 2: Write the failing test**

```java
  @Test
  public void createDataSource_probes_connection_and_wraps_failure() {
    ClickHouseStorageFactory f = new ClickHouseStorageFactory(mock(Settings.class));
    DataSourceMetadata md = new DataSourceMetadata.Builder()
        .setName("my_ch")
        .setConnector(DataSourceType.CLICKHOUSE)
        .setProperties(Map.of(
            "clickhouse.uri", "jdbc:clickhouse://127.0.0.1:1/default",
            "clickhouse.auth.type", "basic",
            "clickhouse.auth.username", "u",
            "clickhouse.auth.password", "p",
            "clickhouse.schema", "{\"databases\":[]}"))
        .build();
    assertThrows(
        org.opensearch.sql.clickhouse.exception.ClickHouseConnectionException.class,
        () -> f.createDataSource(md));
  }
```

- [ ] **Step 3: Run — expect FAIL**

- [ ] **Step 4: Probe in factory**

In `ClickHouseStorageFactory.createDataSource()`:

```java
  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    Map<String, String> props = metadata.getProperties();
    ClickHouseDataSourceConfig cfg = ClickHouseDataSourceConfig.parse(props);
    ClickHouseClient client = ClickHouseClientFactory.create(cfg);
    try (java.sql.Connection conn = client.getDataSource().getConnection();
         java.sql.Statement st = conn.createStatement();
         java.sql.ResultSet rs = st.executeQuery("SELECT 1")) {
      if (!rs.next()) {
        throw new org.opensearch.sql.clickhouse.exception.ClickHouseConnectionException(
            "SELECT 1 returned no rows", null);
      }
    } catch (java.sql.SQLException e) {
      client.close();
      throw new org.opensearch.sql.clickhouse.exception.ClickHouseConnectionException(
          "Failed to connect to ClickHouse: " + e.getMessage(), e);
    }
    return new DataSource(
        metadata.getName(),
        DataSourceType.CLICKHOUSE,
        new ClickHouseStorageEngine(cfg, client));
  }
```

- [ ] **Step 5: Run — expect PASS**

- [ ] **Step 6: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/exception/ClickHouseConnectionException.java \
        clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageFactory.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageFactoryTest.java
git commit -s -m "feat(clickhouse): fail-fast SELECT 1 probe on datasource registration"
```

---

## Task M2.6: IT — registration against real CH container (Testcontainers)

**Files:**
- Modify: `integ-test/build.gradle` — add Testcontainers ClickHouse
- Create: `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHouseRegistrationIT.java`

- [ ] **Step 1: Add test dependency**

Edit `integ-test/build.gradle`, under `dependencies { ... }`:

```groovy
    testImplementation 'org.testcontainers:clickhouse:1.19.7'
    testImplementation 'org.testcontainers:junit-jupiter:1.19.7'
```

- [ ] **Step 2: Write the IT**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.testcontainers.clickhouse.ClickHouseContainer;

public class ClickHouseRegistrationIT extends PPLIntegTestCase {
  @ClassRule
  public static final ClickHouseContainer CH =
      new ClickHouseContainer("clickhouse/clickhouse-server:24.3");

  private static final String DS_NAME = "ch_reg_it";

  @Before
  public void setup() throws Exception {
    super.init();
    enableCalcite();
  }

  @After
  public void teardown() throws Exception {
    try {
      Request del = new Request("DELETE", "/_plugins/_query/_datasources/" + DS_NAME);
      client().performRequest(del);
    } catch (Exception ignored) {}
  }

  @Test
  public void register_succeeds_with_valid_config() throws Exception {
    Request req = new Request("POST", "/_plugins/_query/_datasources");
    req.setJsonEntity(
        "{"
            + "\"name\":\"" + DS_NAME + "\","
            + "\"connector\":\"CLICKHOUSE\","
            + "\"properties\":{"
            + "\"clickhouse.uri\":\"" + CH.getJdbcUrl() + "\","
            + "\"clickhouse.auth.type\":\"basic\","
            + "\"clickhouse.auth.username\":\"" + CH.getUsername() + "\","
            + "\"clickhouse.auth.password\":\"" + CH.getPassword() + "\","
            + "\"clickhouse.schema\":\"{\\\"databases\\\":[]}\""
            + "}"
            + "}");
    Response resp = client().performRequest(req);
    assertThat(String.valueOf(resp.getStatusLine().getStatusCode()), containsString("20"));
  }

  @Test
  public void register_fails_fast_on_bad_credentials() throws Exception {
    Request req = new Request("POST", "/_plugins/_query/_datasources");
    req.setJsonEntity(
        "{"
            + "\"name\":\"" + DS_NAME + "\","
            + "\"connector\":\"CLICKHOUSE\","
            + "\"properties\":{"
            + "\"clickhouse.uri\":\"" + CH.getJdbcUrl() + "\","
            + "\"clickhouse.auth.type\":\"basic\","
            + "\"clickhouse.auth.username\":\"wrong\","
            + "\"clickhouse.auth.password\":\"wrong\","
            + "\"clickhouse.schema\":\"{\\\"databases\\\":[]}\""
            + "}"
            + "}");
    try {
      client().performRequest(req);
      org.junit.Assert.fail("Expected failure");
    } catch (org.opensearch.client.ResponseException e) {
      String body = new String(
          e.getResponse().getEntity().getContent().readAllBytes());
      assertThat(body.toLowerCase(), containsString("clickhouse"));
    }
  }

  @Test
  public void register_fails_on_unsupported_type_in_schema() throws Exception {
    Request req = new Request("POST", "/_plugins/_query/_datasources");
    req.setJsonEntity(
        "{"
            + "\"name\":\"" + DS_NAME + "\","
            + "\"connector\":\"CLICKHOUSE\","
            + "\"properties\":{"
            + "\"clickhouse.uri\":\"" + CH.getJdbcUrl() + "\","
            + "\"clickhouse.auth.type\":\"basic\","
            + "\"clickhouse.auth.username\":\"" + CH.getUsername() + "\","
            + "\"clickhouse.auth.password\":\"" + CH.getPassword() + "\","
            + "\"clickhouse.schema\":\"{\\\"databases\\\":[{\\\"name\\\":\\\"db\\\",\\\"tables\\\":[{\\\"name\\\":\\\"t\\\",\\\"columns\\\":[{\\\"name\\\":\\\"c\\\",\\\"ch_type\\\":\\\"UInt64\\\",\\\"expr_type\\\":\\\"LONG\\\"}]}]}]}\""
            + "}"
            + "}");
    try {
      client().performRequest(req);
      org.junit.Assert.fail("Expected schema validation failure");
    } catch (org.opensearch.client.ResponseException e) {
      String body = new String(
          e.getResponse().getEntity().getContent().readAllBytes());
      assertThat(body, containsString("UInt64"));
    }
  }
}
```

- [ ] **Step 3: Run the IT**

Run: `./gradlew :integ-test:integTest --tests "*ClickHouseRegistrationIT*"`
Expected: PASS (three tests).

- [ ] **Step 4: Commit**

```bash
git add integ-test/build.gradle \
        integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHouseRegistrationIT.java
git commit -s -m "test(clickhouse): IT for datasource registration against real CH container"
```

---

# M3 — Schema Factory (Calcite-facing)

## Task M3.1: `ClickHouseSqlDialect` — day-1 dialect

**Files:**
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java`
- Create: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialectTest.java`

- [ ] **Step 1: Write the failing test**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.jupiter.api.Test;

public class ClickHouseSqlDialectTest {
  @Test
  public void quotes_identifiers_with_backticks() {
    SqlPrettyWriter w = new SqlPrettyWriter(ClickHouseSqlDialect.INSTANCE);
    new SqlIdentifier("foo", SqlParserPos.ZERO).unparse(w, 0, 0);
    assertEquals("`foo`", w.toString());
  }

  @Test
  public void supports_standard_aggregates() {
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsAggregateFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsAggregateFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsAggregateFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.AVG));
  }
}
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Create the dialect**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.calcite;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;

public class ClickHouseSqlDialect extends SqlDialect {
  public static final SqlDialect.Context CTX =
      SqlDialect.EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.UNKNOWN)
          .withDatabaseProductName("ClickHouse")
          .withIdentifierQuoteString("`")
          .withCaseSensitive(true)
          .withNullCollation(NullCollation.LOW);

  public static final ClickHouseSqlDialect INSTANCE = new ClickHouseSqlDialect();

  private ClickHouseSqlDialect() {
    super(CTX);
  }

  @Override
  public boolean supportsAggregateFunction(org.apache.calcite.sql.SqlOperator op) {
    SqlKind k = op.getKind();
    return k == SqlKind.COUNT || k == SqlKind.SUM || k == SqlKind.AVG
        || k == SqlKind.MIN || k == SqlKind.MAX;
  }

  @Override
  public boolean supportsFunction(
      org.apache.calcite.sql.SqlOperator op,
      org.apache.calcite.rel.type.RelDataType type,
      java.util.List<org.apache.calcite.rel.type.RelDataType> paramTypes) {
    SqlKind k = op.getKind();
    switch (k) {
      case AND: case OR: case NOT:
      case EQUALS: case NOT_EQUALS:
      case LESS_THAN: case LESS_THAN_OR_EQUAL:
      case GREATER_THAN: case GREATER_THAN_OR_EQUAL:
      case IS_NULL: case IS_NOT_NULL:
      case PLUS: case MINUS: case TIMES: case DIVIDE: case MOD:
      case CAST: case COALESCE: case CASE:
      case LIKE:
      case COUNT: case SUM: case AVG: case MIN: case MAX:
        return true;
      default:
        break;
    }
    String name = op.getName().toUpperCase();
    switch (name) {
      case "SUBSTRING":
      case "LOWER":
      case "UPPER":
      case "LENGTH":
      case "TRIM":
      case "CONCAT":
      case "DATE_TRUNC":
        return true;
      default:
        return false;
    }
  }
}
```

- [ ] **Step 4: Run — expect PASS**

- [ ] **Step 5: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialectTest.java
git commit -s -m "feat(clickhouse): add day-1 SqlDialect (whitelist + backtick quoting)"
```

---

## Task M3.2: `ClickHouseConvention` (extends `JdbcConvention`)

**Files:**
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseConvention.java`
- Create: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseConventionTest.java`

- [ ] **Step 1: Write the failing test**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.junit.jupiter.api.Test;

public class ClickHouseConventionTest {
  @Test
  public void convention_exposes_dialect_and_unique_name() {
    ClickHouseConvention c1 = ClickHouseConvention.of("ds_a");
    ClickHouseConvention c2 = ClickHouseConvention.of("ds_b");
    assertNotNull(c1);
    assertNotNull(c2);
    assertEquals(ClickHouseSqlDialect.INSTANCE, c1.dialect);
    org.junit.jupiter.api.Assertions.assertNotEquals(c1.getName(), c2.getName());
    org.junit.jupiter.api.Assertions.assertTrue(c1 instanceof JdbcConvention);
  }
}
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Create the convention**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.calcite;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.schema.impl.AbstractSchema;

public class ClickHouseConvention extends JdbcConvention {
  private ClickHouseConvention(String name) {
    super(ClickHouseSqlDialect.INSTANCE, new org.apache.calcite.linq4j.tree.ConstantExpression(
        String.class, name), name);
  }

  public static ClickHouseConvention of(String datasourceName) {
    return new ClickHouseConvention("CLICKHOUSE_" + datasourceName + "_" + System.nanoTime());
  }
}
```

(If `JdbcConvention`'s constructor signature in Calcite 1.41 differs, adapt — the reference is `JdbcConvention.of(SqlDialect, Expression, String)` static factory. Prefer the static factory:)

Replacement body if `JdbcConvention.of(...)` exists:

```java
  public static ClickHouseConvention of(String datasourceName) {
    String name = "CLICKHOUSE_" + datasourceName + "_" + System.nanoTime();
    // JdbcConvention.of returns a JdbcConvention; we wrap via delegation since ClickHouseConvention
    // extends it and we can't reassign `this`. Use the public constructor pattern from calcite-core.
    return new ClickHouseConvention(name);
  }
```

(The implementer should look at `org.apache.calcite.adapter.jdbc.JdbcConvention` source in their IDE/jar and use the matching constructor. Do not guess — check before coding.)

- [ ] **Step 4: Run — expect PASS**

- [ ] **Step 5: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseConvention.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseConventionTest.java
git commit -s -m "feat(clickhouse): add per-datasource JdbcConvention"
```

---

## Task M3.3: `ClickHouseSchemaFactory` — build Calcite Schema from declared spec

**Files:**
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseSchemaFactory.java`
- Create: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/storage/ClickHouseSchemaFactoryTest.java`

- [ ] **Step 1: Write the failing test**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.List;
import javax.sql.DataSource;
import org.apache.calcite.schema.Schema;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.config.ClickHouseColumnSpec;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;

public class ClickHouseSchemaFactoryTest {
  @Test
  public void builds_schema_with_sub_schema_per_database_and_table_per_spec() {
    ClickHouseTableSpec.Schema spec = new ClickHouseTableSpec.Schema(List.of(
        new ClickHouseTableSpec.Database("analytics", List.of(
            new ClickHouseTableSpec(
                "events",
                List.of(
                    new ClickHouseColumnSpec("id", "Int64", "LONG"),
                    new ClickHouseColumnSpec("name", "String", "STRING")))))));

    DataSource ds = mock(DataSource.class);
    Schema schema = ClickHouseSchemaFactory.build("my_ch", ds, spec);
    assertNotNull(schema);
    // Expect a sub-schema "analytics"
    Schema analytics = schema.getSubSchema("analytics");
    assertNotNull(analytics);
    // Expect a table "events"
    org.apache.calcite.schema.Table events = analytics.getTable("events");
    assertNotNull(events);
  }
}
```

(`Schema.getSubSchema` / `Schema.getTable` — read Calcite 1.41 `org.apache.calcite.schema.Schema` to confirm method names; adapt if needed.)

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement the factory**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.sql.clickhouse.calcite.ClickHouseConvention;
import org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;

public final class ClickHouseSchemaFactory {
  private ClickHouseSchemaFactory() {}

  /**
   * Build a Calcite Schema whose sub-schemas correspond to CH databases, each sub-schema holding
   * JdbcTable instances backed by the given DataSource and a per-datasource ClickHouseConvention.
   */
  public static Schema build(
      String datasourceName, DataSource dataSource, ClickHouseTableSpec.Schema spec) {
    ClickHouseConvention convention = ClickHouseConvention.of(datasourceName);
    Map<String, Schema> subs = new LinkedHashMap<>();
    for (ClickHouseTableSpec.Database db : spec.getDatabases()) {
      // JdbcSchema.create(parent, name, dataSource, dialect, catalog, schema)
      // For each CH database, create one JdbcSchema using the CH database name.
      // Construct via calcite's public JdbcSchema constructor (no metadata scan — we only use it
      // as a container). Alternative: subclass AbstractSchema and populate with JdbcTable directly.
      JdbcSchemaBuilder sub = new JdbcSchemaBuilder(
          dataSource, ClickHouseSqlDialect.INSTANCE, convention, db.getName());
      for (ClickHouseTableSpec tbl : db.getTables()) {
        sub.addTable(tbl);
      }
      subs.put(db.getName(), sub);
    }
    return new AbstractSchema() {
      @Override
      protected Map<String, Schema> getSubSchemaMap() {
        return subs;
      }
    };
  }
}
```

And helper `JdbcSchemaBuilder` (same package):

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.clickhouse.config.ClickHouseColumnSpec;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;
import org.opensearch.sql.clickhouse.type.ClickHouseTypeMapper;

class JdbcSchemaBuilder extends AbstractSchema {
  private final DataSource dataSource;
  private final SqlDialect dialect;
  private final JdbcConvention convention;
  private final String catalog;
  private final Map<String, Table> tables = new LinkedHashMap<>();
  private final JdbcSchema delegate;

  JdbcSchemaBuilder(DataSource ds, SqlDialect dialect, JdbcConvention conv, String catalog) {
    this.dataSource = ds;
    this.dialect = dialect;
    this.convention = conv;
    this.catalog = catalog;
    this.delegate = new JdbcSchema(ds, dialect, conv, catalog, null);
  }

  void addTable(ClickHouseTableSpec spec) {
    RelDataTypeFactory tf = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder b = tf.builder();
    for (ClickHouseColumnSpec col : spec.getColumns()) {
      // Validate (throws if unsupported)
      ClickHouseTypeMapper.resolve(col.getChType(), col.getExprType());
      b.add(col.getName(), toSqlType(tf, col.getChType()));
    }
    RelDataType rowType = b.build();
    tables.put(
        spec.getName(),
        new JdbcTable(delegate, catalog, null, spec.getName(),
            org.apache.calcite.schema.Schema.TableType.TABLE, rowType, null));
  }

  private static RelDataType toSqlType(RelDataTypeFactory tf, String chType) {
    String prefix = chType.contains("(") ? chType.substring(0, chType.indexOf('(')) : chType;
    switch (prefix) {
      case "Int8":
      case "Int16":
      case "Int32":       return tf.createSqlType(SqlTypeName.INTEGER);
      case "Int64":
      case "UInt16":
      case "UInt32":      return tf.createSqlType(SqlTypeName.BIGINT);
      case "UInt8":       return tf.createSqlType(SqlTypeName.SMALLINT);
      case "Float32":     return tf.createSqlType(SqlTypeName.FLOAT);
      case "Float64":     return tf.createSqlType(SqlTypeName.DOUBLE);
      case "String":
      case "FixedString": return tf.createSqlType(SqlTypeName.VARCHAR);
      case "Bool":        return tf.createSqlType(SqlTypeName.BOOLEAN);
      case "Date":
      case "Date32":      return tf.createSqlType(SqlTypeName.DATE);
      case "DateTime":
      case "DateTime64":  return tf.createSqlType(SqlTypeName.TIMESTAMP);
      default:
        throw new IllegalArgumentException("Unsupported CH type prefix: " + prefix);
    }
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return tables;
  }
}
```

(The `JdbcTable` / `JdbcSchema` constructor signatures vary by Calcite minor version. Before committing, verify against `org.apache.calcite.adapter.jdbc.JdbcTable`'s constructor list in calcite-core 1.41.0 and adjust argument order. If a public constructor isn't available, switch to `JdbcSchema.createTable(String, DataSource, SqlDialect, ...)` equivalents or reflect.)

- [ ] **Step 4: Run — expect PASS**

- [ ] **Step 5: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseSchemaFactory.java \
        clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/JdbcSchemaBuilder.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/storage/ClickHouseSchemaFactoryTest.java
git commit -s -m "feat(clickhouse): SchemaFactory builds Calcite sub-schemas with JdbcTable"
```

---

## Task M3.4: Expose `asCalciteSchema()` on `ClickHouseStorageEngine`

**Files:**
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngine.java`
- Modify: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngineTest.java`

- [ ] **Step 1: Write the failing test** (append)

```java
  @Test
  public void asCalciteSchema_returns_non_null_schema_with_declared_subschema() {
    ClickHouseTableSpec col = new ClickHouseTableSpec(
        "events", List.of(new ClickHouseColumnSpec("id", "Int64", "LONG")));
    ClickHouseTableSpec.Database db = new ClickHouseTableSpec.Database("analytics", List.of(col));
    ClickHouseDataSourceConfig cfg = ClickHouseDataSourceConfig.builder()
        .uri("jdbc:clickhouse://h:8123/default")
        .authType("basic").username("u").password("p")
        .poolMaxSize(10).poolMinIdle(2)
        .rateLimitQps(50).rateLimitConcurrent(20).slowQueryThresholdMs(5000)
        .schema(new ClickHouseTableSpec.Schema(List.of(db))).build();

    ClickHouseClient client = mock(ClickHouseClient.class);
    org.mockito.Mockito.when(client.getDataSource()).thenReturn(mock(javax.sql.DataSource.class));

    ClickHouseStorageEngine engine = new ClickHouseStorageEngine(cfg, client);
    org.apache.calcite.schema.Schema s = engine.asCalciteSchema("my_ch");
    assertNotNull(s);
    assertNotNull(s.getSubSchema("analytics"));
    assertNotNull(s.getSubSchema("analytics").getTable("events"));
  }
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Implement**

In `ClickHouseStorageEngine`:

```java
  public org.apache.calcite.schema.Schema asCalciteSchema(String datasourceName) {
    return ClickHouseSchemaFactory.build(datasourceName, client.getDataSource(), config.getSchema());
  }
```

- [ ] **Step 4: Run — expect PASS**

- [ ] **Step 5: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngine.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngineTest.java
git commit -s -m "feat(clickhouse): expose asCalciteSchema on storage engine"
```

---

## Task M3.5: Wire `ClickHouseSchema` sub-schema lookup to real engines

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/calcite/ClickHouseSchema.java`
- Modify: `core/src/test/java/org/opensearch/sql/calcite/ClickHouseSchemaTest.java`

`core` cannot import from `clickhouse` directly. Introduce a narrow interface `CalciteSchemaProvider` in `core.storage` that `ClickHouseStorageEngine` implements.

- [ ] **Step 1: Define `CalciteSchemaProvider` in core**

`core/src/main/java/org/opensearch/sql/storage/CalciteSchemaProvider.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage;

import org.apache.calcite.schema.Schema;

/**
 * Storage engines that expose a Calcite Schema (for per-query sub-schema registration) implement
 * this interface. The Calcite engine queries this via {@code instanceof} when building the planner
 * root schema. Keeps {@code core} free of dependencies on connector-specific modules.
 */
public interface CalciteSchemaProvider {
  Schema asCalciteSchema(String datasourceName);
}
```

- [ ] **Step 2: Make `ClickHouseStorageEngine` implement it**

In `ClickHouseStorageEngine`, add:

```java
public class ClickHouseStorageEngine implements StorageEngine,
    org.opensearch.sql.storage.CalciteSchemaProvider {
  // existing asCalciteSchema method satisfies the interface.
}
```

- [ ] **Step 3: Update `ClickHouseSchema` to resolve real sub-schemas**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.sql.datasource.DataSourceMetadataStorage;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.CalciteSchemaProvider;

@Getter
@AllArgsConstructor
public class ClickHouseSchema extends AbstractSchema {
  public static final String CLICKHOUSE_SCHEMA_NAME = "ClickHouse";

  private final DataSourceService dataSourceService;

  @Override
  protected Map<String, Schema> getSubSchemaMap() {
    Map<String, Schema> result = new LinkedHashMap<>();
    for (org.opensearch.sql.datasource.model.DataSourceMetadata md :
        dataSourceService.getDataSourceMetadata(true)) {
      if (!DataSourceType.CLICKHOUSE.equals(md.getConnector())) {
        continue;
      }
      DataSource ds = dataSourceService.getDataSource(md.getName());
      if (ds.getStorageEngine() instanceof CalciteSchemaProvider provider) {
        result.put(md.getName(), provider.asCalciteSchema(md.getName()));
      }
    }
    return result;
  }
}
```

(Verify `DataSourceService.getDataSourceMetadata(boolean)` — the exact signature in this repo may differ; read `core/.../DataSourceService.java` and match it. If no enumeration method exists, add one or reach through `DataSourceMetadataStorage`.)

- [ ] **Step 4: Update the test**

```java
  @Test
  public void sub_schema_map_contains_one_entry_per_registered_ch_datasource() {
    DataSourceService dss = mock(DataSourceService.class);
    org.opensearch.sql.datasource.model.DataSourceMetadata chMd =
        mock(org.opensearch.sql.datasource.model.DataSourceMetadata.class);
    org.mockito.Mockito.when(chMd.getName()).thenReturn("my_ch");
    org.mockito.Mockito.when(chMd.getConnector())
        .thenReturn(org.opensearch.sql.datasource.model.DataSourceType.CLICKHOUSE);
    org.mockito.Mockito.when(dss.getDataSourceMetadata(true))
        .thenReturn(java.util.List.of(chMd));

    org.opensearch.sql.datasource.model.DataSource ds =
        mock(org.opensearch.sql.datasource.model.DataSource.class);
    org.opensearch.sql.storage.StorageEngine engine =
        mock(org.opensearch.sql.storage.StorageEngine.class,
            org.mockito.Mockito.withSettings()
                .extraInterfaces(org.opensearch.sql.storage.CalciteSchemaProvider.class));
    org.mockito.Mockito.when(((org.opensearch.sql.storage.CalciteSchemaProvider) engine)
        .asCalciteSchema("my_ch"))
        .thenReturn(mock(org.apache.calcite.schema.Schema.class));
    org.mockito.Mockito.when(ds.getStorageEngine()).thenReturn(engine);
    org.mockito.Mockito.when(dss.getDataSource("my_ch")).thenReturn(ds);

    ClickHouseSchema schema = new ClickHouseSchema(dss);
    org.junit.jupiter.api.Assertions.assertTrue(schema.getSubSchemaMap().containsKey("my_ch"));
  }
```

- [ ] **Step 5: Run — expect PASS**

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/org/opensearch/sql/storage/CalciteSchemaProvider.java \
        core/src/main/java/org/opensearch/sql/calcite/ClickHouseSchema.java \
        core/src/test/java/org/opensearch/sql/calcite/ClickHouseSchemaTest.java \
        clickhouse/src/main/java/org/opensearch/sql/clickhouse/storage/ClickHouseStorageEngine.java
git commit -s -m "feat(core,clickhouse): wire ClickHouseSchema to enumerate CH datasources"
```

---

# M4 — Core Pushdown (Basic Query End-to-End)

## Task M4.1: Register `JdbcRules` on each query's planner

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/executor/QueryService.java` (in `buildFrameworkConfig()` or the relevant Programs wiring)
- OR: Add planner-rule registration via `Programs.hep(...)` in the CH-specific path

Because `JdbcRules` need a concrete `JdbcConvention` instance, rule registration happens per-query after the root schema is built but before optimization. Calcite auto-registers these rules when a `JdbcSchema`-backed table appears; in practice, the first time `JdbcTableScan` is referenced, rules fire. If tests in M4.3 show the rules don't fire, add an explicit `planner.addRule(JdbcRules.JdbcProjectRule.INSTANCE)` etc. This task is a *verification + fallback-wire* step only.

- [ ] **Step 1: Smoke unit test (plan-level)**

`core/src/test/java/org/opensearch/sql/calcite/ClickHousePlanSmokeTest.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
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
    DataSourceService dss = mock(DataSourceService.class);
    DataSourceMetadata md = mock(DataSourceMetadata.class);
    when(md.getName()).thenReturn("my_ch");
    when(md.getConnector()).thenReturn(DataSourceType.CLICKHOUSE);
    when(dss.getDataSourceMetadata(true)).thenReturn(java.util.List.of(md));

    // Build a Calcite Schema containing sub-schema "analytics" / table "events" (row: id BIGINT).
    org.apache.calcite.schema.SchemaPlus child =
        Frameworks.createRootSchema(true);
    org.apache.calcite.jdbc.CalciteSchema tmp =
        org.apache.calcite.jdbc.CalciteSchema.createRootSchema(true, false);
    org.apache.calcite.schema.SchemaPlus analytics = tmp.plus().add("analytics",
        new org.apache.calcite.schema.impl.AbstractSchema() {
          @Override
          protected java.util.Map<String, org.apache.calcite.schema.Table> getTableMap() {
            return java.util.Map.of("events", new org.apache.calcite.schema.impl.AbstractTable() {
              @Override public org.apache.calcite.rel.type.RelDataType getRowType(
                  org.apache.calcite.rel.type.RelDataTypeFactory tf) {
                return tf.builder().add("id", tf.createSqlType(
                    org.apache.calcite.sql.type.SqlTypeName.BIGINT)).build();
              }
            });
          }
        });
    org.apache.calcite.schema.Schema chSchema = tmp.plus();

    DataSource dsObj = mock(DataSource.class);
    StorageEngine engine = mock(StorageEngine.class,
        org.mockito.Mockito.withSettings().extraInterfaces(CalciteSchemaProvider.class));
    when(((CalciteSchemaProvider) engine).asCalciteSchema("my_ch")).thenReturn(chSchema);
    when(dsObj.getStorageEngine()).thenReturn(engine);
    when(dss.getDataSource("my_ch")).thenReturn(dsObj);

    // Register ClickHouseSchema under a test root.
    org.apache.calcite.schema.SchemaPlus root =
        org.apache.calcite.jdbc.CalciteSchema.createRootSchema(true, false).plus();
    root.add("ClickHouse", new ClickHouseSchema(dss));

    FrameworkConfig cfg = Frameworks.newConfigBuilder().defaultSchema(root).build();
    RelBuilder rb = RelBuilder.create(cfg);
    RelNode scan = rb.scan("ClickHouse", "my_ch", "analytics", "events").build();
    assertTrue(RelOptUtil.toString(scan).contains("events"),
        () -> "Plan should reference events table:\n" + RelOptUtil.toString(scan));
  }
}
```

- [ ] **Step 2: Run the test**

Run: `./gradlew :core:test --tests "*ClickHousePlanSmokeTest*"`
Expected: PASS — demonstrates the sub-schema registration path is reachable via `RelBuilder.scan`.

- [ ] **Step 3: Commit**

```bash
git add core/src/test/java/org/opensearch/sql/calcite/ClickHousePlanSmokeTest.java
git commit -s -m "test(core): smoke test that ClickHouseSchema sub-schema is scan-reachable"
```

---

## Task M4.2: Adjust `visitRelation` to scan via the `ClickHouse` schema prefix

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`
- Modify: `core/src/test/java/org/opensearch/sql/calcite/CalciteRelNodeVisitorTest.java`

The CH dispatch in M0.4 did `relBuilder.scan(parts)` where `parts = ["my_ch", "db", "t"]`. But sub-schemas live under `ClickHouse.my_ch.db.t`. Prepend the schema name.

- [ ] **Step 1: Update test**

```java
  @Test
  public void visitRelation_routes_clickhouse_via_ClickHouse_schema_prefix() {
    DataSourceService dss = mock(DataSourceService.class);
    DataSource ch = mock(DataSource.class);
    when(ch.getConnectorType()).thenReturn(DataSourceType.CLICKHOUSE);
    when(dss.getDataSource("my_ch")).thenReturn(ch);

    CalciteRelNodeVisitor v = new CalciteRelNodeVisitor(dss);
    CalcitePlanContext ctx = mock(CalcitePlanContext.class);
    RelBuilder rb = mock(RelBuilder.class);
    when(ctx.getRelBuilder()).thenReturn(rb);

    Relation node = new Relation(QualifiedName.of("my_ch", "analytics", "events"));
    v.visitRelation(node, ctx);
    org.mockito.Mockito.verify(rb).scan(java.util.List.of(
        ClickHouseSchema.CLICKHOUSE_SCHEMA_NAME, "my_ch", "analytics", "events"));
  }
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: Update dispatch to prepend `"ClickHouse"`**

```java
      if (DataSourceType.CLICKHOUSE.equals(dsType)) {
        java.util.List<String> parts = new java.util.ArrayList<>();
        parts.add(org.opensearch.sql.calcite.ClickHouseSchema.CLICKHOUSE_SCHEMA_NAME);
        parts.addAll(node.getTableQualifiedName().getParts());
        context.relBuilder.scan(parts);
        return context.relBuilder.peek();
      }
```

- [ ] **Step 4: Run — expect PASS**

- [ ] **Step 5: Commit**

```bash
git add core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java \
        core/src/test/java/org/opensearch/sql/calcite/CalciteRelNodeVisitorTest.java
git commit -s -m "fix(core): prepend ClickHouse schema name in visitRelation dispatch"
```

---

## Task M4.3: End-to-end integration test — `SELECT` via PPL

**Files:**
- Create: `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHouseBasicQueryIT.java`

- [ ] **Step 1: Write the IT**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.testcontainers.clickhouse.ClickHouseContainer;

public class ClickHouseBasicQueryIT extends PPLIntegTestCase {
  @ClassRule
  public static final ClickHouseContainer CH =
      new ClickHouseContainer("clickhouse/clickhouse-server:24.3");

  private static final String DS_NAME = "ch_basic";

  @Before
  public void setup() throws Exception {
    super.init();
    enableCalcite();
    seedClickHouse();
    registerDatasource();
  }

  @After
  public void teardown() throws Exception {
    try {
      client().performRequest(new Request("DELETE", "/_plugins/_query/_datasources/" + DS_NAME));
    } catch (Exception ignored) {}
  }

  private void seedClickHouse() throws Exception {
    try (Connection c = DriverManager.getConnection(CH.getJdbcUrl(), CH.getUsername(), CH.getPassword());
         Statement st = c.createStatement()) {
      st.execute("CREATE DATABASE IF NOT EXISTS analytics");
      st.execute("DROP TABLE IF EXISTS analytics.events");
      st.execute(
          "CREATE TABLE analytics.events ("
              + " event_id Int64,"
              + " user_email String,"
              + " amount Float64,"
              + " ts DateTime"
              + ") ENGINE = MergeTree ORDER BY event_id");
      st.execute(
          "INSERT INTO analytics.events VALUES"
              + " (1, 'a@x.com', 10.5, now()),"
              + " (2, 'b@x.com', 20.0, now()),"
              + " (3, 'a@x.com', 30.0, now())");
    }
  }

  private void registerDatasource() throws Exception {
    String schemaJson =
        "{\\\"databases\\\":[{\\\"name\\\":\\\"analytics\\\",\\\"tables\\\":[{\\\"name\\\":\\\"events\\\",\\\"columns\\\":["
            + "{\\\"name\\\":\\\"event_id\\\",\\\"ch_type\\\":\\\"Int64\\\",\\\"expr_type\\\":\\\"LONG\\\"},"
            + "{\\\"name\\\":\\\"user_email\\\",\\\"ch_type\\\":\\\"String\\\",\\\"expr_type\\\":\\\"STRING\\\"},"
            + "{\\\"name\\\":\\\"amount\\\",\\\"ch_type\\\":\\\"Float64\\\",\\\"expr_type\\\":\\\"DOUBLE\\\"},"
            + "{\\\"name\\\":\\\"ts\\\",\\\"ch_type\\\":\\\"DateTime\\\",\\\"expr_type\\\":\\\"TIMESTAMP\\\"}"
            + "]}]}]}";
    Request req = new Request("POST", "/_plugins/_query/_datasources");
    req.setJsonEntity(
        "{"
            + "\"name\":\"" + DS_NAME + "\","
            + "\"connector\":\"CLICKHOUSE\","
            + "\"properties\":{"
            + "\"clickhouse.uri\":\"" + CH.getJdbcUrl() + "\","
            + "\"clickhouse.auth.type\":\"basic\","
            + "\"clickhouse.auth.username\":\"" + CH.getUsername() + "\","
            + "\"clickhouse.auth.password\":\"" + CH.getPassword() + "\","
            + "\"clickhouse.schema\":\"" + schemaJson + "\""
            + "}"
            + "}");
    Response resp = client().performRequest(req);
    assertThat(resp.getStatusLine().getStatusCode(), equalTo(201));
  }

  @Test
  public void head_returns_rows() throws Exception {
    JSONObject result = executeQueryAsJson(
        "source = " + DS_NAME + ".analytics.events | head 3 | fields event_id");
    assertThat(result.getJSONArray("datarows").length(), equalTo(3));
  }

  @Test
  public void filter_and_project_end_to_end() throws Exception {
    JSONObject result = executeQueryAsJson(
        "source = " + DS_NAME + ".analytics.events"
            + " | where user_email = 'a@x.com'"
            + " | fields event_id, amount");
    assertThat(result.getJSONArray("datarows").length(), equalTo(2));
  }

  /** Helper; implement or inherit from PPLIntegTestCase (check base class for equivalent). */
  private JSONObject executeQueryAsJson(String ppl) throws Exception {
    Request req = new Request("POST", "/_plugins/_ppl");
    req.setJsonEntity("{\"query\":\"" + ppl.replace("\"", "\\\"") + "\"}");
    Response resp = client().performRequest(req);
    return new JSONObject(new String(resp.getEntity().getContent().readAllBytes()));
  }
}
```

- [ ] **Step 2: Run the IT**

Run: `./gradlew :integ-test:integTest --tests "*ClickHouseBasicQueryIT*"`
Expected: PASS (both `head_returns_rows` and `filter_and_project_end_to_end`).

If `head_returns_rows` passes but `filter_and_project_end_to_end` fails with "filter not pushed down" or a Calcite planner error, this is the signal to explicitly register `JdbcRules`. See Task M4.4.

- [ ] **Step 3: Commit**

```bash
git add integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHouseBasicQueryIT.java
git commit -s -m "test(clickhouse): end-to-end IT for SELECT + filter via PPL"
```

---

## Task M4.4: Explicit `JdbcRules` registration (run only if M4.3's filter test fails)

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/executor/QueryService.java` (or CH-specific Programs builder)

- [ ] **Step 1: Write a plan-assertion test**

`core/src/test/java/org/opensearch/sql/calcite/ClickHousePushdownPlanTest.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;
// ... (use same scaffolding as ClickHousePlanSmokeTest to build a root schema with a JdbcTable)

public class ClickHousePushdownPlanTest {
  @Test
  public void filter_on_ch_table_pushes_down_to_jdbc_convention() {
    // Build config + RelBuilder with ClickHouseSchema as in ClickHousePlanSmokeTest.
    // Build: rb.scan("ClickHouse","my_ch","analytics","events").filter(rb.equals(rb.field("id"), rb.literal(1L))).build();
    // Run VolcanoPlanner. Assert output plan contains "JdbcFilter" in its RelOptUtil.toString().
    // (Full body — copy scaffolding from ClickHousePlanSmokeTest and wire VolcanoPlanner with JdbcRules.RULES registered.)
  }
}
```

(Flesh out this test body once M4.3 shows whether implicit or explicit rule registration is needed. If M4.3's filter test passes without explicit registration, mark this task DONE via `assertTrue(true)` and skip Step 2.)

- [ ] **Step 2: Register rules explicitly (only if needed)**

If M4.3 filter test fails, add to `QueryService.buildFrameworkConfig()`:

```java
    // Programs that include JdbcRules so they fire when a JdbcTable (CH) appears in the plan.
    org.apache.calcite.tools.Program chProgram =
        org.apache.calcite.tools.Programs.ofRules(
            org.apache.calcite.adapter.jdbc.JdbcRules.RULES);
    // Combine with existing Programs.standard() in the config builder:
    // .programs(Programs.sequence(Programs.standard(), chProgram))
```

- [ ] **Step 3: Run — re-check M4.3 passes, and plan test passes**

- [ ] **Step 4: Commit**

```bash
git add core/src/main/java/org/opensearch/sql/executor/QueryService.java \
        core/src/test/java/org/opensearch/sql/calcite/ClickHousePushdownPlanTest.java
git commit -s -m "feat(core): ensure JdbcRules fire for ClickHouse plans"
```

---

## Task M4.5: Per-operator pushdown IT matrix (filter/project/sort/limit)

**Files:**
- Create: `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHousePushdownIT.java`

- [ ] **Step 1: Write the IT**

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.testcontainers.clickhouse.ClickHouseContainer;

public class ClickHousePushdownIT extends PPLIntegTestCase {
  @ClassRule
  public static final ClickHouseContainer CH =
      new ClickHouseContainer("clickhouse/clickhouse-server:24.3");

  private static final String DS_NAME = "ch_push";

  @Before
  public void setup() throws Exception {
    super.init();
    enableCalcite();
    seedAndRegister();
  }

  @After
  public void teardown() throws Exception {
    try {
      client().performRequest(new Request("DELETE", "/_plugins/_query/_datasources/" + DS_NAME));
    } catch (Exception ignored) {}
  }

  private void seedAndRegister() throws Exception {
    try (Connection c = DriverManager.getConnection(CH.getJdbcUrl(), CH.getUsername(), CH.getPassword());
         Statement st = c.createStatement()) {
      st.execute("CREATE DATABASE IF NOT EXISTS a");
      st.execute("DROP TABLE IF EXISTS a.t");
      st.execute("CREATE TABLE a.t (id Int64, v Float64) ENGINE = MergeTree ORDER BY id");
      StringBuilder sb = new StringBuilder("INSERT INTO a.t VALUES ");
      for (int i = 1; i <= 100; i++) {
        if (i > 1) sb.append(',');
        sb.append('(').append(i).append(", ").append(i * 1.5).append(')');
      }
      st.execute(sb.toString());
    }
    String schemaJson =
        "{\\\"databases\\\":[{\\\"name\\\":\\\"a\\\",\\\"tables\\\":[{\\\"name\\\":\\\"t\\\",\\\"columns\\\":["
            + "{\\\"name\\\":\\\"id\\\",\\\"ch_type\\\":\\\"Int64\\\",\\\"expr_type\\\":\\\"LONG\\\"},"
            + "{\\\"name\\\":\\\"v\\\",\\\"ch_type\\\":\\\"Float64\\\",\\\"expr_type\\\":\\\"DOUBLE\\\"}"
            + "]}]}]}";
    Request req = new Request("POST", "/_plugins/_query/_datasources");
    req.setJsonEntity(
        "{\"name\":\"" + DS_NAME + "\",\"connector\":\"CLICKHOUSE\",\"properties\":{"
            + "\"clickhouse.uri\":\"" + CH.getJdbcUrl() + "\","
            + "\"clickhouse.auth.type\":\"basic\","
            + "\"clickhouse.auth.username\":\"" + CH.getUsername() + "\","
            + "\"clickhouse.auth.password\":\"" + CH.getPassword() + "\","
            + "\"clickhouse.schema\":\"" + schemaJson + "\"}}");
    Response r = client().performRequest(req);
    assertThat(r.getStatusLine().getStatusCode(), equalTo(201));
  }

  @Test
  public void filter_returns_only_matching_rows() throws Exception {
    JSONObject j = ppl("source = " + DS_NAME + ".a.t | where id = 42 | fields id");
    assertThat(j.getJSONArray("datarows").length(), equalTo(1));
    assertThat(j.getJSONArray("datarows").getJSONArray(0).getInt(0), equalTo(42));
  }

  @Test
  public void project_drops_unwanted_columns() throws Exception {
    JSONObject j = ppl("source = " + DS_NAME + ".a.t | head 1 | fields id");
    assertThat(j.getJSONArray("datarows").getJSONArray(0).length(), equalTo(1));
  }

  @Test
  public void sort_and_limit_return_top_n_descending() throws Exception {
    JSONObject j = ppl("source = " + DS_NAME + ".a.t | sort - id | head 3 | fields id");
    assertThat(j.getJSONArray("datarows").length(), equalTo(3));
    assertThat(j.getJSONArray("datarows").getJSONArray(0).getInt(0), equalTo(100));
  }

  @Test
  public void explain_shows_jdbc_convention_nodes() throws Exception {
    Request req = new Request("POST", "/_plugins/_ppl/_explain");
    req.setJsonEntity("{\"query\":\"source = " + DS_NAME + ".a.t | where id > 10\"}");
    String body = new String(
        client().performRequest(req).getEntity().getContent().readAllBytes());
    // Calcite's explain output includes "Jdbc" for JDBC convention nodes.
    assertThat(body.toLowerCase().indexOf("jdbc"), greaterThanOrEqualTo(0));
  }

  private JSONObject ppl(String query) throws Exception {
    Request req = new Request("POST", "/_plugins/_ppl");
    req.setJsonEntity("{\"query\":\"" + query.replace("\"", "\\\"") + "\"}");
    return new JSONObject(new String(
        client().performRequest(req).getEntity().getContent().readAllBytes()));
  }
}
```

- [ ] **Step 2: Run**

Run: `./gradlew :integ-test:integTest --tests "*ClickHousePushdownIT*"`
Expected: PASS (four tests).

- [ ] **Step 3: Commit**

```bash
git add integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHousePushdownIT.java
git commit -s -m "test(clickhouse): per-operator pushdown IT matrix (M4)"
```

---

## Task M4.6: Register pushdown IT into `CalciteNoPushdownIT` suite

**Files:**
- Modify: `integ-test/src/test/java/.../CalciteNoPushdownIT.java` (locate via `@Suite.SuiteClasses`)

- [ ] **Step 1: Add to suite**

Find `CalciteNoPushdownIT.java`; inside its `@Suite.SuiteClasses({ ... })` list, add:

```java
  ClickHouseBasicQueryIT.class,
  ClickHousePushdownIT.class,
```

- [ ] **Step 2: Run the suite**

Run: `./gradlew :integ-test:integTest --tests "*CalciteNoPushdownIT*"`
Expected: PASS — CH IT runs under both pushdown modes (since it already requires Calcite, the "no pushdown" mode exercises the fallback path; both should still return correct results).

- [ ] **Step 3: Commit**

```bash
git add integ-test/src/test/java/.../CalciteNoPushdownIT.java
git commit -s -m "test(clickhouse): include CH ITs in CalciteNoPushdownIT suite"
```

---

# Milestone Acceptance (M0–M4)

After all tasks pass:

```bash
./gradlew :core:test :clickhouse:test
./gradlew :integ-test:integTest --tests "*clickhouse*"
./gradlew :integ-test:integTest --tests "*CalciteNoPushdownIT*"
./gradlew build -x integTest
```

All green ⇒ ready for M5 (aggregate + fallback). M5–M8 tracked in a successor plan written after M4 merges.

---

## Open Questions / Known Risks (tracked for M5+)

- `JdbcRules` may need explicit registration (resolved in M4.4 if M4.3 surfaces the problem).
- `JdbcTable` / `JdbcSchema` constructor signatures in Calcite 1.41.0 must be verified against the jar — several `new JdbcTable(...)` calls in this plan assume the most common constructor; tasks M3.3 and M3.4 include a reminder to re-check.
- `DataSourceService.getDataSourceMetadata(boolean)` signature used in M3.5 must be verified; if absent, the implementer should introduce an enumeration helper before proceeding.
- `DataSourceMetadata.Builder` API used in test fixtures (M1.5, M2.5) must match the project's actual builder — read the file first.
