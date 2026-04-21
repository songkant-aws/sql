# PPL → ClickHouse Pushdown Coverage Expansion Implementation Plan

> **Status (2026-04-21, end-of-implementation):** Tasks 1–6 and Task 13 shipped. Tasks 7–12 did NOT ship — they were reverted (Task 7) or dropped (8–12) because of Calcite 1.41 JDBC-convention rule constraints we did not surface during brainstorming. See the companion spec's **Implementation Outcome** section for the architectural rationale (short version: `JdbcProjectRule` / `JdbcFilterRule` reject UDFs unconditionally; `JdbcAggregateRule` checks only `SqlKind`, not `SqlOperator`). The task list below is the **original plan as written**, not the shipped result — it is preserved for historical / audit reasons. Do NOT use it as a to-do for net-new work.
>
> **Actually shipped (by commit):** `52fc7a0bd` (HEP hook), `ebfc433df` (math whitelist), `a55b80b0e` (string whitelist), `e74017b89` (predicate whitelist), `06f447d23` (window whitelist), `46e6a1018` (stat-aggregate whitelist), `67cdb450a` (Tier-1 end-to-end ITs). Reverts: `263567245` (SpanBucket), `30c3ced6b` (UDAF unparsers), `c4ffb9de3` (datetime unparser).

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the `ClickHouseSqlDialect` whitelist and add HEP rewrites so PPL operators that already lower to `Project / Filter / Aggregate / Project(RexOver)` get pushed to ClickHouse instead of running in Enumerable.

**Architecture:** Three tiers. Tier 1 is a pure whitelist expansion in `ClickHouseSqlDialect.supportsFunction` / `supportsAggregateFunction`. Tier 2 adds a dialect `unparseCall` override (delegating to per-domain unparsers) and HEP rules for rewrites that restructure the tree (span/bin, percentile). Tier 3 is a reactive defensive guard — only added if a plan test shows Calcite's `PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW` alternative winning cost.

**Tech Stack:** Apache Calcite 1.41 (`JdbcRules`, `HepPlanner`, `RelRule`, `SqlDialect.unparseCall`, `SqlAggFunction`), JUnit 5 (clickhouse module unit / plan tests), JUnit 4 (integ-test module ITs), ClickHouse JDBC driver, testcontainers.

**Branch:** `feat/ppl-federation` (continuation — no new branch). Every task commits with DCO sign-off (`git commit -s`). Commit messages use the `feat(ch-pushdown): ...` / `test(ch-pushdown): ...` prefix.

**Working directory:** `/workspace/songkant/sql/.worktrees/ppl-federation`

**Module layout:**
- `clickhouse/` — dialect, unparsers, HEP rules, registration, plan tests
- `core/` — HEP rule registration hook (`CalciteToolsHelper`)
- `integ-test/` — end-to-end ITs against a live ClickHouse

---

## File Structure

### Files that already exist and get modified

- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java`
  — expand `supportsFunction` + `supportsAggregateFunction`; add `unparseCall` delegation (Tasks 2, 3, 4, 5, 6, 7, 9, 10, 12).

- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/federation/CalciteFederationRegistration.java`
  — register new HEP rules idempotently (Tasks 8, 11, 13).

- `core/src/main/java/org/opensearch/sql/calcite/utils/CalciteToolsHelper.java`
  — add `registerHepRule` hook mirroring `registerVolcanoRule` (Task 1).

- `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialectTest.java`
  — unit tests per whitelist family.

- `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHousePushdownIT.java`
  — per-tier ITs with `query_log` assertions.

### New files

- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseDateTimeUnparser.java`
  — datetime unparse map + handler interface (Task 7).

- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseAggregateUnparser.java`
  — aggregate rename unparse (Task 10).

- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/SpanBucketToStdOpRule.java`
  — HEP rule for `SPAN_BUCKET` rewrites (Tasks 8, 9).

- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/PplUdafToChAggRule.java`
  — HEP rule for `PERCENTILE_APPROX → quantile(p)(x)` (Task 11).

- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/ChQuantileAggFunction.java`
  — custom `SqlAggFunction` emitting curried `quantile(p)(x)` (Task 11).

- `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseDateTimeUnparserTest.java`
- `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseAggregateUnparserTest.java`
- `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/SpanBucketToStdOpRuleTest.java`
- `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/PplUdafToChAggRuleTest.java`

---

## Task conventions

**Gradle test commands** (run from worktree root):

```bash
# Dialect unit tests (fast)
./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialectTest' -q

# Pushdown unit tests (new package)
./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.pushdown.*' -q

# Plan test
./gradlew :clickhouse:test --tests 'org.opensearch.sql.calcite.ClickHousePushdownPlanTest' -q

# Full clickhouse module test (regression guard)
./gradlew :clickhouse:test -q

# Integration tests against live CH (testcontainers)
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.clickhouse.ClickHousePushdownIT' -q
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.clickhouse.ClickHouseFederationIT' -q
```

**Commit template**:

```bash
git commit -s -m "$(cat <<'EOF'
<type>(ch-pushdown): <short summary>

<body explaining why, mentioning the tier and operator family>
EOF
)"
```

---

## Task 1: HEP rule registration hook

**Why:** Today, `CalciteToolsHelper` only exposes `registerVolcanoRule`. Tier-2 rules (`SpanBucketToStdOpRule`, `PplUdafToChAggRule`) are HEP-phase, not Volcano-phase — they must run before `JdbcRules` get a chance to pick a convention. We need the matching `registerHepRule` symmetry.

**Files:**
- Modify: `core/src/main/java/org/opensearch/sql/calcite/utils/CalciteToolsHelper.java` (lines 581-610)
- Test: `core/src/test/java/org/opensearch/sql/calcite/utils/CalciteToolsHelperHepHookTest.java` (new)

- [ ] **Step 1: Write the failing test**

Create `core/src/test/java/org/opensearch/sql/calcite/utils/CalciteToolsHelperHepHookTest.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.junit.jupiter.api.Test;

/** Pins the {@code registerHepRule} hook contract — mirrors {@code registerVolcanoRule}. */
public class CalciteToolsHelperHepHookTest {

  @Test
  public void register_hep_rule_is_idempotent_and_visible() {
    RelOptRule rule = FilterMergeRule.Config.DEFAULT.toRule();
    int before = CalciteToolsHelper.getExtraHepRules().size();
    CalciteToolsHelper.registerHepRule(rule);
    CalciteToolsHelper.registerHepRule(rule); // idempotent
    int after = CalciteToolsHelper.getExtraHepRules().size();

    assertEquals(before + 1, after, "registerHepRule must be idempotent");
    assertTrue(
        CalciteToolsHelper.getExtraHepRules().contains(rule),
        "registered rule must be visible via getExtraHepRules()");
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :core:test --tests 'org.opensearch.sql.calcite.utils.CalciteToolsHelperHepHookTest' -q`

Expected: FAIL with `registerHepRule` / `getExtraHepRules` not found.

- [ ] **Step 3: Add the hook to `CalciteToolsHelper.java`**

In `core/src/main/java/org/opensearch/sql/calcite/utils/CalciteToolsHelper.java`, between the existing `getExtraVolcanoRules()` method (line 600) and the `HEP_PROGRAM` declaration (line 602), insert:

```java
  // Extra HEP rules contributed by optional modules (e.g. clickhouse). These run in the
  // HEP phase (before Volcano) alongside the built-in hepRuleList.
  private static final CopyOnWriteArrayList<RelOptRule> extraHepRules =
      new CopyOnWriteArrayList<>();

  public static void registerHepRule(RelOptRule rule) {
    if (extraHepRules.stream().noneMatch(r -> r == rule)) {
      extraHepRules.add(rule);
    }
  }

  public static List<RelOptRule> getExtraHepRules() {
    return List.copyOf(extraHepRules);
  }
```

Then replace the `HEP_PROGRAM` declaration (currently line 602-603) with one that includes the extra rules at build time:

```java
  private static HepProgram buildHepProgram() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleCollection(hepRuleList);
    List<RelOptRule> extras = getExtraHepRules();
    if (!extras.isEmpty()) {
      builder.addRuleCollection(extras);
    }
    return builder.build();
  }
```

And update `optimize(...)` (currently line 605-610) to build the program fresh each call (cheap — it's just a list wrapper):

```java
  public static RelNode optimize(RelNode plan, CalcitePlanContext context) {
    Util.discard(context);
    HepPlanner planner = new HepPlanner(buildHepProgram());
    planner.setRoot(plan);
    return planner.findBestExp();
  }
```

Delete the old static `HEP_PROGRAM` field (it's no longer referenced).

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :core:test --tests 'org.opensearch.sql.calcite.utils.CalciteToolsHelperHepHookTest' -q`

Expected: PASS.

- [ ] **Step 5: Run core regression sweep**

Run: `./gradlew :core:test -q`

Expected: PASS (no existing core tests broken by the HEP program change).

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/org/opensearch/sql/calcite/utils/CalciteToolsHelper.java \
        core/src/test/java/org/opensearch/sql/calcite/utils/CalciteToolsHelperHepHookTest.java
git commit -s -m "$(cat <<'EOF'
feat(ch-pushdown): add registerHepRule hook to CalciteToolsHelper

Mirrors registerVolcanoRule so optional modules (clickhouse) can contribute
HEP-phase rules. Used next by SpanBucketToStdOpRule and PplUdafToChAggRule
which need to rewrite before Volcano picks the JDBC convention.
EOF
)"
```

---

## Task 2: Tier-1 scalar math whitelist

**Why:** `ABS, CEIL, FLOOR, SQRT, EXP, LN, LOG10, POWER, ROUND, MOD, SIGN, SIN, COS, TAN, ATAN, ATAN2` all unparse to CH-native syntax already. The only gate is `supportsFunction`.

**Files:**
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java`
- Test: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialectTest.java`

- [ ] **Step 1: Write the failing test**

Append to `ClickHouseSqlDialectTest.java` (after `supports_standard_aggregates`):

```java
  @Test
  public void supports_scalar_math_functions() {
    org.apache.calcite.rel.type.RelDataTypeFactory tf =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    org.apache.calcite.rel.type.RelDataType dbl = tf.createSqlType(
        org.apache.calcite.sql.type.SqlTypeName.DOUBLE);
    java.util.List<org.apache.calcite.rel.type.RelDataType> oneDouble = java.util.List.of(dbl);
    java.util.List<org.apache.calcite.rel.type.RelDataType> twoDouble = java.util.List.of(dbl, dbl);

    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.ABS, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.CEIL, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.FLOOR, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.SQRT, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.EXP, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.LN, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.LOG10, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.POWER, dbl, twoDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.ROUND, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.SIGN, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.SIN, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.COS, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.TAN, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.ATAN, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.ATAN2, dbl, twoDouble));
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialectTest.supports_scalar_math_functions' -q`

Expected: FAIL — `supportsFunction` returns `false` for these ops.

- [ ] **Step 3: Expand the `SqlKind` switch in `ClickHouseSqlDialect.supportsFunction`**

In `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java`, change the first `switch (k)` block (currently lines 63-76) to add the math kinds:

```java
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
      // Tier-1 scalar math
      case CEIL: case FLOOR:
        return true;
      default:
        break;
    }
```

Then in the name-based `switch` block (currently lines 78-89), add the math names:

```java
    String name = op.getName().toUpperCase();
    switch (name) {
      case "SUBSTRING":
      case "LOWER":
      case "UPPER":
      case "LENGTH":
      case "TRIM":
      case "CONCAT":
      case "DATE_TRUNC":
      // Tier-1 scalar math (name-based because SqlKind is OTHER_FUNCTION)
      case "ABS":
      case "SQRT":
      case "EXP":
      case "LN":
      case "LOG10":
      case "POWER":
      case "ROUND":
      case "SIGN":
      case "SIN":
      case "COS":
      case "TAN":
      case "ATAN":
      case "ATAN2":
        return true;
      default:
        return false;
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialectTest.supports_scalar_math_functions' -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialectTest.java
git commit -s -m "$(cat <<'EOF'
feat(ch-pushdown): Tier-1 scalar math whitelist

ABS/CEIL/FLOOR/SQRT/EXP/LN/LOG10/POWER/ROUND/SIGN/SIN/COS/TAN/ATAN/ATAN2
now pass the supportsFunction gate so JdbcProjectRule / JdbcFilterRule
accept them into the JDBC subtree. All unparse to CH-native syntax already.
EOF
)"
```

---

## Task 3: Tier-1 scalar string whitelist

**Why:** `POSITION`, `CHAR_LENGTH`, `REPLACE`, `REVERSE` unparse to CH syntax already.

**Files:**
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java`
- Test: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialectTest.java`

- [ ] **Step 1: Write the failing test**

Append to `ClickHouseSqlDialectTest.java`:

```java
  @Test
  public void supports_scalar_string_functions() {
    org.apache.calcite.rel.type.RelDataTypeFactory tf =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    org.apache.calcite.rel.type.RelDataType str = tf.createSqlType(
        org.apache.calcite.sql.type.SqlTypeName.VARCHAR);
    org.apache.calcite.rel.type.RelDataType intT = tf.createSqlType(
        org.apache.calcite.sql.type.SqlTypeName.INTEGER);
    java.util.List<org.apache.calcite.rel.type.RelDataType> oneStr = java.util.List.of(str);
    java.util.List<org.apache.calcite.rel.type.RelDataType> twoStr = java.util.List.of(str, str);
    java.util.List<org.apache.calcite.rel.type.RelDataType> threeStr =
        java.util.List.of(str, str, str);

    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.POSITION, intT, twoStr));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.CHAR_LENGTH, intT, oneStr));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.REPLACE, str, threeStr));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.REVERSE, str, oneStr));
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialectTest.supports_scalar_string_functions' -q`

Expected: FAIL.

- [ ] **Step 3: Add to the name-based switch in `ClickHouseSqlDialect.supportsFunction`**

Extend the name switch:

```java
      case "SUBSTRING":
      case "LOWER":
      case "UPPER":
      case "LENGTH":
      case "TRIM":
      case "CONCAT":
      case "DATE_TRUNC":
      case "ABS": case "SQRT": case "EXP": case "LN": case "LOG10":
      case "POWER": case "ROUND": case "SIGN":
      case "SIN": case "COS": case "TAN": case "ATAN": case "ATAN2":
      // Tier-1 scalar string
      case "POSITION":
      case "CHAR_LENGTH":
      case "REPLACE":
      case "REVERSE":
        return true;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialectTest.supports_scalar_string_functions' -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialectTest.java
git commit -s -m "$(cat <<'EOF'
feat(ch-pushdown): Tier-1 scalar string whitelist

POSITION/CHAR_LENGTH/REPLACE/REVERSE now pass supportsFunction.
All map to CH-native functions of the same name.
EOF
)"
```

---

## Task 4: Tier-1 predicate whitelist (SEARCH / IS_TRUE / IS_FALSE)

**Why:** `SEARCH` is Calcite's consolidated in-list / range predicate (already handled by `unparseCall` in the base dialect). `IS_TRUE` / `IS_FALSE` unparse to syntax CH accepts (`expr = true` / `expr = false`).

**Files:**
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java`
- Test: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialectTest.java`

- [ ] **Step 1: Write the failing test**

Append to `ClickHouseSqlDialectTest.java`:

```java
  @Test
  public void supports_predicate_functions() {
    org.apache.calcite.rel.type.RelDataTypeFactory tf =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    org.apache.calcite.rel.type.RelDataType bool = tf.createSqlType(
        org.apache.calcite.sql.type.SqlTypeName.BOOLEAN);
    java.util.List<org.apache.calcite.rel.type.RelDataType> oneBool = java.util.List.of(bool);

    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_TRUE, bool, oneBool));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_FALSE, bool, oneBool));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.SEARCH, bool, oneBool));
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialectTest.supports_predicate_functions' -q`

Expected: FAIL.

- [ ] **Step 3: Add predicate kinds to the `SqlKind` switch**

```java
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
      case CEIL: case FLOOR:
      // Tier-1 predicate
      case IS_TRUE: case IS_FALSE: case SEARCH:
        return true;
      default:
        break;
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialectTest.supports_predicate_functions' -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialectTest.java
git commit -s -m "$(cat <<'EOF'
feat(ch-pushdown): Tier-1 predicate whitelist (IS_TRUE/IS_FALSE/SEARCH)

SEARCH is Calcite's consolidated in-list/range; IS_TRUE/IS_FALSE
unparse to CH-native equality checks.
EOF
)"
```

---

## Task 5: Tier-1 window analytics whitelist

**Why:** `ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTH_VALUE, FIRST_VALUE, LAST_VALUE` — CH names match Calcite's. `ClickHouseSqlDialect` already inherits `supportsWindowFunctions() = true` from the base `SqlDialect`, so `JdbcProjectRule` allows `Project(RexOver)`. We just need the per-op whitelist.

**Files:**
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java`
- Test: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialectTest.java`

- [ ] **Step 1: Write the failing test**

Append to `ClickHouseSqlDialectTest.java`:

```java
  @Test
  public void supports_window_analytic_functions() {
    org.apache.calcite.rel.type.RelDataTypeFactory tf =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    org.apache.calcite.rel.type.RelDataType bigint = tf.createSqlType(
        org.apache.calcite.sql.type.SqlTypeName.BIGINT);
    org.apache.calcite.rel.type.RelDataType dbl = tf.createSqlType(
        org.apache.calcite.sql.type.SqlTypeName.DOUBLE);
    java.util.List<org.apache.calcite.rel.type.RelDataType> none = java.util.List.of();
    java.util.List<org.apache.calcite.rel.type.RelDataType> oneDouble = java.util.List.of(dbl);
    java.util.List<org.apache.calcite.rel.type.RelDataType> twoDouble =
        java.util.List.of(dbl, bigint);

    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.ROW_NUMBER, bigint, none));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.RANK, bigint, none));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.DENSE_RANK, bigint, none));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.LAG, dbl, twoDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.LEAD, dbl, twoDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.FIRST_VALUE, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.LAST_VALUE, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.NTH_VALUE, dbl, twoDouble));
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialectTest.supports_window_analytic_functions' -q`

Expected: FAIL.

- [ ] **Step 3: Add window kinds to the `SqlKind` switch**

```java
    switch (k) {
      // ... existing cases ...
      case CEIL: case FLOOR:
      case IS_TRUE: case IS_FALSE: case SEARCH:
      // Tier-1 window analytics
      case ROW_NUMBER: case RANK: case DENSE_RANK:
      case LAG: case LEAD:
      case FIRST_VALUE: case LAST_VALUE: case NTH_VALUE:
        return true;
      default:
        break;
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialectTest.supports_window_analytic_functions' -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialectTest.java
git commit -s -m "$(cat <<'EOF'
feat(ch-pushdown): Tier-1 window analytic whitelist

ROW_NUMBER/RANK/DENSE_RANK/LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE.
ClickHouseSqlDialect already inherits supportsWindowFunctions=true from
the base SqlDialect, so JdbcProjectRule admits Project(RexOver). This
task unblocks | dedup, | rare, | top, | eventstats, | trendline,
| timechart, and pure-projection | appendcol / | streamstats.
EOF
)"
```

---

## Task 6: Tier-1 statistical aggregate whitelist (STDDEV / VAR)

**Why:** CH accepts `stddevPop / stddevSamp / varPop / varSamp` natively, and Calcite's default unparse uses the ANSI names which CH also accepts.

**Files:**
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java`
- Test: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialectTest.java`

- [ ] **Step 1: Write the failing test**

Append to `ClickHouseSqlDialectTest.java`:

```java
  @Test
  public void supports_statistical_aggregates() {
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsAggregateFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.STDDEV_POP));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsAggregateFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.STDDEV_SAMP));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsAggregateFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.VAR_POP));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsAggregateFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.VAR_SAMP));
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialectTest.supports_statistical_aggregates' -q`

Expected: FAIL — `supportsAggregateFunction` only returns true for COUNT/SUM/AVG/MIN/MAX.

- [ ] **Step 3: Extend `supportsAggregateFunction` in `ClickHouseSqlDialect`**

Replace the existing two overloads (currently lines 47-55) with:

```java
  @Override
  public boolean supportsAggregateFunction(SqlKind kind) {
    switch (kind) {
      case COUNT: case SUM: case AVG: case MIN: case MAX:
      // Tier-1 statistical aggregates
      case STDDEV_POP: case STDDEV_SAMP: case VAR_POP: case VAR_SAMP:
        return true;
      default:
        return false;
    }
  }

  public boolean supportsAggregateFunction(org.apache.calcite.sql.SqlOperator op) {
    return supportsAggregateFunction(op.getKind());
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialectTest.supports_statistical_aggregates' -q`

Expected: PASS.

- [ ] **Step 5: Run all dialect tests**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialectTest' -q`

Expected: PASS (all prior tests still pass).

- [ ] **Step 6: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialectTest.java
git commit -s -m "$(cat <<'EOF'
feat(ch-pushdown): Tier-1 statistical aggregate whitelist

STDDEV_POP/STDDEV_SAMP/VAR_POP/VAR_SAMP. CH accepts ANSI names verbatim.
EOF
)"
```

---

## Task 7: Tier-2 DateTime unparser

**Why:** PPL's `DATE_FORMAT`, `DATE_ADD`, `DATE_SUB`, `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `NOW`, `UNIX_TIMESTAMP`, `FROM_UNIXTIME` are registered as UDFs that emit via Calcite's default `UserDefinedFunction` unparse. The output (`"DATE_FORMAT"(expr, 'fmt')`) isn't valid CH. We need a dialect `unparseCall` override that rewrites them to CH-native (`formatDateTime(expr, 'fmt')`, `toYear(expr)`, `addDays(expr, 7)` etc.).

**Scope:** the "high-frequency 9" per the spec. Other datetime UDFs fall back to Enumerable — no regression.

**Strategy:** Map `Map<String, UnparseHandler>` keyed by uppercase operator name is the single source of truth. `supportsFunction` consults it; `unparseCall` delegates. Keeps whitelist and SQL-gen atomic — you can't accidentally approve an op with no handler.

**Files:**
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseDateTimeUnparser.java`
- Create: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseDateTimeUnparserTest.java`
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java`

- [ ] **Step 1: Write the failing unit test**

Create `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseDateTimeUnparserTest.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect;

public class ClickHouseDateTimeUnparserTest {

  @Test
  public void registers_all_high_frequency_datetime_ops() {
    // Whitelist size pins the scope — if someone adds or removes a handler,
    // this test changes and they must justify it in the PR.
    assertEquals(12, ClickHouseDateTimeUnparser.handlerCount(),
        "expected 12 datetime handlers (high-frequency 9 + extract helpers)");
  }

  @Test
  public void unparses_date_format_to_formatDateTime() {
    String sql = unparseCall("DATE_FORMAT", "ts_col", "'%Y-%m-%d'");
    assertTrue(sql.contains("formatDateTime"),
        "expected formatDateTime but got: " + sql);
    assertFalse(sql.contains("DATE_FORMAT"),
        "must rewrite away DATE_FORMAT but got: " + sql);
  }

  @Test
  public void unparses_year_to_toYear() {
    String sql = unparseCall("YEAR", "ts_col");
    assertTrue(sql.contains("toYear"), "expected toYear but got: " + sql);
  }

  @Test
  public void unparses_now_to_now() {
    String sql = unparseCall("NOW");
    assertTrue(sql.contains("now"), "expected now() but got: " + sql);
  }

  @Test
  public void unknown_op_returns_null_handler() {
    assertFalse(ClickHouseDateTimeUnparser.hasHandler("NOT_A_DATETIME_FN"));
  }

  // Minimal unparse harness — build a SqlBasicCall by name and run it through
  // the dialect's writer. We don't need Calcite's type checker for this — just
  // text shape assertions.
  private static String unparseCall(String opName, String... argSqlFragments) {
    SqlWriter w = new SqlPrettyWriter(ClickHouseSqlDialect.INSTANCE);
    SqlOperator op = ClickHouseDateTimeUnparser.operatorFor(opName);
    SqlNode[] args = new SqlNode[argSqlFragments.length];
    for (int i = 0; i < argSqlFragments.length; i++) {
      args[i] = new org.apache.calcite.sql.SqlIdentifier(argSqlFragments[i], SqlParserPos.ZERO);
    }
    org.apache.calcite.sql.SqlCall call =
        new org.apache.calcite.sql.SqlBasicCall(op, args, SqlParserPos.ZERO);
    ClickHouseDateTimeUnparser.unparse(w, call, 0, 0);
    return w.toString();
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.pushdown.ClickHouseDateTimeUnparserTest' -q`

Expected: FAIL — `ClickHouseDateTimeUnparser` does not exist.

- [ ] **Step 3: Create the unparser**

Create `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseDateTimeUnparser.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Single source of truth for rewriting PPL datetime UDF calls into ClickHouse-native SQL at
 * unparse time. Keyed by uppercase operator name. {@link
 * org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect#supportsFunction} consults this
 * map; {@link org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect#unparseCall} delegates
 * to the matching handler.
 *
 * <p>Keeping the whitelist and the rewrite in the same Map prevents the error mode "approved
 * the function but forgot to add an unparse handler".
 */
public final class ClickHouseDateTimeUnparser {

  /** Handler strategy: rename (same arg order), reorder, or wrap. */
  @FunctionalInterface
  interface UnparseHandler {
    void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec);
  }

  private static final Map<String, UnparseHandler> HANDLERS = new LinkedHashMap<>();

  static {
    // Simple rename: PPL name → CH name, arg order preserved.
    HANDLERS.put("DATE_FORMAT", rename("formatDateTime"));
    HANDLERS.put("NOW", rename("now"));
    HANDLERS.put("UNIX_TIMESTAMP", rename("toUnixTimestamp"));
    HANDLERS.put("FROM_UNIXTIME", rename("fromUnixTimestamp"));
    HANDLERS.put("YEAR", rename("toYear"));
    HANDLERS.put("MONTH", rename("toMonth"));
    HANDLERS.put("DAY", rename("toDayOfMonth"));
    HANDLERS.put("DAYOFMONTH", rename("toDayOfMonth"));
    HANDLERS.put("HOUR", rename("toHour"));
    HANDLERS.put("MINUTE", rename("toMinute"));
    HANDLERS.put("SECOND", rename("toSecond"));
    // DATE_ADD and DATE_SUB: CH uses unit-specific functions (addDays, subtractHours, ...).
    // We emit the generic addSeconds/subtractSeconds fallback only when we can't prove the unit
    // from the call shape. That fallback works for any numeric interval argument and is CH-native.
    HANDLERS.put("DATE_ADD", rename("addSeconds"));
    HANDLERS.put("DATE_SUB", rename("subtractSeconds"));
  }

  private ClickHouseDateTimeUnparser() {}

  public static boolean hasHandler(String upperName) {
    return HANDLERS.containsKey(upperName);
  }

  public static int handlerCount() {
    return HANDLERS.size();
  }

  /** Public for dialect delegation. */
  public static void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    String key = call.getOperator().getName().toUpperCase();
    UnparseHandler h = HANDLERS.get(key);
    if (h == null) {
      // Fall back to Calcite's default unparse — caller should have gated on hasHandler, but
      // belt-and-braces.
      call.getOperator().unparse(writer, call, leftPrec, rightPrec);
      return;
    }
    h.unparse(writer, call, leftPrec, rightPrec);
  }

  /** Test-only: minimal SqlOperator for unit harness. Never registered in real planning. */
  public static SqlOperator operatorFor(String upperName) {
    return new SqlFunction(
        upperName,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  private static UnparseHandler rename(String chFunctionName) {
    return (writer, call, l, r) -> {
      SqlWriter.Frame frame = writer.startFunCall(chFunctionName);
      for (SqlNode arg : call.getOperandList()) {
        writer.sep(",");
        arg.unparse(writer, 0, 0);
      }
      writer.endFunCall(frame);
    };
  }
}
```

- [ ] **Step 4: Wire the dialect to delegate**

In `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java`, add an `unparseCall` override (after `unparseOffsetFetch`):

```java
  @Override
  public void unparseCall(SqlWriter writer, org.apache.calcite.sql.SqlCall call,
                          int leftPrec, int rightPrec) {
    String name = call.getOperator().getName().toUpperCase();
    if (org.opensearch.sql.clickhouse.calcite.pushdown.ClickHouseDateTimeUnparser
        .hasHandler(name)) {
      org.opensearch.sql.clickhouse.calcite.pushdown.ClickHouseDateTimeUnparser
          .unparse(writer, call, leftPrec, rightPrec);
      return;
    }
    super.unparseCall(writer, call, leftPrec, rightPrec);
  }
```

Also extend `supportsFunction` to consult the datetime map. Add this block at the top of the name-based switch (before the existing `case "SUBSTRING":` block):

```java
    // Tier-2 datetime delegation
    if (org.opensearch.sql.clickhouse.calcite.pushdown.ClickHouseDateTimeUnparser
        .hasHandler(name)) {
      return true;
    }
```

- [ ] **Step 5: Run unit tests**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.pushdown.ClickHouseDateTimeUnparserTest' -q`

Expected: PASS.

- [ ] **Step 6: Run full dialect test**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialectTest' -q`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseDateTimeUnparser.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseDateTimeUnparserTest.java \
        clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java
git commit -s -m "$(cat <<'EOF'
feat(ch-pushdown): Tier-2 datetime unparse handlers

Map-keyed PPL→CH datetime rewrites: DATE_FORMAT→formatDateTime,
YEAR/MONTH/DAY→toYear/toMonth/toDayOfMonth, HOUR/MINUTE/SECOND→
toHour/toMinute/toSecond, NOW→now(), UNIX_TIMESTAMP→toUnixTimestamp,
FROM_UNIXTIME→fromUnixTimestamp, DATE_ADD/SUB→addSeconds/subtractSeconds.
Single-map source-of-truth so supportsFunction and unparseCall cannot
drift.
EOF
)"
```

---

## Task 8: Tier-2 SpanBucketToStdOpRule — unconditional numeric branch

**Why:** PPL `bin span=5` emits `SPAN_BUCKET(col, 5, "NONE")` — numeric, no time semantics. The semantically-equivalent pure-SQL rewrite is `FLOOR(col / 5) * 5`. That's standard SQL and works for any target (not just CH). Separating into two HEP rules (unconditional numeric first, CH-guarded time in Task 9) keeps the safe-for-all-targets rewrite from being accidentally CH-scoped.

**Files:**
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/SpanBucketToStdOpRule.java`
- Create: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/SpanBucketToStdOpRuleTest.java`
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/federation/CalciteFederationRegistration.java`

- [ ] **Step 1: Write the failing unit test**

Create `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/SpanBucketToStdOpRuleTest.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

/** Unit coverage for {@link SpanBucketToStdOpRule} numeric branch. */
public class SpanBucketToStdOpRuleTest {

  @Test
  public void numeric_span_rewrites_to_floor_mul() {
    // Build: Project[SPAN_BUCKET(id, 5, "NONE")] on a stub relation.
    RelBuilder rb = TestRelBuilder.newBuilder();
    rb.values(new String[]{"id"}, 1, 2, 3);
    RexNode spanCall =
        rb.call(
            SpanBucketOperators.SPAN_BUCKET,
            rb.field("id"),
            rb.literal(5),
            rb.literal("NONE"));
    RelNode before = rb.project(spanCall).build();

    HepPlanner planner = new HepPlanner(
        new HepProgramBuilder().addRuleInstance(SpanBucketToStdOpRule.INSTANCE).build());
    planner.setRoot(before);
    RelNode after = planner.findBestExp();

    String tree = RelNodeToString.explain(after);
    assertTrue(tree.contains("FLOOR"),
        "expected FLOOR after rewrite, got: " + tree);
    assertTrue(tree.contains("*"),
        "expected multiply-back after rewrite, got: " + tree);
    assertFalse(tree.contains("SPAN_BUCKET"),
        "SPAN_BUCKET must be gone after rewrite, got: " + tree);
  }

  @Test
  public void non_literal_width_does_not_match() {
    // If the width isn't a literal we cannot safely rewrite — rule must no-op.
    RelBuilder rb = TestRelBuilder.newBuilder();
    rb.values(new String[]{"id", "w"}, 1, 2, 3, 4);
    RexNode spanCall =
        rb.call(
            SpanBucketOperators.SPAN_BUCKET,
            rb.field("id"),
            rb.field("w"), // non-literal!
            rb.literal("NONE"));
    RelNode before = rb.project(spanCall).build();

    HepPlanner planner = new HepPlanner(
        new HepProgramBuilder().addRuleInstance(SpanBucketToStdOpRule.INSTANCE).build());
    planner.setRoot(before);
    RelNode after = planner.findBestExp();

    String tree = RelNodeToString.explain(after);
    assertTrue(tree.contains("SPAN_BUCKET"),
        "SPAN_BUCKET must survive when width is non-literal, got: " + tree);
  }

  @Test
  public void time_unit_does_not_match_in_numeric_branch() {
    // Time-unit span is the CH-guarded branch (Task 9) — this rule must not touch it.
    RelBuilder rb = TestRelBuilder.newBuilder();
    rb.values(new String[]{"ts"}, 1L);
    RexNode spanCall =
        rb.call(
            SpanBucketOperators.SPAN_BUCKET,
            rb.field("ts"),
            rb.literal(1),
            rb.literal("DAY"));
    RelNode before = rb.project(spanCall).build();

    HepPlanner planner = new HepPlanner(
        new HepProgramBuilder().addRuleInstance(SpanBucketToStdOpRule.INSTANCE).build());
    planner.setRoot(before);
    RelNode after = planner.findBestExp();

    String tree = RelNodeToString.explain(after);
    assertTrue(tree.contains("SPAN_BUCKET"),
        "time-unit span must be left for Task 9's CH-guarded branch, got: " + tree);
  }
}
```

This test depends on two test-only helpers (`TestRelBuilder`, `RelNodeToString`, and the `SpanBucketOperators` reference). Create them now:

Create `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/TestRelBuilder.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.FrameworkConfig;

/** Minimal RelBuilder factory for pushdown-rule unit tests. No real schema needed. */
final class TestRelBuilder {
  private TestRelBuilder() {}

  static RelBuilder newBuilder() {
    SchemaPlus root = Frameworks.createRootSchema(true);
    FrameworkConfig cfg = Frameworks.newConfigBuilder().defaultSchema(root).build();
    return RelBuilder.create(cfg);
  }
}
```

Create `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/RelNodeToString.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;

import java.io.PrintWriter;
import java.io.StringWriter;

/** Test helper: explain a RelNode as plain string for substring assertions. */
final class RelNodeToString {
  private RelNodeToString() {}

  static String explain(RelNode rel) {
    StringWriter sw = new StringWriter();
    RelWriter rw = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, false);
    rel.explain(rw);
    return sw.toString();
  }
}
```

Create `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/SpanBucketOperators.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Test-only reference to the PPL SPAN_BUCKET operator name. We don't need the full UDF
 * registration — we just need a SqlOperator instance the rule can recognise by name.
 */
final class SpanBucketOperators {
  static final SqlOperator SPAN_BUCKET =
      new SqlFunction(
          "SPAN_BUCKET",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0,
          null,
          OperandTypes.VARIADIC,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  private SpanBucketOperators() {}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.pushdown.SpanBucketToStdOpRuleTest' -q`

Expected: FAIL — `SpanBucketToStdOpRule` does not exist.

- [ ] **Step 3: Create the rule**

Create `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/SpanBucketToStdOpRule.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.immutables.value.Value;

/**
 * HEP rule: rewrites {@code SPAN_BUCKET(col, n_literal, "NONE")} (numeric span with no time
 * semantics) into the standard-SQL form {@code FLOOR(col / n) * n}. Unconditional — runs for
 * any downstream convention, because FLOOR and multiply are in every SQL dialect.
 *
 * <p>Does NOT match time-unit spans (DAY/HOUR/etc.) — those are handled by the CH-guarded
 * companion rule in Task 9 (extended {@link SpanBucketToStdOpRule} — same class, new branch).
 * Does NOT match when the width argument is a non-literal.
 */
@Value.Enclosing
public final class SpanBucketToStdOpRule extends RelRule<SpanBucketToStdOpRule.Config> {

  public static final SpanBucketToStdOpRule INSTANCE = Config.DEFAULT.toRule();

  private static final String SPAN_BUCKET = "SPAN_BUCKET";
  private static final String NONE_UNIT = "NONE";

  private SpanBucketToStdOpRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Project project = call.rel(0);
    RexBuilder rb = project.getCluster().getRexBuilder();
    final boolean[] changed = {false};

    RexShuttle shuttle =
        new RexShuttle() {
          @Override
          public RexNode visitCall(RexCall c) {
            if (c.getOperator().getName().equalsIgnoreCase(SPAN_BUCKET)) {
              RexNode rewritten = tryRewriteNumeric(c, rb);
              if (rewritten != null) {
                changed[0] = true;
                return rewritten;
              }
            }
            return super.visitCall(c);
          }
        };

    List<RexNode> newProjects = new ArrayList<>();
    for (RexNode p : project.getProjects()) {
      newProjects.add(p.accept(shuttle));
    }

    if (!changed[0]) {
      return; // no SPAN_BUCKET(numeric, literal) found — nothing to do.
    }

    LogicalProject rewritten =
        LogicalProject.create(
            project.getInput(),
            project.getHints(),
            newProjects,
            project.getRowType().getFieldNames(),
            project.getVariablesSet());
    call.transformTo(rewritten);
  }

  /** Returns rewritten RexNode or null if this call doesn't match the numeric branch. */
  private static RexNode tryRewriteNumeric(RexCall c, RexBuilder rb) {
    List<RexNode> ops = c.getOperands();
    if (ops.size() != 3) {
      return null;
    }
    RexNode col = ops.get(0);
    RexNode width = ops.get(1);
    RexNode unit = ops.get(2);
    if (!(width instanceof RexLiteral) || !(unit instanceof RexLiteral)) {
      return null;
    }
    Object unitVal = ((RexLiteral) unit).getValue2();
    if (unitVal == null || !NONE_UNIT.equalsIgnoreCase(unitVal.toString())) {
      // Time-unit branch — left for Task 9.
      return null;
    }
    // FLOOR(col / width) * width
    RexNode divide = rb.makeCall(SqlStdOperatorTable.DIVIDE, col, width);
    RexNode floor = rb.makeCall(SqlStdOperatorTable.FLOOR, divide);
    return rb.makeCall(SqlStdOperatorTable.MULTIPLY, floor, width);
  }

  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        ImmutableSpanBucketToStdOpRule.Config.builder()
            .operandSupplier(b -> b.operand(LogicalProject.class).anyInputs())
            .description("SpanBucketToStdOpRule")
            .build();

    @Override
    default SpanBucketToStdOpRule toRule() {
      return new SpanBucketToStdOpRule(this);
    }
  }
}
```

- [ ] **Step 4: Run unit tests**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.pushdown.SpanBucketToStdOpRuleTest' -q`

Expected: PASS.

- [ ] **Step 5: Register the rule in `CalciteFederationRegistration`**

In `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/federation/CalciteFederationRegistration.java`, extend `ensureRegistered()`:

```java
  public static void ensureRegistered() {
    if (DONE.compareAndSet(false, true)) {
      CalciteToolsHelper.registerVolcanoRule(SideInputInListRule.INSTANCE);
      // Tier-2 HEP rewrites
      CalciteToolsHelper.registerHepRule(
          org.opensearch.sql.clickhouse.calcite.pushdown.SpanBucketToStdOpRule.INSTANCE);
      SideInputBindableHookRegistry.register(SideInputBindableWrapper::maybeWrap);
    }
  }
```

- [ ] **Step 6: Run full clickhouse module tests (regression)**

Run: `./gradlew :clickhouse:test -q`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/SpanBucketToStdOpRule.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/SpanBucketToStdOpRuleTest.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/TestRelBuilder.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/RelNodeToString.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/SpanBucketOperators.java \
        clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/federation/CalciteFederationRegistration.java
git commit -s -m "$(cat <<'EOF'
feat(ch-pushdown): Tier-2 SpanBucket numeric rewrite (HEP)

Rewrites SPAN_BUCKET(col, n_literal, "NONE") into FLOOR(col/n)*n.
Unconditional — standard SQL, works for any downstream convention.
Time-unit branch is deferred to Task 9 (CH-guarded).
EOF
)"
```

---

## Task 9: Tier-2 SpanBucketToStdOpRule — CH-guarded time branch

**Why:** PPL `bin span=1h` emits `SPAN_BUCKET(ts, 1, "HOUR")`. The semantic equivalent is `toStartOfInterval(ts, INTERVAL 1 HOUR)` — but that's CH-specific, so it can only be emitted when the target is CH. We guard by checking that the Project's convention resolves to the JDBC convention bound to `ClickHouseSqlDialect`.

**Files:**
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/SpanBucketToStdOpRule.java`
- Modify: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/SpanBucketToStdOpRuleTest.java`

- [ ] **Step 1: Write the failing test**

Append to `SpanBucketToStdOpRuleTest.java`:

```java
  @Test
  public void time_unit_span_rewrites_to_toStartOfInterval_when_ch_bound() {
    // Mark the project as CH-bound via a custom trait (simulated by a wrapper Project that the
    // rule's guard helper recognises). In HEP phase, the real convention is usually NONE, so
    // our guard reads a hint/marker rather than the trait itself — documented in the rule.
    RelBuilder rb = TestRelBuilder.newBuilder();
    rb.values(new String[]{"ts"}, 1L);
    RexNode spanCall =
        rb.call(
            SpanBucketOperators.SPAN_BUCKET,
            rb.field("ts"),
            rb.literal(1),
            rb.literal("HOUR"));
    RelNode before =
        ChBoundProject.mark(rb.project(spanCall).build()); // helper wraps with a CH marker hint

    HepPlanner planner = new HepPlanner(
        new HepProgramBuilder().addRuleInstance(SpanBucketToStdOpRule.INSTANCE).build());
    planner.setRoot(before);
    RelNode after = planner.findBestExp();

    String tree = RelNodeToString.explain(after);
    assertTrue(tree.toLowerCase().contains("tostartofinterval"),
        "expected toStartOfInterval after CH-bound rewrite, got: " + tree);
    assertFalse(tree.contains("SPAN_BUCKET"),
        "SPAN_BUCKET must be gone, got: " + tree);
  }

  @Test
  public void time_unit_span_not_rewritten_when_not_ch_bound() {
    RelBuilder rb = TestRelBuilder.newBuilder();
    rb.values(new String[]{"ts"}, 1L);
    RexNode spanCall =
        rb.call(
            SpanBucketOperators.SPAN_BUCKET,
            rb.field("ts"),
            rb.literal(1),
            rb.literal("HOUR"));
    RelNode before = rb.project(spanCall).build(); // no CH marker

    HepPlanner planner = new HepPlanner(
        new HepProgramBuilder().addRuleInstance(SpanBucketToStdOpRule.INSTANCE).build());
    planner.setRoot(before);
    RelNode after = planner.findBestExp();

    String tree = RelNodeToString.explain(after);
    assertTrue(tree.contains("SPAN_BUCKET"),
        "time-unit SPAN_BUCKET must survive when target is not CH, got: " + tree);
  }
```

Create `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/ChBoundProject.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalProject;

/** Test helper: attach the CH marker hint the rule uses to identify CH-bound subtrees. */
final class ChBoundProject {
  private ChBoundProject() {}

  static RelNode mark(RelNode rel) {
    if (!(rel instanceof Project)) {
      return rel;
    }
    Project p = (Project) rel;
    RelHint hint = RelHint.builder(SpanBucketToStdOpRule.CH_HINT_NAME).build();
    return LogicalProject.create(
        p.getInput(),
        List.of(hint),
        p.getProjects(),
        p.getRowType().getFieldNames(),
        p.getVariablesSet());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.pushdown.SpanBucketToStdOpRuleTest' -q`

Expected: FAIL — `CH_HINT_NAME` undefined, time-unit branch returns null, `toStartOfInterval` not emitted.

- [ ] **Step 3: Extend the rule**

In `SpanBucketToStdOpRule.java`, add the hint constant, CH-guard helper, and time-unit branch. Replace `tryRewriteNumeric(...)` with a dispatcher that handles both:

```java
  public static final String CH_HINT_NAME = "CH_CONVENTION_BOUND";

  private static final java.util.Set<String> TIME_UNITS =
      java.util.Set.of("SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "YEAR");

  @Override
  public void onMatch(RelOptRuleCall call) {
    Project project = call.rel(0);
    RexBuilder rb = project.getCluster().getRexBuilder();
    final boolean chBound = isChBound(project);
    final boolean[] changed = {false};

    RexShuttle shuttle =
        new RexShuttle() {
          @Override
          public RexNode visitCall(RexCall c) {
            if (c.getOperator().getName().equalsIgnoreCase(SPAN_BUCKET)) {
              RexNode rewritten = tryRewrite(c, rb, chBound);
              if (rewritten != null) {
                changed[0] = true;
                return rewritten;
              }
            }
            return super.visitCall(c);
          }
        };

    List<RexNode> newProjects = new ArrayList<>();
    for (RexNode p : project.getProjects()) {
      newProjects.add(p.accept(shuttle));
    }

    if (!changed[0]) {
      return;
    }

    LogicalProject rewritten =
        LogicalProject.create(
            project.getInput(),
            project.getHints(),
            newProjects,
            project.getRowType().getFieldNames(),
            project.getVariablesSet());
    call.transformTo(rewritten);
  }

  private static boolean isChBound(Project p) {
    return p.getHints().stream()
        .anyMatch(h -> CH_HINT_NAME.equalsIgnoreCase(h.hintName));
  }

  private static RexNode tryRewrite(RexCall c, RexBuilder rb, boolean chBound) {
    List<RexNode> ops = c.getOperands();
    if (ops.size() != 3) return null;
    RexNode col = ops.get(0);
    RexNode width = ops.get(1);
    RexNode unit = ops.get(2);
    if (!(width instanceof RexLiteral) || !(unit instanceof RexLiteral)) return null;
    Object unitVal = ((RexLiteral) unit).getValue2();
    if (unitVal == null) return null;
    String unitStr = unitVal.toString().toUpperCase();

    if (NONE_UNIT.equalsIgnoreCase(unitStr)) {
      RexNode divide = rb.makeCall(SqlStdOperatorTable.DIVIDE, col, width);
      RexNode floor = rb.makeCall(SqlStdOperatorTable.FLOOR, divide);
      return rb.makeCall(SqlStdOperatorTable.MULTIPLY, floor, width);
    }

    if (chBound && TIME_UNITS.contains(unitStr)) {
      return buildToStartOfInterval(col, width, unitStr, rb);
    }

    return null; // non-CH time-unit or unsupported unit: leave for Enumerable.
  }

  private static RexNode buildToStartOfInterval(
      RexNode col, RexNode width, String unit, RexBuilder rb) {
    // Emit: toStartOfInterval(col, INTERVAL <width> <unit>)
    // Implemented as a call to the pre-registered CH-side SqlOperator TO_START_OF_INTERVAL,
    // created ad-hoc here. Interval literal is built via Calcite's standard INTERVAL support.
    org.apache.calcite.sql.SqlOperator op =
        new org.apache.calcite.sql.SqlFunction(
            "toStartOfInterval",
            org.apache.calcite.sql.SqlKind.OTHER_FUNCTION,
            org.apache.calcite.sql.type.ReturnTypes.ARG0_NULLABLE,
            null,
            org.apache.calcite.sql.type.OperandTypes.ANY_ANY,
            org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION);
    // Interval constructed as "<width> <unit>" string literal — CH accepts this form in
    // toStartOfInterval (e.g., toStartOfInterval(ts, INTERVAL 1 HOUR)).
    org.apache.calcite.avatica.util.TimeUnit tu =
        org.apache.calcite.avatica.util.TimeUnit.valueOf(unit);
    org.apache.calcite.sql.SqlIntervalQualifier qual =
        new org.apache.calcite.sql.SqlIntervalQualifier(
            tu, null, org.apache.calcite.sql.parser.SqlParserPos.ZERO);
    long widthVal = ((RexLiteral) width).getValueAs(Long.class);
    RexNode interval =
        rb.makeIntervalLiteral(
            java.math.BigDecimal.valueOf(widthVal * multiplierFor(tu)), qual);
    return rb.makeCall(op, col, interval);
  }

  private static long multiplierFor(org.apache.calcite.avatica.util.TimeUnit tu) {
    switch (tu) {
      case SECOND: return 1_000L;
      case MINUTE: return 60_000L;
      case HOUR:   return 3_600_000L;
      case DAY:    return 86_400_000L;
      case WEEK:   return 604_800_000L;
      case MONTH:  return 1L;  // CH months handled as INTERVAL <n> MONTH directly
      case YEAR:   return 1L;  // same
      default:     return 1L;
    }
  }
```

- [ ] **Step 4: Run unit tests**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.pushdown.SpanBucketToStdOpRuleTest' -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/SpanBucketToStdOpRule.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/SpanBucketToStdOpRuleTest.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/ChBoundProject.java
git commit -s -m "$(cat <<'EOF'
feat(ch-pushdown): Tier-2 SpanBucket time-unit branch (CH-guarded HEP)

Rewrites SPAN_BUCKET(col, n, time_unit) to toStartOfInterval(col,
INTERVAL n unit) when the Project carries a CH_CONVENTION_BOUND hint.
Non-CH time-unit spans remain SPAN_BUCKET → evaluated in Enumerable,
no regression for other dialects.
EOF
)"
```

---

## Task 10: Tier-2 AggregateUnparser — DISTINCT_COUNT_APPROX → uniq

**Why:** PPL exposes `DISTINCT_COUNT_APPROX(x)`. CH native `uniq(x)` matches the semantics (HyperLogLog-based approximate distinct count). One whitelist entry + one unparse rename; no tree restructuring needed.

**Files:**
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseAggregateUnparser.java`
- Create: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseAggregateUnparserTest.java`
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java`

- [ ] **Step 1: Write the failing test**

Create `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseAggregateUnparserTest.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect;

public class ClickHouseAggregateUnparserTest {

  @Test
  public void distinct_count_approx_unparses_to_uniq() {
    SqlPrettyWriter w = new SqlPrettyWriter(ClickHouseSqlDialect.INSTANCE);
    SqlNode[] args = new SqlNode[]{
        new SqlIdentifier("x", SqlParserPos.ZERO)
    };
    SqlBasicCall call = new SqlBasicCall(
        ClickHouseAggregateUnparser.operatorFor("DISTINCT_COUNT_APPROX"),
        args,
        SqlParserPos.ZERO);
    ClickHouseAggregateUnparser.unparse(w, call, 0, 0);

    String sql = w.toString();
    assertTrue(sql.contains("uniq"), "expected uniq but got: " + sql);
    assertFalse(sql.contains("DISTINCT_COUNT_APPROX"),
        "must rewrite away DISTINCT_COUNT_APPROX but got: " + sql);
  }

  @Test
  public void has_handler_true_for_distinct_count_approx() {
    assertTrue(ClickHouseAggregateUnparser.hasHandler("DISTINCT_COUNT_APPROX"));
  }

  @Test
  public void has_handler_false_for_unknown() {
    assertFalse(ClickHouseAggregateUnparser.hasHandler("NOT_AN_AGG"));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.pushdown.ClickHouseAggregateUnparserTest' -q`

Expected: FAIL — class doesn't exist.

- [ ] **Step 3: Create the unparser**

Create `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseAggregateUnparser.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Aggregate-function unparse overrides for CH. Same Map-as-single-source-of-truth pattern as
 * {@link ClickHouseDateTimeUnparser}.
 */
public final class ClickHouseAggregateUnparser {

  @FunctionalInterface
  interface UnparseHandler {
    void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec);
  }

  private static final Map<String, UnparseHandler> HANDLERS = new LinkedHashMap<>();

  static {
    HANDLERS.put("DISTINCT_COUNT_APPROX", rename("uniq"));
  }

  private ClickHouseAggregateUnparser() {}

  public static boolean hasHandler(String upperName) {
    return HANDLERS.containsKey(upperName);
  }

  public static void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    String key = call.getOperator().getName().toUpperCase();
    UnparseHandler h = HANDLERS.get(key);
    if (h == null) {
      call.getOperator().unparse(writer, call, leftPrec, rightPrec);
      return;
    }
    h.unparse(writer, call, leftPrec, rightPrec);
  }

  public static SqlOperator operatorFor(String upperName) {
    return new SqlFunction(
        upperName,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  private static UnparseHandler rename(String chFunctionName) {
    return (writer, call, l, r) -> {
      SqlWriter.Frame frame = writer.startFunCall(chFunctionName);
      for (SqlNode arg : call.getOperandList()) {
        writer.sep(",");
        arg.unparse(writer, 0, 0);
      }
      writer.endFunCall(frame);
    };
  }
}
```

- [ ] **Step 4: Wire the dialect**

In `ClickHouseSqlDialect.java`, extend the `unparseCall` override added in Task 7:

```java
  @Override
  public void unparseCall(SqlWriter writer, org.apache.calcite.sql.SqlCall call,
                          int leftPrec, int rightPrec) {
    String name = call.getOperator().getName().toUpperCase();
    if (org.opensearch.sql.clickhouse.calcite.pushdown.ClickHouseDateTimeUnparser
        .hasHandler(name)) {
      org.opensearch.sql.clickhouse.calcite.pushdown.ClickHouseDateTimeUnparser
          .unparse(writer, call, leftPrec, rightPrec);
      return;
    }
    if (org.opensearch.sql.clickhouse.calcite.pushdown.ClickHouseAggregateUnparser
        .hasHandler(name)) {
      org.opensearch.sql.clickhouse.calcite.pushdown.ClickHouseAggregateUnparser
          .unparse(writer, call, leftPrec, rightPrec);
      return;
    }
    super.unparseCall(writer, call, leftPrec, rightPrec);
  }
```

And extend `supportsAggregateFunction(SqlOperator)` overload to consult the map:

```java
  public boolean supportsAggregateFunction(org.apache.calcite.sql.SqlOperator op) {
    if (org.opensearch.sql.clickhouse.calcite.pushdown.ClickHouseAggregateUnparser
        .hasHandler(op.getName().toUpperCase())) {
      return true;
    }
    return supportsAggregateFunction(op.getKind());
  }
```

- [ ] **Step 5: Run tests**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.pushdown.ClickHouseAggregateUnparserTest' -q`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseAggregateUnparser.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/ClickHouseAggregateUnparserTest.java \
        clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/ClickHouseSqlDialect.java
git commit -s -m "$(cat <<'EOF'
feat(ch-pushdown): Tier-2 aggregate rename DISTINCT_COUNT_APPROX→uniq

Same Map-as-SSOT pattern as ClickHouseDateTimeUnparser. Dialect
supportsAggregateFunction(SqlOperator) and unparseCall both consult
the map, so whitelist and rewrite cannot drift.
EOF
)"
```

---

## Task 11: Tier-2 PERCENTILE_APPROX → quantile(p)(x) (HEP)

**Why:** CH's `quantile` uses the curried form `quantile(0.9)(x)` — the percentile `p` is an *aggregate-function parameter*, not an argument. Calcite's default unparse has no way to emit that form, so a simple rename doesn't work. We HEP-rewrite `Aggregate(PERCENTILE_APPROX(x, p_literal))` → `Aggregate(CH_QUANTILE_AGG{p}(x))` where `CH_QUANTILE_AGG` is a custom `SqlAggFunction` that emits the curried syntax in its `unparse`.

**Scope:** only when `p` is a literal (required: CH needs a compile-time parameter) AND the Aggregate carries the CH hint (same marker as Task 9).

**Files:**
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/ChQuantileAggFunction.java`
- Create: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/PplUdafToChAggRule.java`
- Create: `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/PplUdafToChAggRuleTest.java`
- Modify: `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/federation/CalciteFederationRegistration.java`

- [ ] **Step 1: Write the failing test**

Create `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/PplUdafToChAggRuleTest.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

public class PplUdafToChAggRuleTest {

  @Test
  public void percentile_approx_rewrites_to_quantile_when_ch_bound_and_p_literal() {
    RelBuilder rb = TestRelBuilder.newBuilder();
    rb.values(new String[]{"x"}, 1.0, 2.0, 3.0);
    RelNode before = ChBoundAggregate.mark(
        rb.aggregate(
              rb.groupKey(),
              rb.aggregateCall(
                  PercentileApproxOperator.INSTANCE,
                  rb.field("x"),
                  rb.literal(0.9))
                .as("p90"))
          .build());

    HepPlanner planner = new HepPlanner(
        new HepProgramBuilder().addRuleInstance(PplUdafToChAggRule.INSTANCE).build());
    planner.setRoot(before);
    RelNode after = planner.findBestExp();

    String tree = RelNodeToString.explain(after);
    assertTrue(tree.toLowerCase().contains("quantile"),
        "expected quantile after rewrite, got: " + tree);
    assertFalse(tree.contains("PERCENTILE_APPROX"),
        "PERCENTILE_APPROX must be gone, got: " + tree);
  }

  @Test
  public void non_literal_percentile_does_not_rewrite() {
    RelBuilder rb = TestRelBuilder.newBuilder();
    rb.values(new String[]{"x", "p"}, 1.0, 0.9);
    RelNode before = ChBoundAggregate.mark(
        rb.aggregate(
              rb.groupKey(),
              rb.aggregateCall(
                  PercentileApproxOperator.INSTANCE,
                  rb.field("x"),
                  rb.field("p"))
                .as("pn"))
          .build());

    HepPlanner planner = new HepPlanner(
        new HepProgramBuilder().addRuleInstance(PplUdafToChAggRule.INSTANCE).build());
    planner.setRoot(before);
    RelNode after = planner.findBestExp();

    String tree = RelNodeToString.explain(after);
    assertTrue(tree.contains("PERCENTILE_APPROX"),
        "PERCENTILE_APPROX must survive when p is not literal, got: " + tree);
  }

  @Test
  public void non_ch_bound_percentile_does_not_rewrite() {
    RelBuilder rb = TestRelBuilder.newBuilder();
    rb.values(new String[]{"x"}, 1.0);
    RelNode before =
        rb.aggregate(
              rb.groupKey(),
              rb.aggregateCall(
                  PercentileApproxOperator.INSTANCE,
                  rb.field("x"),
                  rb.literal(0.9))
                .as("p90"))
          .build();

    HepPlanner planner = new HepPlanner(
        new HepProgramBuilder().addRuleInstance(PplUdafToChAggRule.INSTANCE).build());
    planner.setRoot(before);
    RelNode after = planner.findBestExp();

    String tree = RelNodeToString.explain(after);
    assertTrue(tree.contains("PERCENTILE_APPROX"),
        "PERCENTILE_APPROX must survive when target isn't CH, got: " + tree);
  }
}
```

Create `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/PercentileApproxOperator.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/** Test-only: stand-in aggregate matching PPL PERCENTILE_APPROX by name. */
final class PercentileApproxOperator {
  static final SqlAggFunction INSTANCE =
      new SqlAggFunction(
          "PERCENTILE_APPROX",
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_FORCE_NULLABLE,
          null,
          OperandTypes.NUMERIC_NUMERIC,
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false,
          org.apache.calcite.sql.validate.SqlMonotonicity.NOT_MONOTONIC) {};

  private PercentileApproxOperator() {}
}
```

Create `clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/ChBoundAggregate.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;

/** Mark an Aggregate with the CH_CONVENTION_BOUND hint (see PplUdafToChAggRule.CH_HINT_NAME). */
final class ChBoundAggregate {
  private ChBoundAggregate() {}

  static RelNode mark(RelNode rel) {
    if (!(rel instanceof Aggregate)) return rel;
    Aggregate a = (Aggregate) rel;
    RelHint hint = RelHint.builder(PplUdafToChAggRule.CH_HINT_NAME).build();
    return LogicalAggregate.create(
        a.getInput(),
        List.of(hint),
        a.getGroupSet(),
        a.getGroupSets(),
        a.getAggCallList());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.pushdown.PplUdafToChAggRuleTest' -q`

Expected: FAIL — none of the production classes exist.

- [ ] **Step 3: Create the custom aggregate function**

Create `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/ChQuantileAggFunction.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import java.util.Objects;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlMonotonicity;

/**
 * Custom aggregate that unparses to the CH curried form {@code quantile(p)(x)}. Parameter
 * {@code p} is baked into this instance so different percentiles produce different aggregate
 * instances in the plan — this matches how Calcite Aggregate keeps its call list.
 */
public final class ChQuantileAggFunction extends SqlAggFunction {

  private final double p;

  public ChQuantileAggFunction(double p) {
    super(
        "quantile",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_FORCE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        SqlMonotonicity.NOT_MONOTONIC);
    this.p = p;
  }

  public double getP() {
    return p;
  }

  @Override
  public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    // Emit: quantile(p)(x)
    // Calcite writer has no native curried form; we emit literally in two FunCall frames.
    writer.keyword("quantile");
    SqlWriter.Frame param = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
    writer.literal(String.valueOf(p));
    writer.endList(param);
    SqlWriter.Frame arg = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
    for (SqlNode a : call.getOperandList()) {
      writer.sep(",");
      a.unparse(writer, 0, 0);
    }
    writer.endList(arg);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ChQuantileAggFunction)) return false;
    return Double.compare(((ChQuantileAggFunction) o).p, p) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash("ChQuantileAggFunction", p);
  }
}
```

- [ ] **Step 4: Create the HEP rule**

Create `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/PplUdafToChAggRule.java`:

```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

/**
 * Rewrite {@code Aggregate( PERCENTILE_APPROX(x, p_literal) )} into {@code Aggregate(
 * ChQuantileAggFunction(p)(x) )} — collapses the percentile argument into the aggregate
 * function identity so it can unparse as the CH curried form.
 *
 * <p>Guard: only fires when the Aggregate carries the {@link #CH_HINT_NAME} hint AND every
 * PERCENTILE_APPROX call's second argument is a RexLiteral.
 */
@Value.Enclosing
public final class PplUdafToChAggRule extends RelRule<PplUdafToChAggRule.Config> {

  public static final String CH_HINT_NAME = "CH_CONVENTION_BOUND";
  public static final PplUdafToChAggRule INSTANCE = Config.DEFAULT.toRule();

  private static final String PERCENTILE_APPROX = "PERCENTILE_APPROX";

  private PplUdafToChAggRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate agg = call.rel(0);
    if (!isChBound(agg)) return;

    List<AggregateCall> newCalls = new ArrayList<>();
    boolean changed = false;
    for (AggregateCall c : agg.getAggCallList()) {
      AggregateCall rewritten = tryRewriteOne(c, agg);
      if (rewritten != null) {
        newCalls.add(rewritten);
        changed = true;
      } else {
        newCalls.add(c);
      }
    }
    if (!changed) return;

    LogicalAggregate out =
        LogicalAggregate.create(
            agg.getInput(),
            agg.getHints(),
            agg.getGroupSet(),
            agg.getGroupSets(),
            newCalls);
    call.transformTo(out);
  }

  private static boolean isChBound(Aggregate agg) {
    return agg.getHints().stream()
        .anyMatch(h -> CH_HINT_NAME.equalsIgnoreCase(h.hintName));
  }

  private static AggregateCall tryRewriteOne(AggregateCall c, Aggregate agg) {
    if (!c.getAggregation().getName().equalsIgnoreCase(PERCENTILE_APPROX)) return null;
    List<Integer> argList = c.getArgList();
    if (argList.size() != 2) return null;

    // Find the second argument's literal value. Since AggregateCall references input columns by
    // index, we need to look up the Project/Values directly beneath. For literal second arg, the
    // input row-type should expose a RexLiteral at that column — but argList is a column index,
    // not a RexNode, so we need to consult the input's rowType. HEP can't inspect a RexNode
    // directly from an AggregateCall; the pattern is to require that the column is a constant
    // propagated through a Project in the input.
    //
    // In the PPL-built plan, PERCENTILE_APPROX(x, 0.9) lands as an Aggregate with argList=[idx_x,
    // idx_p_const] — idx_p_const points to a LiteralProject column. We chase that.
    Double p = findLiteralColumn(agg.getInput(), argList.get(1));
    if (p == null) return null;

    ChQuantileAggFunction chFn = new ChQuantileAggFunction(p);
    return AggregateCall.create(
        chFn,
        c.isDistinct(),
        c.isApproximate(),
        c.ignoreNulls(),
        List.of(argList.get(0)), // only x survives — p is absorbed into the agg identity
        c.filterArg,
        c.collation,
        c.getType(),
        c.getName());
  }

  /**
   * Walks the immediate Project chain below {@code rel} looking for the literal RexNode at
   * {@code columnIndex}. Returns null when the column isn't derivable as a numeric literal.
   */
  private static Double findLiteralColumn(
      org.apache.calcite.rel.RelNode rel, int columnIndex) {
    if (rel instanceof org.apache.calcite.rel.core.Project) {
      org.apache.calcite.rel.core.Project p = (org.apache.calcite.rel.core.Project) rel;
      RexNode n = p.getProjects().get(columnIndex);
      if (n instanceof RexLiteral) {
        Object v = ((RexLiteral) n).getValue2();
        if (v instanceof Number) return ((Number) v).doubleValue();
      }
    }
    return null;
  }

  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        ImmutablePplUdafToChAggRule.Config.builder()
            .operandSupplier(b -> b.operand(LogicalAggregate.class).anyInputs())
            .description("PplUdafToChAggRule")
            .build();

    @Override
    default PplUdafToChAggRule toRule() {
      return new PplUdafToChAggRule(this);
    }
  }
}
```

- [ ] **Step 5: Register the rule in `CalciteFederationRegistration`**

Extend `ensureRegistered()`:

```java
  public static void ensureRegistered() {
    if (DONE.compareAndSet(false, true)) {
      CalciteToolsHelper.registerVolcanoRule(SideInputInListRule.INSTANCE);
      CalciteToolsHelper.registerHepRule(
          org.opensearch.sql.clickhouse.calcite.pushdown.SpanBucketToStdOpRule.INSTANCE);
      CalciteToolsHelper.registerHepRule(
          org.opensearch.sql.clickhouse.calcite.pushdown.PplUdafToChAggRule.INSTANCE);
      SideInputBindableHookRegistry.register(SideInputBindableWrapper::maybeWrap);
    }
  }
```

- [ ] **Step 6: Run tests**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.clickhouse.calcite.pushdown.PplUdafToChAggRuleTest' -q`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/ChQuantileAggFunction.java \
        clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/pushdown/PplUdafToChAggRule.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/PplUdafToChAggRuleTest.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/PercentileApproxOperator.java \
        clickhouse/src/test/java/org/opensearch/sql/clickhouse/calcite/pushdown/ChBoundAggregate.java \
        clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/federation/CalciteFederationRegistration.java
git commit -s -m "$(cat <<'EOF'
feat(ch-pushdown): Tier-2 PERCENTILE_APPROX→quantile(p)(x) (HEP)

Custom ChQuantileAggFunction emits CH's curried quantile syntax. HEP
rule collapses PERCENTILE_APPROX(x, p_literal) into the custom agg with
p baked into the function identity. Guarded on CH_CONVENTION_BOUND hint
and literal p.
EOF
)"
```

---

## Task 12: Attach CH_CONVENTION_BOUND hint to CH-bound subtrees

**Why:** Tasks 9 and 11 guard their rewrites on the `CH_CONVENTION_BOUND` hint. For the guard to fire in real plans, the hint must be attached to RelNodes whose downstream target is the CH JDBC convention. The cleanest attachment point is inside the CH schema's relation creation, when PPL plans first touch a CH-backed table — exactly where we already know the target is CH.

Find where `ClickHouseSchema` / `ClickHouseTable` produces RelNodes (likely via `Table.toRel` or a `ClickHouseTableScan` subclass), and attach the hint through a wrapping Project at plan-build time. If the schema only produces a bare `TableScan` and the first Project is built later, attach via a post-pass over the RelNode in the federation pipeline.

**Files:**
- Modify: a single wiring point — details depend on where CH table scans emerge. The task subagent must find this by searching for `ClickHouseSchema`, `ClickHouseTable`, or the JdbcTable equivalent in the module.
- Test: a plan-level test in `ClickHousePushdownPlanTest.java` that builds a PPL query touching a CH table and asserts the hint appears on the first Project.

- [ ] **Step 1: Locate the CH schema's RelNode entry point**

Search the clickhouse module for where `Table.toRel` or equivalent lives. Commands to run:

```bash
grep -rn "toRel" clickhouse/src/main/java --include '*.java'
grep -rn "class.*Table.*extends" clickhouse/src/main/java --include '*.java'
grep -rn "LogicalTableScan.create\|TableScan.*create" clickhouse/src/main/java --include '*.java'
```

Identify the single file/method that produces the first CH-bound RelNode. Read it fully before deciding where to attach.

- [ ] **Step 2: Write the failing plan test**

Append to `clickhouse/src/test/java/org/opensearch/sql/calcite/ClickHousePushdownPlanTest.java`:

```java
  @Test
  public void first_project_on_ch_table_carries_ch_bound_hint() {
    // Build: source = ch.a.t | fields id   via the module's Calcite facade,
    // then walk the rel tree and assert the first Project (post-TableScan) carries
    // CH_CONVENTION_BOUND.
    //
    // See ClickHousePushdownPlanTest existing setup for how the JdbcSchema stub is
    // built; reuse that. Assertion:
    //   assertTrue(rel has a Project with hint named "CH_CONVENTION_BOUND")
    //
    // The exact query-facade invocation depends on where the module exposes
    // PPL-to-RelNode compilation; the subagent must find it via:
    //   grep -rn "CalciteRelNodeVisitor\|PplParser\|compileToRel" clickhouse/src
    // and use that helper.
    //
    // If no such helper exists in the clickhouse module, the subagent should
    // create a minimal one in this test file only, so the test stays self-
    // contained. Do NOT add a new production helper for this.
  }
```

**Note to implementer:** the precise PPL-parse facade used here depends on Wave-6 helpers already in the clickhouse test tree. If the fixture doesn't exist, write a minimal inline helper using `RelBuilder` + a stub `JdbcTableScan` (similar to what `ClickHousePushdownPlanTest.clickhouse_convention_register_adds_rules_to_planner` already does). The goal is to prove the hint attach happens — not to exercise the full PPL parser.

- [ ] **Step 3: Run test to verify it fails**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.calcite.ClickHousePushdownPlanTest.first_project_on_ch_table_carries_ch_bound_hint' -q`

Expected: FAIL.

- [ ] **Step 4: Attach the hint**

In the CH schema's table-toRel entry point (discovered in Step 1), wrap the produced RelNode in a `LogicalProject` that carries the `RelHint.builder("CH_CONVENTION_BOUND").build()` hint. If the existing code already builds a `LogicalProject`, simply append the hint to its `getHints()` list. Concrete example if the entry point is a method returning `RelNode`:

```java
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    RelNode scan = baseTable.toRel(context, relOptTable);
    // Attach CH marker so HEP rules (SpanBucketToStdOpRule time-branch,
    // PplUdafToChAggRule) can identify this subtree as CH-bound.
    RelHint chHint = RelHint.builder(
        org.opensearch.sql.clickhouse.calcite.pushdown.PplUdafToChAggRule.CH_HINT_NAME).build();
    // Identity project that only carries the hint — lets downstream Projects propagate
    // the marker via hint inheritance.
    RexBuilder rb = scan.getCluster().getRexBuilder();
    List<RexNode> identity = new ArrayList<>();
    for (int i = 0; i < scan.getRowType().getFieldCount(); i++) {
      identity.add(rb.makeInputRef(scan, i));
    }
    return LogicalProject.create(
        scan,
        List.of(chHint),
        identity,
        scan.getRowType().getFieldNames(),
        ImmutableSet.of());
  }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :clickhouse:test --tests 'org.opensearch.sql.calcite.ClickHousePushdownPlanTest.first_project_on_ch_table_carries_ch_bound_hint' -q`

Expected: PASS.

- [ ] **Step 6: Run full clickhouse and integ-test suites to ensure no regression**

```bash
./gradlew :clickhouse:test -q
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.clickhouse.ClickHousePushdownIT' -q
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.clickhouse.ClickHouseFederationIT' -q
```

Expected: ALL PASS. If any CH pushdown / federation IT regresses, the identity Project added here is being mis-interpreted downstream — diagnose before committing.

- [ ] **Step 7: Commit**

```bash
git add clickhouse/src/main/java/... \
        clickhouse/src/test/java/org/opensearch/sql/calcite/ClickHousePushdownPlanTest.java
git commit -s -m "$(cat <<'EOF'
feat(ch-pushdown): attach CH_CONVENTION_BOUND hint to CH-bound RelNodes

Identity LogicalProject carrying the hint is inserted above every CH
table scan. Downstream HEP rules (SpanBucketToStdOpRule time branch,
PplUdafToChAggRule) use the hint to gate their CH-only rewrites.
EOF
)"
```

---

## Task 13: End-to-end IT — math/string/predicate/window/stddev pushdown

**Why:** Tier-1 whitelist changes need runtime proof. Each family (math / string / predicate / window / statistical aggregate) should produce actual SQL in CH's `query_log` containing the CH-native operator.

**Files:**
- Modify: `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHousePushdownIT.java`

- [ ] **Step 1: Add Tier-1 math family IT**

Append a test method near the existing `query_log_confirms_filter_project_sort_pushdown` method:

```java
  @Test
  public void query_log_confirms_tier1_math_pushdown() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE IF EXISTS system.query_log");
    }

    executeQuery(
        "source = " + DS_NAME + ".a.t | eval r = round(v, 1) | fields id, r | head 1");
    executeQuery(
        "source = " + DS_NAME + ".a.t | where abs(id) > 50 | stats count() as c");

    List<String> observed = new ArrayList<>();
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("SYSTEM FLUSH LOGS");
      try (ResultSet rs = st.executeQuery(
          "SELECT query FROM system.query_log "
              + "WHERE type = 'QueryFinish' "
              + "  AND query LIKE '%FROM %`a`.`t`%' "
              + "  AND query NOT LIKE '%system.query_log%' "
              + "ORDER BY event_time ASC")) {
        while (rs.next()) observed.add(rs.getString(1));
      }
    }

    assertThat("expected both math queries in CH log",
        observed.size(), greaterThanOrEqualTo(2));
    String roundSql = findMatching(observed, "ROUND");
    assertThat("ROUND must be pushed into CH SQL", roundSql, containsString("ROUND"));
    String absSql = findMatching(observed, "ABS");
    assertThat("ABS must be pushed into CH SQL", absSql, containsString("ABS"));
  }
```

- [ ] **Step 2: Add Tier-1 window family IT**

```java
  @Test
  public void query_log_confirms_tier1_window_pushdown() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE IF EXISTS system.query_log");
    }

    // `eventstats` naturally lowers to Project(RexOver(ROW_NUMBER/RANK/...)) over the CH table.
    executeQuery(
        "source = " + DS_NAME + ".a.t | eventstats count() as total | head 3");

    List<String> observed = new ArrayList<>();
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("SYSTEM FLUSH LOGS");
      try (ResultSet rs = st.executeQuery(
          "SELECT query FROM system.query_log "
              + "WHERE type = 'QueryFinish' "
              + "  AND query LIKE '%FROM %`a`.`t`%' "
              + "  AND query NOT LIKE '%system.query_log%' "
              + "ORDER BY event_time ASC")) {
        while (rs.next()) observed.add(rs.getString(1));
      }
    }

    assertThat("expected at least one CH log entry for eventstats",
        observed.size(), greaterThanOrEqualTo(1));
    String windowSql = findMatching(observed, "OVER");
    assertThat("window function must push as OVER clause to CH",
        windowSql, containsString("OVER"));
  }
```

- [ ] **Step 3: Add Tier-1 statistical aggregate IT**

```java
  @Test
  public void query_log_confirms_tier1_stddev_pushdown() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE IF EXISTS system.query_log");
    }

    executeQuery(
        "source = " + DS_NAME + ".a.t | stats stddev_pop(v) as sd, var_samp(v) as vs");

    List<String> observed = new ArrayList<>();
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("SYSTEM FLUSH LOGS");
      try (ResultSet rs = st.executeQuery(
          "SELECT query FROM system.query_log "
              + "WHERE type = 'QueryFinish' "
              + "  AND query LIKE '%FROM %`a`.`t`%' "
              + "  AND query NOT LIKE '%system.query_log%' "
              + "ORDER BY event_time ASC")) {
        while (rs.next()) observed.add(rs.getString(1));
      }
    }

    assertThat(observed.size(), greaterThanOrEqualTo(1));
    String sql = observed.get(0);
    assertThat(sql.toUpperCase(), containsString("STDDEV_POP"));
    assertThat(sql.toUpperCase(), containsString("VAR_SAMP"));
  }
```

- [ ] **Step 4: Run Tier-1 ITs**

```bash
./gradlew :integ-test:integTest \
  --tests 'org.opensearch.sql.clickhouse.ClickHousePushdownIT.query_log_confirms_tier1_math_pushdown' \
  --tests 'org.opensearch.sql.clickhouse.ClickHousePushdownIT.query_log_confirms_tier1_window_pushdown' \
  --tests 'org.opensearch.sql.clickhouse.ClickHousePushdownIT.query_log_confirms_tier1_stddev_pushdown' -q
```

Expected: PASS.

- [ ] **Step 5: Run the full ClickHousePushdownIT suite as regression guard**

```bash
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.clickhouse.ClickHousePushdownIT' -q
```

Expected: ALL PASS (the 26 existing assertions plus the 3 new).

- [ ] **Step 6: Commit**

```bash
git add integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHousePushdownIT.java
git commit -s -m "$(cat <<'EOF'
test(ch-pushdown): ITs for Tier-1 math/window/stddev pushdown

Three new IT methods verify round/abs math, eventstats window, and
stddev/var aggregate all produce CH-native SQL in system.query_log.
EOF
)"
```

---

## Task 14: End-to-end IT — Tier-2 datetime/span/UDAF pushdown

**Why:** Tier-2 rewrites need runtime proof — each family must produce CH-native SQL.

**Files:**
- Modify: `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHousePushdownIT.java`

- [ ] **Step 1: Extend `seedAndRegister` with a time-typed column**

The existing table `a.t` is `(id Int64, v Float64)`. Add a `ts DateTime` column for datetime ITs. Replace the CREATE TABLE + INSERT block in `seedAndRegister`:

```java
      st.execute("CREATE TABLE a.t (id Int64, v Float64, ts DateTime) ENGINE = MergeTree ORDER BY id");
      StringBuilder sb = new StringBuilder("INSERT INTO a.t VALUES ");
      for (int i = 1; i <= 100; i++) {
        if (i > 1) sb.append(',');
        sb.append("(")
            .append(i).append(", ")
            .append(i * 1.5).append(", ")
            .append("toDateTime('2024-01-").append(String.format("%02d", (i % 28) + 1))
            .append(" 12:00:00')")
            .append(")");
      }
      st.execute(sb.toString());
```

And extend `schemaJson` to include the `ts` column:

```java
    String schemaJson =
        "{\\\"databases\\\":[{\\\"name\\\":\\\"a\\\",\\\"tables\\\":[{\\\"name\\\":\\\"t\\\",\\\"columns\\\":["
            + "{\\\"name\\\":\\\"id\\\",\\\"ch_type\\\":\\\"Int64\\\",\\\"expr_type\\\":\\\"LONG\\\"},"
            + "{\\\"name\\\":\\\"v\\\",\\\"ch_type\\\":\\\"Float64\\\",\\\"expr_type\\\":\\\"DOUBLE\\\"},"
            + "{\\\"name\\\":\\\"ts\\\",\\\"ch_type\\\":\\\"DateTime\\\",\\\"expr_type\\\":\\\"TIMESTAMP\\\"}"
            + "]}]}]}";
```

- [ ] **Step 2: Run the prior IT suite to prove the schema change is backwards-compatible**

```bash
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.clickhouse.ClickHousePushdownIT' -q
```

Expected: ALL 26 prior tests + 3 Tier-1 ITs PASS. If any fail because the added `ts` column changes row shape assertions, adjust the assertions (not the schema) — the datetime column is required for Tier-2 ITs.

- [ ] **Step 3: Add Tier-2 datetime IT**

```java
  @Test
  public void query_log_confirms_tier2_datetime_pushdown() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE IF EXISTS system.query_log");
    }

    executeQuery(
        "source = " + DS_NAME + ".a.t | eval y = year(ts), m = month(ts) | fields id, y, m | head 1");
    executeQuery(
        "source = " + DS_NAME + ".a.t | eval f = date_format(ts, '%Y-%m-%d') | fields id, f | head 1");

    List<String> observed = new ArrayList<>();
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("SYSTEM FLUSH LOGS");
      try (ResultSet rs = st.executeQuery(
          "SELECT query FROM system.query_log "
              + "WHERE type = 'QueryFinish' "
              + "  AND query LIKE '%FROM %`a`.`t`%' "
              + "  AND query NOT LIKE '%system.query_log%' "
              + "ORDER BY event_time ASC")) {
        while (rs.next()) observed.add(rs.getString(1));
      }
    }
    assertThat(observed.size(), greaterThanOrEqualTo(2));
    String yearSql = findMatching(observed, "toYear");
    assertThat("year() must unparse to toYear", yearSql, containsString("toYear"));
    String fmtSql = findMatching(observed, "formatDateTime");
    assertThat("date_format() must unparse to formatDateTime",
        fmtSql, containsString("formatDateTime"));
  }
```

- [ ] **Step 4: Add Tier-2 span/bin IT**

```java
  @Test
  public void query_log_confirms_tier2_span_numeric_pushdown() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE IF EXISTS system.query_log");
    }

    // Numeric span: bin bucket = FLOOR(id/5)*5 — should push.
    executeQuery(
        "source = " + DS_NAME + ".a.t | bin id span=5 | stats count() as c by id | head 5");

    List<String> observed = new ArrayList<>();
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("SYSTEM FLUSH LOGS");
      try (ResultSet rs = st.executeQuery(
          "SELECT query FROM system.query_log "
              + "WHERE type = 'QueryFinish' "
              + "  AND query LIKE '%FROM %`a`.`t`%' "
              + "  AND query NOT LIKE '%system.query_log%' "
              + "ORDER BY event_time ASC")) {
        while (rs.next()) observed.add(rs.getString(1));
      }
    }
    assertThat(observed.size(), greaterThanOrEqualTo(1));
    String sql = observed.get(0);
    assertThat("numeric span must push as FLOOR/multiply standard SQL",
        sql.toUpperCase(), containsString("FLOOR"));
  }

  @Test
  public void query_log_confirms_tier2_span_time_pushdown() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE IF EXISTS system.query_log");
    }

    // Time span: should push as toStartOfInterval(ts, INTERVAL 1 HOUR).
    executeQuery(
        "source = " + DS_NAME + ".a.t | bin ts span=1h | stats count() as c by ts | head 5");

    List<String> observed = new ArrayList<>();
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("SYSTEM FLUSH LOGS");
      try (ResultSet rs = st.executeQuery(
          "SELECT query FROM system.query_log "
              + "WHERE type = 'QueryFinish' "
              + "  AND query LIKE '%FROM %`a`.`t`%' "
              + "  AND query NOT LIKE '%system.query_log%' "
              + "ORDER BY event_time ASC")) {
        while (rs.next()) observed.add(rs.getString(1));
      }
    }
    assertThat(observed.size(), greaterThanOrEqualTo(1));
    String sql = observed.get(0);
    assertThat("time span must push as toStartOfInterval",
        sql, containsString("toStartOfInterval"));
  }
```

- [ ] **Step 5: Add Tier-2 quantile IT**

```java
  @Test
  public void query_log_confirms_tier2_percentile_pushdown() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE IF EXISTS system.query_log");
    }

    executeQuery(
        "source = " + DS_NAME + ".a.t | stats percentile_approx(v, 0.9) as p90");

    List<String> observed = new ArrayList<>();
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("SYSTEM FLUSH LOGS");
      try (ResultSet rs = st.executeQuery(
          "SELECT query FROM system.query_log "
              + "WHERE type = 'QueryFinish' "
              + "  AND query LIKE '%FROM %`a`.`t`%' "
              + "  AND query NOT LIKE '%system.query_log%' "
              + "ORDER BY event_time ASC")) {
        while (rs.next()) observed.add(rs.getString(1));
      }
    }
    assertThat(observed.size(), greaterThanOrEqualTo(1));
    String sql = observed.get(0);
    assertThat("PERCENTILE_APPROX(x, 0.9) must push as CH quantile(0.9)(x)",
        sql, containsString("quantile(0.9)"));
  }

  @Test
  public void query_log_confirms_tier2_uniq_pushdown() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE IF EXISTS system.query_log");
    }

    executeQuery(
        "source = " + DS_NAME + ".a.t | stats distinct_count_approx(id) as u");

    List<String> observed = new ArrayList<>();
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("SYSTEM FLUSH LOGS");
      try (ResultSet rs = st.executeQuery(
          "SELECT query FROM system.query_log "
              + "WHERE type = 'QueryFinish' "
              + "  AND query LIKE '%FROM %`a`.`t`%' "
              + "  AND query NOT LIKE '%system.query_log%' "
              + "ORDER BY event_time ASC")) {
        while (rs.next()) observed.add(rs.getString(1));
      }
    }
    assertThat(observed.size(), greaterThanOrEqualTo(1));
    String sql = observed.get(0);
    assertThat("DISTINCT_COUNT_APPROX must push as uniq",
        sql, containsString("uniq"));
  }
```

- [ ] **Step 6: Run all Tier-2 ITs**

```bash
./gradlew :integ-test:integTest \
  --tests 'org.opensearch.sql.clickhouse.ClickHousePushdownIT.query_log_confirms_tier2_datetime_pushdown' \
  --tests 'org.opensearch.sql.clickhouse.ClickHousePushdownIT.query_log_confirms_tier2_span_numeric_pushdown' \
  --tests 'org.opensearch.sql.clickhouse.ClickHousePushdownIT.query_log_confirms_tier2_span_time_pushdown' \
  --tests 'org.opensearch.sql.clickhouse.ClickHousePushdownIT.query_log_confirms_tier2_percentile_pushdown' \
  --tests 'org.opensearch.sql.clickhouse.ClickHousePushdownIT.query_log_confirms_tier2_uniq_pushdown' -q
```

Expected: PASS.

- [ ] **Step 7: Run full regression suite**

```bash
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.clickhouse.ClickHousePushdownIT' -q
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.clickhouse.ClickHouseFederationIT' -q
```

Expected: ALL PASS.

- [ ] **Step 8: Commit**

```bash
git add integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHousePushdownIT.java
git commit -s -m "$(cat <<'EOF'
test(ch-pushdown): ITs for Tier-2 datetime/span/UDAF pushdown

Five new ITs prove year()/date_format() unparse to toYear/formatDateTime;
numeric span rewrites to FLOOR/multiply; time span rewrites to
toStartOfInterval; percentile_approx(x,0.9) becomes quantile(0.9)(x);
distinct_count_approx becomes uniq.
EOF
)"
```

---

## Task 15: Tier-3 defensive PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW guard (conditional)

**Why:** Calcite's `CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW` adds a `LogicalWindow` alternative to any `Project(RexOver)`. Under some cost metrics this alternative can win, breaking JDBC pushdown. We only intervene if Tier-1/2 ITs demonstrate the regression.

**Decision gate:** After Task 14 completes, re-run `./gradlew :integ-test:integTest --tests 'org.opensearch.sql.clickhouse.ClickHousePushdownIT.query_log_confirms_tier1_window_pushdown'`. If the window IT passes, **skip this task entirely**. Only implement if that IT shows `LogicalWindow` in the plan instead of `JdbcProject` with `RexOver`.

- [ ] **Step 1: Decision — does Tier-1 window IT show Enumerable fallback?**

Run the window IT. Dump `explain` manually if needed:

```bash
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.clickhouse.ClickHousePushdownIT.query_log_confirms_tier1_window_pushdown' -i
```

Inspect the captured plan:
- If CH SQL contains `OVER` → pushdown worked, skip this task.
- If `LogicalWindow` appears in the RelNode tree and CH SQL has no `OVER` → continue.

- [ ] **Step 2: If needed, remove the rule scoped to CH**

In `CalciteFederationRegistration.ensureRegistered()`, add (gated by whatever mechanism the runtime uses to identify CH plans — typically via the same CH hint or a Volcano rule-set scoped registration):

```java
      CalciteToolsHelper.removeRule(
          org.apache.calcite.rel.rules.CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
```

This requires a corresponding `removeRule` hook in `CalciteToolsHelper` if not already present. Implement both as part of this task if you reach it.

- [ ] **Step 3: Re-run Tier-1 window IT**

Expected: PASS with CH SQL containing `OVER`.

- [ ] **Step 4: Run full regression**

```bash
./gradlew :integ-test:integTest -q
```

Expected: ALL PASS. (In particular the clickhouse convention-expression-fix ITs from `feat/clickhouse-datasource` must stay green.)

- [ ] **Step 5: Commit (only if the guard was necessary)**

```bash
git add core/src/main/java/org/opensearch/sql/calcite/utils/CalciteToolsHelper.java \
        clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/federation/CalciteFederationRegistration.java
git commit -s -m "$(cat <<'EOF'
feat(ch-pushdown): Tier-3 remove PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW

Tier-1 window IT showed <describe concrete failure>. Removing the core
rule inside CH federation registration lets JdbcProjectRule keep the
Project(RexOver) shape and push down OVER clauses verbatim.
EOF
)"
```

---

## Task 16: Final regression sweep and documentation update

- [ ] **Step 1: Run the complete regression matrix**

```bash
./gradlew :core:test -q
./gradlew :clickhouse:test -q
./gradlew :integ-test:integTest --tests 'org.opensearch.sql.clickhouse.*' -q
```

Expected: ALL PASS.

- [ ] **Step 2: Update the design spec with empirical results**

Append a "Landed: Empirical SQL Examples" section to `docs/superpowers/specs/2026-04-21-ppl-ch-pushdown-expansion-design.md` containing one CH SQL snippet per Tier-1 / Tier-2 family, captured from `system.query_log` during the IT run. This is the same pattern as the 2026-04-20 IN-list spec's empirical appendix.

Include for each: PPL query in → CH SQL out. Three to five concrete examples are enough — they are the permanent proof of what actually pushes.

- [ ] **Step 3: Commit doc update**

```bash
git add docs/superpowers/specs/2026-04-21-ppl-ch-pushdown-expansion-design.md
git commit -s -m "$(cat <<'EOF'
docs(ch-pushdown): record empirical pushdown SQL from ITs

Appends per-tier CH SQL snippets captured from system.query_log during
the IT run. Permanent proof of what landed.
EOF
)"
```

- [ ] **Step 4: Push to origin (same branch)**

```bash
git push origin feat/ppl-federation
```

Expected: successful push.

---

## Self-Review

1. **Spec coverage:**
   - Tier-1 whitelist expansion (math/string/predicate/window/stddev) → Tasks 2-6. ✓
   - Tier-2 datetime unparse → Task 7. ✓
   - Tier-2 span/bin HEP rewrite (unconditional + CH-guarded) → Tasks 8, 9. ✓
   - Tier-2 hybrid UDAF (dialect + HEP) → Tasks 10, 11. ✓
   - HEP hook → Task 1. ✓
   - CH hint attachment (the prerequisite for Tasks 9 and 11 guards) → Task 12. ✓
   - Tier-3 defensive guard (conditional) → Task 15. ✓
   - Scope boundary: pure-projection branches only → enforced by the rules themselves and by the "no Correlate/FULL JOIN" non-goals in the spec. ✓
   - Per-tier testing gates → Tasks 13, 14. ✓
   - Regression guardrails (ClickHousePushdownIT 26 + ClickHouseFederationIT 4) → run in Tasks 12, 13, 14, 16. ✓

2. **Placeholder scan:** No "TBD"/"TODO"/"etc." — every step has concrete commands and code. Task 12 Step 1 intentionally leaves the exact search command for the implementer to run because the entry point is discovered dynamically; this is exploration, not a placeholder. Tier-3 Task 15 is explicitly conditional on a runtime decision gate and is documented as such.

3. **Type consistency:** `CH_HINT_NAME = "CH_CONVENTION_BOUND"` is consistent across `SpanBucketToStdOpRule`, `PplUdafToChAggRule`, and the Task-12 attach point. `ChQuantileAggFunction.p` is `double`. Test helpers (`TestRelBuilder`, `RelNodeToString`, `ChBoundProject`, `ChBoundAggregate`, `SpanBucketOperators`, `PercentileApproxOperator`) are package-private in the test tree, consistent with existing module conventions.
