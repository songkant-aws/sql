/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.executor.QueryType;

public class EarliestLatestFoldRuleTest extends UnifiedQueryTestBase {

  @Override
  protected UnifiedQueryContext.Builder contextBuilder() {
    return UnifiedQueryContext.builder()
        .language(QueryType.PPL)
        .catalog(
            DEFAULT_CATALOG,
            new AbstractSchema() {
              @Override
              protected Map<String, Table> getTableMap() {
                return Map.of("events", createEventsTable());
              }
            });
  }

  @Before
  public void setUp() {
    super.setUp();
  }

  private Table createEventsTable() {
    return SimpleTable.builder()
        .col("id", INTEGER)
        .col("name", VARCHAR)
        .col("ts", TIMESTAMP)
        .row(new Object[] {1, "row1", 1672531200000L})
        .row(new Object[] {2, "row2", 1675209600000L})
        .build();
  }

  @Test
  public void testEarliestWithAbsoluteLiteralFoldsToGtEq() {
    RelNode plan =
        givenQuery("source = catalog.events | where earliest('2024-01-15 00:00:00', ts)").plan();
    String tree = RelOptUtil.toString(plan);
    assertTrue(
        "expected fold to emit `>=` against the ts column, got:\n" + tree, tree.contains(">=($2,"));
    assertTrue(
        "expected fold to emit a TIMESTAMP literal, got:\n" + tree,
        tree.contains("2024-01-15 00:00:00"));
    assertNoEarliestUdfRemains(tree);
  }

  @Test
  public void testLatestWithAbsoluteLiteralFoldsToLtEq() {
    RelNode plan =
        givenQuery("source = catalog.events | where latest('2024-01-15 00:00:00', ts)").plan();
    String tree = RelOptUtil.toString(plan);
    assertTrue(
        "expected fold to emit `<=` against the ts column, got:\n" + tree, tree.contains("<=($2,"));
    assertTrue(
        "expected fold to emit a TIMESTAMP literal, got:\n" + tree,
        tree.contains("2024-01-15 00:00:00"));
    assertNoLatestUdfRemains(tree);
  }

  @Test
  public void testEarliestWithRelativeLiteralFoldsToGtEq() {
    // The right-hand side timestamp depends on Instant.now() at fold time, so we
    // only check the operator and that the UDF call is gone.
    RelNode plan = givenQuery("source = catalog.events | where earliest('-7d', ts)").plan();
    String tree = RelOptUtil.toString(plan);
    assertTrue("expected `>=` predicate, got:\n" + tree, tree.contains(">=($2,"));
    assertNoEarliestUdfRemains(tree);
  }

  @Test
  public void testEarliestWithSnapLiteralFolds() {
    RelNode plan = givenQuery("source = catalog.events | where earliest('@d', ts)").plan();
    String tree = RelOptUtil.toString(plan);
    assertTrue("expected `>=` predicate, got:\n" + tree, tree.contains(">=($2,"));
    assertNoEarliestUdfRemains(tree);
  }

  /**
   * Pure-offset DSL (no snap, no absolute) emits {@code now() + INTERVAL} so the
   * backend folds at its own plan time, keeping all migrated datetime functions
   * coherent on the same "now". RHS shape is symbolic, not a TIMESTAMP literal.
   */
  @Test
  public void testEarliestWithPureOffsetEmitsNowSymbolic() {
    RelNode plan = givenQuery("source = catalog.events | where earliest('-7d', ts)").plan();
    String tree = RelOptUtil.toString(plan);
    assertTrue("expected `>=` predicate, got:\n" + tree, tree.contains(">=($2,"));
    assertTrue(
        "expected RHS to be symbolic now() + INTERVAL, got:\n" + tree,
        tree.contains("now()") && tree.contains("INTERVAL"));
    assertFalse(
        "RHS should not be a pre-resolved TIMESTAMP literal, got:\n" + tree,
        tree.matches("(?s).*>=\\(\\$2, \\d{4}-\\d{2}-\\d{2}.*"));
    assertNoEarliestUdfRemains(tree);
  }

  /**
   * Snap DSL keeps the JVM-resolved literal shape because there's no clean substrait
   * primitive for snap-to-day across backends.
   */
  @Test
  public void testEarliestWithSnapKeepsLiteralShape() {
    RelNode plan = givenQuery("source = catalog.events | where earliest('@d', ts)").plan();
    String tree = RelOptUtil.toString(plan);
    assertFalse(
        "snap form must NOT use symbolic now(), got:\n" + tree,
        tree.contains("now()"));
  }

  /**
   * Absolute literal DSL keeps the JVM-resolved shape — there's no "now" involved
   * to defer to the backend.
   */
  @Test
  public void testEarliestWithAbsoluteLiteralKeepsLiteralShape() {
    RelNode plan =
        givenQuery("source = catalog.events | where earliest('2024-01-15 00:00:00', ts)").plan();
    String tree = RelOptUtil.toString(plan);
    assertFalse(
        "absolute form must NOT use symbolic now(), got:\n" + tree,
        tree.contains("now()"));
  }

  @Test
  public void testEarliestWithMixedOffsetAndSnapFolds() {
    RelNode plan = givenQuery("source = catalog.events | where earliest('-1h@d', ts)").plan();
    String tree = RelOptUtil.toString(plan);
    assertTrue("expected `>=` predicate, got:\n" + tree, tree.contains(">=($2,"));
    assertNoEarliestUdfRemains(tree);
  }

  @Test
  public void testCombinedEarliestAndLatestFold() {
    RelNode plan =
        givenQuery(
                "source = catalog.events | where earliest('2024-01-15 00:00:00', ts) AND "
                    + "latest('2024-12-31 23:59:59', ts)")
            .plan();
    String tree = RelOptUtil.toString(plan);
    assertTrue("expected both bounds folded, got:\n" + tree,
        tree.contains(">=($2,") && tree.contains("<=($2,"));
    assertNoEarliestUdfRemains(tree);
    assertNoLatestUdfRemains(tree);
  }

  // ── helpers ────────────────────────────────────────────────────────────────

  private static void assertNoEarliestUdfRemains(String tree) {
    // The DatetimeOutputCastRule wraps the projection in CAST AS VARCHAR, so the
    // EARLIEST UDF call (return type BOOLEAN) should be entirely gone after the
    // fold rule. We grep for the literal operator name to catch any residual.
    assertFalse(
        "EARLIEST UDF must be eliminated by the fold:\n" + tree, tree.contains("EARLIEST("));
  }

  private static void assertNoLatestUdfRemains(String tree) {
    assertFalse("LATEST UDF must be eliminated by the fold:\n" + tree, tree.contains("LATEST("));
  }
}
