/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the pure helpers on {@link SideInputInListRule} plus a smoke test for its rule
 * instance. Full plan-graph validation happens in ClickHouseFederationIT.
 */
class SideInputInListRuleTest {

  private RelBuilder builder;

  @BeforeEach
  void setUp() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    // "os" simulates the OS/left side, "ch" simulates the CH/right side. Both carry a "user_id"
    // column so the rule's name-based mapping from right-top row type to scan row type is
    // exercised end-to-end.
    rootSchema.add(
        "os",
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory f) {
            return f.builder()
                .add("user_id", SqlTypeName.BIGINT)
                .add("name", SqlTypeName.VARCHAR)
                .build();
          }
        });
    rootSchema.add(
        "ch",
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory f) {
            return f.builder()
                .add("user_id", SqlTypeName.BIGINT)
                .add("v", SqlTypeName.DOUBLE)
                .build();
          }
        });
    FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(rootSchema).build();
    builder = RelBuilder.create(config);
  }

  @Test
  void ruleInstanceExists() {
    assertNotNull(SideInputInListRule.INSTANCE);
  }

  // -------------------- isJoinTypeCompatibleWithInListPushdown --------------------

  @Test
  void joinTypeGuardInnerAndSemiAreAlwaysSafe() {
    // INNER and SEMI are orientation-independent: neither preserves rows on either side that
    // don't have a match, so filtering the JDBC side by `key IN (<bounded keys>)` is a no-op on
    // the result for both (drain left, filter right) and (drain right, filter left).
    assertTrue(
        SideInputInListRule.isJoinTypeCompatibleWithInListPushdown(
            JoinRelType.INNER, SideInputSide.RIGHT_IS_JDBC));
    assertTrue(
        SideInputInListRule.isJoinTypeCompatibleWithInListPushdown(
            JoinRelType.INNER, SideInputSide.LEFT_IS_JDBC));
    assertTrue(
        SideInputInListRule.isJoinTypeCompatibleWithInListPushdown(
            JoinRelType.SEMI, SideInputSide.RIGHT_IS_JDBC));
    assertTrue(
        SideInputInListRule.isJoinTypeCompatibleWithInListPushdown(
            JoinRelType.SEMI, SideInputSide.LEFT_IS_JDBC));
  }

  @Test
  void joinTypeGuardLeftAndAntiSafeOnlyWhenFilteringRight() {
    // LEFT preserves unmatched left rows; ANTI returns left rows with no right match. Filtering
    // the *left* would drop exactly the rows that should survive — so these types are only safe
    // when the filter lands on the right (i.e. JDBC is on the right).
    assertTrue(
        SideInputInListRule.isJoinTypeCompatibleWithInListPushdown(
            JoinRelType.LEFT, SideInputSide.RIGHT_IS_JDBC));
    assertFalse(
        SideInputInListRule.isJoinTypeCompatibleWithInListPushdown(
            JoinRelType.LEFT, SideInputSide.LEFT_IS_JDBC));
    assertTrue(
        SideInputInListRule.isJoinTypeCompatibleWithInListPushdown(
            JoinRelType.ANTI, SideInputSide.RIGHT_IS_JDBC));
    assertFalse(
        SideInputInListRule.isJoinTypeCompatibleWithInListPushdown(
            JoinRelType.ANTI, SideInputSide.LEFT_IS_JDBC));
  }

  @Test
  void joinTypeGuardRightSafeOnlyWhenFilteringLeft() {
    // RIGHT preserves unmatched right rows. Filtering the *right* would drop them — so RIGHT is
    // only safe when the filter lands on the left (i.e. JDBC is on the left).
    assertFalse(
        SideInputInListRule.isJoinTypeCompatibleWithInListPushdown(
            JoinRelType.RIGHT, SideInputSide.RIGHT_IS_JDBC));
    assertTrue(
        SideInputInListRule.isJoinTypeCompatibleWithInListPushdown(
            JoinRelType.RIGHT, SideInputSide.LEFT_IS_JDBC));
  }

  @Test
  void joinTypeGuardFullNeverSafe() {
    // FULL preserves unmatched rows on both sides; filtering either side drops rows that should
    // survive. Rejected regardless of orientation.
    assertFalse(
        SideInputInListRule.isJoinTypeCompatibleWithInListPushdown(
            JoinRelType.FULL, SideInputSide.RIGHT_IS_JDBC));
    assertFalse(
        SideInputInListRule.isJoinTypeCompatibleWithInListPushdown(
            JoinRelType.FULL, SideInputSide.LEFT_IS_JDBC));
  }

  // -------------------- detectJdbcSide --------------------

  @Test
  void detectJdbcSideReturnsNullWhenNeitherChildIsJdbc() {
    // When both children are plain (non-JDBC) rels, detectJdbcSide must return null so the rule
    // no-ops cleanly rather than inventing a side. End-to-end coverage for the JDBC-present case
    // lives in ClickHouseFederationIT (a JDBC mock that passes Calcite's type-inference invariants
    // would dominate this file without adding signal).
    RelNode join =
        builder
            .scan("os")
            .scan("ch")
            .join(
                JoinRelType.INNER,
                builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0)))
            .build();
    assertNull(SideInputInListRule.detectJdbcSide((Join) join));
  }

  @Test
  void ruleOperandMatchesGenericJoinNotASpecificPhysicalSubtype() {
    // Pin the operand class to Join.class so the rule fires on EnumerableHashJoin,
    // EnumerableMergeJoin, and EnumerableNestedLoopJoin alike. If a future change narrows the
    // operand to a specific physical subtype (e.g. EnumerableHashJoin.class), plans picked by
    // Volcano as merge/NL joins will silently bypass the IN-list pushdown — this test catches
    // that regression at unit-test time without needing a cost-driven IT to produce a merge/NL
    // plan on demand.
    RelOptRule rule = SideInputInListRule.INSTANCE;
    RelOptRuleOperand operand = rule.getOperand();
    assertEquals(Join.class, operand.getMatchedClass());
  }

  // -------------------- walkForStaticFetch --------------------

  @Test
  void walkForStaticFetchReturnsLiteralFetchFromSortLimit() {
    // `sort(0).limit(0, 100)` produces a Sort(fetch=100). walkForStaticFetch must surface 100.
    RelNode plan = builder.scan("os").sort(0).limit(0, 100).build();
    assertEquals(100L, SideInputInListRule.walkForStaticFetch(plan));
  }

  @Test
  void walkForStaticFetchReturnsMinusOneOnBareScan() {
    // A plain scan has no limit — the helper must return -1L rather than guessing.
    RelNode plan = builder.scan("os").build();
    assertEquals(-1L, SideInputInListRule.walkForStaticFetch(plan));
  }

  @Test
  void walkForStaticFetchReturnsMinusOneOnBranchingJoin() {
    // A Join has 2 inputs; the walker stops at any node with inputs.size() != 1 and returns -1L
    // rather than descending into an arbitrary branch.
    RelNode plan =
        builder
            .scan("os")
            .scan("ch")
            .join(JoinRelType.INNER, builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0)))
            .build();
    assertEquals(-1L, SideInputInListRule.walkForStaticFetch(plan));
  }

  // -------------------- mapRightKeyToScan --------------------

  @Test
  void mapRightKeyToScanFindsColumnByName() {
    // Build a right-top row type whose field at index 1 is named "user_id" (as would appear after
    // an aggregate projecting (s, user_id)). The scan-side "user_id" is at index 0. The helper
    // must translate the right-top index (1) to the scan index (0) by matching names.
    RelNode rightTop =
        builder
            .scan("ch")
            .aggregate(builder.groupKey("user_id"), builder.sum(builder.field("v")).as("s"))
            // Reorder so user_id is second, matching a `stats sum(v) as s by user_id` shape.
            .project(builder.field("s"), builder.field("user_id"))
            .build();
    JdbcTableScan scan = mock(JdbcTableScan.class);
    when(scan.getRowType()).thenReturn(chRowType());
    // rightTopIdx=1 is "user_id" at the right-top output; it lives at index 0 in the scan.
    assertEquals(0, SideInputInListRule.mapRightKeyToScan(rightTop, 1, scan));
  }

  @Test
  void mapRightKeyToScanReturnsMinusOneWhenNameNotInScan() {
    // A right-top projection whose key name is absent from the scan row type should return -1 so
    // the caller can bail rather than wrapping the wrong column.
    RelNode rightTop =
        builder.scan("ch").project(builder.alias(builder.field("user_id"), "alias_id")).build();
    JdbcTableScan scan = mock(JdbcTableScan.class);
    when(scan.getRowType()).thenReturn(chRowType());
    assertEquals(-1, SideInputInListRule.mapRightKeyToScan(rightTop, 0, scan));
  }

  @Test
  void mapRightKeyToScanReturnsFirstMatchOnNameCollision() {
    // Approach: Calcite's RelDataTypeFactory.Builder.add() does not deduplicate field names
    // (uniquify() is a separate, opt-in step), so we stage a real collision by adding "user_id"
    // twice. The helper must return the *first* match (index 0) — this documents the contract
    // of its linear-scan name lookup.
    RelNode rightTop = builder.scan("ch").project(builder.field("user_id")).build();
    RelDataTypeFactory tf = builder.getTypeFactory();
    RelDataType collisionRowType =
        tf.builder()
            .add("user_id", SqlTypeName.BIGINT)
            .add("user_id", SqlTypeName.BIGINT)
            .build();
    JdbcTableScan scan = mock(JdbcTableScan.class);
    when(scan.getRowType()).thenReturn(collisionRowType);
    int idx = SideInputInListRule.mapRightKeyToScan(rightTop, 0, scan);
    // With two fields both literally named "user_id", the first-match contract requires index 0.
    assertEquals(0, idx);
  }

  // -------------------- determineBoundedSize --------------------

  @Test
  void determineBoundedSizeUsesMetadataWhenBoundedInputHasLimit() {
    // Build `scan(os) | limit 50` JOIN `scan(ch)` so the os subtree has a statically-provable
    // getMaxRowCount() == 50 via Calcite's RelMdMaxRowCount. With no bounded_left hint present,
    // determineBoundedSize must fall through to the metadata path and return 50.
    RelNode join =
        builder
            .scan("os")
            .limit(0, 50)
            .scan("ch")
            .join(
                JoinRelType.INNER,
                builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0)))
            .build();
    // Pass the bounded (os) input explicitly — the rule uses SideInputSide to pick which input
    // is bounded, and the helper must respect that choice rather than hardcoding join.getLeft.
    long bound =
        SideInputInListRule.determineBoundedSize((Join) join, ((Join) join).getLeft());
    assertEquals(50L, bound);
  }

  @Test
  void determineBoundedSizeRespectsRightAsBoundedInput() {
    // Mirror of the above, but with the limit on the right subtree — simulates Calcite's
    // JoinCommuteRule having placed the bounded input on the right. Passing join.getRight()
    // (not getLeft()) as the bounded argument must return the correct bound.
    RelNode join =
        builder
            .scan("os")
            .scan("ch")
            .limit(0, 75)
            .join(
                JoinRelType.INNER,
                builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0)))
            .build();
    long bound =
        SideInputInListRule.determineBoundedSize((Join) join, ((Join) join).getRight());
    assertEquals(75L, bound);
  }

  // Note: a previous "determineBoundedSizeReturnsMinusOneWhenLeftIsUnbounded" test was dropped
  // because it asserted a tautology (`bound == -1 || bound <= 10_000`) that Calcite's default
  // row-count estimates trivially satisfy. The layering (hint > maxRowCount > rowCount) is
  // already covered by determineBoundedSizeUsesMetadataWhenLeftHasLimit and by the end-to-end
  // ClickHouseFederationIT; tightening this unit test would require significant stat-provider
  // scaffolding for little additional signal.

  // -------------------- helpers --------------------

  private RelDataType chRowType() {
    RelDataTypeFactory tf = builder.getTypeFactory();
    return tf.builder()
        .add("user_id", SqlTypeName.BIGINT)
        .add("v", SqlTypeName.DOUBLE)
        .build();
  }
}
