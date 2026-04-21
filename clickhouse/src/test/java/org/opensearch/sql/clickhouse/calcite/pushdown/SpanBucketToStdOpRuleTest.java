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
