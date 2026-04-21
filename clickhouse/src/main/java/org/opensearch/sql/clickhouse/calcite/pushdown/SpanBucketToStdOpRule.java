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
