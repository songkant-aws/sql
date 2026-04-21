/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlDialect;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Volcano-phase rule matching {@link EnumerableHashJoin} hinted {@code bounded_left} whose right
 * input is a {@link JdbcToEnumerableConverter}. Rewrites the right JDBC subtree to insert a {@link
 * JdbcSideInputFilter} above the base {@link JdbcTableScan}, enabling runtime {@code WHERE key IN
 * (?)} binding from the drained left side of the join.
 *
 * <p>No-ops if:
 *
 * <ul>
 *   <li>the join has no {@code bounded_left} hint,
 *   <li>the right side is not a {@link JdbcToEnumerableConverter} (after stripping {@link
 *       HepRelVertex}/{@link RelSubset} wrappers),
 *   <li>the right-side JDBC dialect does not advertise {@link
 *       PplFederationDialect#supportsArrayInListParam()},
 *   <li>the hinted {@code size} exceeds the dialect's {@link
 *       PplFederationDialect#getInListPushdownThreshold()},
 *   <li>the join condition isn't a single binary equality on two {@link RexInputRef}s,
 *   <li>a {@link JdbcSideInputFilter} already exists anywhere on the right JDBC subtree
 *       (idempotence guard).
 * </ul>
 *
 * <p>This rule does NOT participate in runtime drain/bind — that is the responsibility of Tasks
 * 11/12.
 */
@Value.Enclosing
public final class SideInputInListRule extends RelRule<SideInputInListRule.Config> {

  private static final Logger LOG = LoggerFactory.getLogger(SideInputInListRule.class);

  public static final SideInputInListRule INSTANCE = Config.DEFAULT.toRule();

  private SideInputInListRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);

    // 1. hint presence
    RelHint boundedHint = findBoundedLeftHint(join);
    if (boundedHint == null) {
      return;
    }

    // 2. parse size from hint kvOptions
    long hintSize;
    try {
      String sizeStr = boundedHint.kvOptions.get("size");
      if (sizeStr == null) {
        return;
      }
      hintSize = Long.parseLong(sizeStr);
    } catch (NumberFormatException e) {
      return;
    }

    // 3. right input must be JdbcToEnumerableConverter (strip planner wrappers)
    RelNode rawRight = join.getRight();
    RelNode strippedRight = unwrap(rawRight);
    if (!(strippedRight instanceof JdbcToEnumerableConverter)) {
      return;
    }
    JdbcToEnumerableConverter converter = (JdbcToEnumerableConverter) strippedRight;

    // 4. idempotence: skip if subtree already contains a JdbcSideInputFilter
    if (containsSideInputFilter(converter)) {
      return;
    }

    // 5. find deepest JdbcTableScan on the right JDBC subtree
    JdbcTableScan scan = findDeepestJdbcTableScan(converter);
    if (scan == null) {
      return;
    }

    // 6. resolve dialect capabilities via JdbcConvention (its public dialect field)
    JdbcConvention jdbcConvention;
    try {
      jdbcConvention = (JdbcConvention) scan.getConvention();
    } catch (ClassCastException e) {
      return;
    }
    if (jdbcConvention == null) {
      return;
    }
    SqlDialect dialect = jdbcConvention.dialect;
    PplFederationDialect caps = PplFederationDialectRegistry.forDialect(dialect);
    if (!caps.supportsArrayInListParam()) {
      return;
    }
    if (hintSize > caps.getInListPushdownThreshold()) {
      return;
    }

    // 7. compute right key index from a single binary equality condition
    Integer rightKeyIdx = extractRightKeyIndex(join);
    if (rightKeyIdx == null) {
      return;
    }

    // 8. warn if no Aggregate is present under the converter (fan-out risk)
    if (!hasAggregateInSubtree(converter)) {
      LOG.warn(
          "SideInputInListRule: no Aggregate found below JdbcToEnumerableConverter; IN-list"
              + " pushdown may cause row fan-out at runtime. Proceeding anyway.");
    }

    // 9. build an array-typed RexDynamicParam (index 0) of the right key's column type
    RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    RelDataType keyType = scan.getRowType().getFieldList().get(rightKeyIdx).getType();
    RelDataType arrayType = typeFactory.createArrayType(keyType, -1);
    RexDynamicParam arrayParam = rexBuilder.makeDynamicParam(arrayType, 0);

    // 10. wrap the scan with a JdbcSideInputFilter and rebuild the parent chain
    JdbcSideInputFilter filter =
        JdbcSideInputFilter.create(scan, rightKeyIdx, arrayParam, jdbcConvention);
    RelNode newJdbcTree = rebuildWithNewScan(converter.getInput(), scan, filter);
    if (newJdbcTree == null) {
      // couldn't rebuild — bail
      return;
    }

    // 11. rebuild the JdbcToEnumerableConverter over the new subtree
    RelNode newConverter = converter.copy(converter.getTraitSet(), List.of(newJdbcTree));

    // 12. rebuild the Join with the new right child
    RelNode newJoin =
        join.copy(
            join.getTraitSet(),
            join.getCondition(),
            join.getLeft(),
            newConverter,
            join.getJoinType(),
            join.isSemiJoinDone());

    call.transformTo(newJoin);
  }

  private static RelHint findBoundedLeftHint(Join join) {
    for (RelHint h : join.getHints()) {
      if ("bounded_left".equals(h.hintName)) {
        return h;
      }
    }
    return null;
  }

  /** Strips HepRelVertex / RelSubset planner-internal wrappers. */
  private static RelNode unwrap(RelNode rel) {
    if (rel instanceof HepRelVertex) {
      return ((HepRelVertex) rel).stripped();
    }
    if (rel instanceof RelSubset) {
      RelSubset subset = (RelSubset) rel;
      RelNode best = subset.getBest();
      return best != null ? best : subset.getOriginal();
    }
    return rel;
  }

  /**
   * Extracts the right-side key column index when the join condition is a single binary equality
   * on two {@link RexInputRef}s. Returns {@code null} for any other shape.
   */
  private static Integer extractRightKeyIndex(Join join) {
    RexNode cond = join.getCondition();
    if (!(cond instanceof RexCall)) {
      return null;
    }
    RexCall call = (RexCall) cond;
    if (call.getKind() != SqlKind.EQUALS || call.getOperands().size() != 2) {
      return null;
    }
    RexNode l = call.getOperands().get(0);
    RexNode r = call.getOperands().get(1);
    if (!(l instanceof RexInputRef) || !(r instanceof RexInputRef)) {
      return null;
    }
    int leftFieldCount = join.getLeft().getRowType().getFieldCount();
    int li = ((RexInputRef) l).getIndex();
    int ri = ((RexInputRef) r).getIndex();
    // Exactly one of (li, ri) must reference the right side.
    if (li >= leftFieldCount && ri < leftFieldCount) {
      return li - leftFieldCount;
    }
    if (ri >= leftFieldCount && li < leftFieldCount) {
      return ri - leftFieldCount;
    }
    return null;
  }

  /** Returns {@code true} if any node under {@code root} (exclusive) is a {@link Aggregate}. */
  private static boolean hasAggregateInSubtree(RelNode root) {
    for (RelNode child : root.getInputs()) {
      RelNode stripped = unwrap(child);
      if (stripped instanceof Aggregate) {
        return true;
      }
      if (hasAggregateInSubtree(stripped)) {
        return true;
      }
    }
    return false;
  }

  /** Returns {@code true} if any node at or below {@code root} is a {@link JdbcSideInputFilter}. */
  private static boolean containsSideInputFilter(RelNode root) {
    RelNode stripped = unwrap(root);
    if (stripped instanceof JdbcSideInputFilter) {
      return true;
    }
    for (RelNode child : stripped.getInputs()) {
      if (containsSideInputFilter(child)) {
        return true;
      }
    }
    return false;
  }

  /** Walks down via single-input chains to find the deepest {@link JdbcTableScan}. */
  private static JdbcTableScan findDeepestJdbcTableScan(RelNode root) {
    RelNode cur = unwrap(root);
    // Descend through any node whose first input leads to a JdbcTableScan.
    while (cur != null) {
      if (cur instanceof JdbcTableScan) {
        return (JdbcTableScan) cur;
      }
      List<RelNode> inputs = cur.getInputs();
      if (inputs.isEmpty()) {
        return null;
      }
      cur = unwrap(inputs.get(0));
    }
    return null;
  }

  /**
   * Rebuilds {@code subtreeRoot}'s parent chain, replacing {@code oldScan} with {@code newScan}.
   * Returns {@code null} if {@code oldScan} is not reachable from {@code subtreeRoot} via the
   * first-input path.
   */
  private static RelNode rebuildWithNewScan(
      RelNode subtreeRoot, JdbcTableScan oldScan, RelNode newScan) {
    RelNode stripped = unwrap(subtreeRoot);
    if (stripped == oldScan) {
      return newScan;
    }
    List<RelNode> inputs = stripped.getInputs();
    if (inputs.isEmpty()) {
      return null;
    }
    RelNode newFirstInput = rebuildWithNewScan(inputs.get(0), oldScan, newScan);
    if (newFirstInput == null) {
      return null;
    }
    List<RelNode> newInputs = new ArrayList<>(inputs);
    newInputs.set(0, newFirstInput);
    return stripped.copy(stripped.getTraitSet(), newInputs);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        ImmutableSideInputInListRule.Config.builder()
            .build()
            .withOperandSupplier(b -> b.operand(EnumerableHashJoin.class).anyInputs())
            .withDescription("SideInputInListRule")
            .as(Config.class);

    @Override
    default SideInputInListRule toRule() {
      return new SideInputInListRule(this);
    }
  }
}
