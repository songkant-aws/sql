/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlDialect;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.planner.logical.rules.BoundedJoinHintRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Volcano-phase rule matching any {@link Join} with a bounded left side and a right input that is a
 * {@link JdbcToEnumerableConverter}. Rewrites the right JDBC subtree to insert a {@link
 * JdbcSideInputFilter} above the base {@link JdbcTableScan}, enabling runtime {@code WHERE key IN
 * (?)} binding from the drained left side of the join.
 *
 * <p>The operand is {@link Join} (rather than a specific physical subtype) so the rule fires on
 * {@code EnumerableHashJoin}, {@code EnumerableMergeJoin}, and {@code EnumerableNestedLoopJoin}
 * alike — all three use the left input as the driving/build side, so draining the left to populate
 * the right-side IN-list is semantically sound for each.
 *
 * <p><b>Join-type guard.</b> Filtering the right to {@code key IN (<left keys>)} is semantically
 * correct only for join types where right-side rows with no left match are already excluded from
 * the result:
 *
 * <ul>
 *   <li>{@link JoinRelType#INNER} / {@link JoinRelType#LEFT}: only rows with a right match
 *       (possibly empty for LEFT) appear; filtering right to {@code IN (left keys)} cannot drop a
 *       row that would otherwise contribute.
 *   <li>{@link JoinRelType#SEMI} / {@link JoinRelType#ANTI}: right rows only act as existence
 *       probes; filtering them to {@code IN (left keys)} preserves the per-left-key EXISTS answer
 *       because right rows whose key is not in the left set never match any left row.
 *   <li>{@link JoinRelType#RIGHT} / {@link JoinRelType#FULL}: unmatched right rows must survive in
 *       the output. Filtering right to {@code IN (left keys)} would drop exactly those unmatched
 *       rows, producing wrong results. The rule rejects these.
 * </ul>
 *
 * <p>No-ops if:
 *
 * <ul>
 *   <li>the join type is {@link JoinRelType#RIGHT} or {@link JoinRelType#FULL} (see above),
 *   <li>the join is not statically-provably bounded on its left side (neither a {@code
 *       bounded_left} hint nor {@link RelMetadataQuery#getMaxRowCount} yields an upper bound
 *       &le; {@link #METADATA_BOUND_CEILING}),
 *   <li>the right side is not a {@link JdbcToEnumerableConverter} (after stripping {@link
 *       HepRelVertex}/{@link RelSubset} wrappers),
 *   <li>the right-side JDBC dialect does not advertise {@link
 *       PplFederationDialect#supportsArrayInListParam()},
 *   <li>the proven upper bound exceeds the right-side dialect's {@link
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

  /**
   * Absolute ceiling used by the metadata fallback for the left-side row-count upper bound.
   * Kept in sync with {@link BoundedJoinHintRule}'s attach-time ceiling (which is
   * package-private) so the metadata fallback matches the hint-based path's behaviour. If
   * {@link BoundedJoinHintRule} ever bumps its ceiling, this literal must follow suit.
   */
  private static final long METADATA_BOUND_CEILING = 10_000L;

  public static final SideInputInListRule INSTANCE = Config.DEFAULT.toRule();

  private SideInputInListRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);

    // 0. join-type guard: only fire on join types where pruning right-side rows whose key is not
    //    in the left-key set preserves the result. See class javadoc for the full derivation. In
    //    particular RIGHT/FULL outer joins surface right rows with no left match, and filtering
    //    those out would silently drop correct rows.
    if (!isJoinTypeCompatibleWithInListPushdown(join.getJoinType())) {
      return;
    }

    // 1. boundedness: prefer the bounded_left hint (set in HEP by BoundedJoinHintRule), but fall
    //    back to structural row-count metadata. Calcite's EnumerableJoinRule.convert path does not
    //    propagate hints onto the physical join node, so by the time this Volcano rule fires the
    //    hint is typically gone. The metadata fallback recovers structural proofs like a
    //    Sort(fetch) on the left subtree (exactly what `| head N` produces) so we still fire on
    //    bounded-left joins even when the hint has been stripped.
    long boundedSize = determineBoundedSize(join);
    if (boundedSize < 0) {
      return;
    }

    // 3. right input must be JdbcToEnumerableConverter (strip planner wrappers)
    RelNode strippedRight = unwrap(join.getRight());
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
    if (boundedSize > caps.getInListPushdownThreshold()) {
      return;
    }

    // 7. compute right key index from a single binary equality condition
    Integer rightKeyIdx = extractRightKeyIndex(join);
    if (rightKeyIdx == null) {
      return;
    }

    // 7a. translate the key index from the right-subtree root row type down to the scan row type.
    //     `rightKeyIdx` is an index into `strippedRight.getRowType()` (the converter's output),
    //     but we wrap the scan at the bottom of the subtree. Intervening JdbcAggregate and
    //     JdbcProject reorder/rename columns, so the same index can select a different column
    //     against the scan (e.g. an aggregate output (s, user_id) vs. a scan (user_id, v) —
    //     index 1 selects `user_id` at the aggregate and `v` at the scan). Map by column name.
    int scanKeyIdx = mapRightKeyToScan(strippedRight, rightKeyIdx, scan);
    if (scanKeyIdx < 0) {
      LOG.debug(
          "SideInputInListRule: right-top key name not found in scan row type; skipping");
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
    RelDataType keyType = scan.getRowType().getFieldList().get(scanKeyIdx).getType();
    RelDataType arrayType = typeFactory.createArrayType(keyType, -1);
    // Index 0 reserved for bounded-left IN-list binding; Task 12's runtime binder relies on this.
    // Don't reuse index 0 for any other federation rule without updating the binder contract.
    RexDynamicParam arrayParam = rexBuilder.makeDynamicParam(arrayType, 0);

    // 10. wrap the scan with a JdbcSideInputFilter and rebuild the parent chain
    JdbcSideInputFilter filter =
        JdbcSideInputFilter.create(scan, scanKeyIdx, arrayParam, jdbcConvention);
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

  /**
   * Returns {@code true} if filtering the right input to {@code key IN (<left keys>)} preserves
   * join semantics for {@code type}. See class javadoc for the per-type derivation. Kept as a
   * separate helper (rather than inlined in {@link #onMatch}) so unit tests can pin the
   * per-join-type decision without exercising the full rule machinery.
   */
  @VisibleForTesting
  static boolean isJoinTypeCompatibleWithInListPushdown(JoinRelType type) {
    switch (type) {
      case INNER:
      case LEFT:
      case SEMI:
      case ANTI:
        return true;
      case RIGHT:
      case FULL:
      default:
        return false;
    }
  }

  private static RelHint findBoundedLeftHint(Join join) {
    for (RelHint h : join.getHints()) {
      if (BoundedJoinHintRule.HINT_NAME.equals(h.hintName)) {
        return h;
      }
    }
    return null;
  }

  /**
   * Returns a statically-provable upper bound for the left-side row count, or {@code -1} if none
   * can be determined. Prefers the {@code bounded_left} hint when present; otherwise consults
   * {@link RelMetadataQuery#getMaxRowCount(RelNode)}. The metadata path handles the common case
   * where Volcano's {@code EnumerableJoinRule.convert} constructs a fresh physical join without
   * copying hints — in that case Calcite's native {@code RelMdMaxRowCount} still returns the
   * {@code Sort.fetch} value (which is what {@code | head N} produces upstream).
   */
  @VisibleForTesting
  static long determineBoundedSize(Join join) {
    RelHint hint = findBoundedLeftHint(join);
    if (hint != null) {
      String sizeStr = hint.kvOptions.get(BoundedJoinHintRule.HINT_SIZE_KEY);
      if (sizeStr != null) {
        try {
          return Long.parseLong(sizeStr);
        } catch (NumberFormatException ignored) {
          // fall through to metadata path
        }
      }
    }
    try {
      RelMetadataQuery mq = join.getCluster().getMetadataQuery();
      Double max = mq.getMaxRowCount(join.getLeft());
      if (max != null && !max.isInfinite() && !max.isNaN() && max <= METADATA_BOUND_CEILING) {
        return (long) Math.ceil(max);
      }
    } catch (Throwable t) {
      // metadata providers can throw CyclicMetadataException etc. — treat as "unknown"
      LOG.debug("SideInputInListRule: metadata getMaxRowCount threw; treating as unbounded", t);
    }
    // Structural fallback: walk the left subtree for a LIMIT-ish node (Sort with a literal fetch,
    // Calcite's EnumerableLimit, LogicalSort, etc.). Storage adapters that push LIMIT into the
    // scan (e.g. OpenSearch's CalciteEnumerableIndexScan exposes its LIMIT only through a private
    // PushDownContext and its `estimateRowCount` override, not through getMaxRowCount) defeat the
    // metadata path; in that case we fall through to the per-rel estimate below.
    long structural = walkForStaticFetch(unwrap(join.getLeft()));
    if (structural >= 0 && structural <= METADATA_BOUND_CEILING) {
      return structural;
    }
    // Last resort: trust estimateRowCount when its value is small. Storage adapters that cap
    // their estimate at a literal LIMIT (OpenSearch's CalciteEnumerableIndexScan) give a
    // provable bound here; we deliberately only accept values <= the ceiling to avoid
    // trusting Calcite's default 1e9-ish estimate.
    try {
      RelMetadataQuery mq = join.getCluster().getMetadataQuery();
      Double est = mq.getRowCount(join.getLeft());
      if (est != null && !est.isInfinite() && !est.isNaN() && est <= METADATA_BOUND_CEILING) {
        return (long) Math.ceil(est);
      }
    } catch (Throwable t) {
      LOG.debug("SideInputInListRule: metadata getRowCount threw; treating as unbounded", t);
    }
    return -1L;
  }

  /**
   * Walks the single-input chain (through planner wrappers) for the first node exposing a literal
   * fetch bound, returning its value. Matches {@link org.apache.calcite.rel.core.Sort}'s fetch
   * slot since {@code EnumerableLimit} extends {@code Sort}. Stops on the first branching/fan-out
   * node and returns {@code -1L} rather than guessing. Public for use by the runtime wrapper.
   */
  @VisibleForTesting
  static long walkForStaticFetch(RelNode root) {
    RelNode cur = unwrap(root);
    while (cur != null) {
      if (cur instanceof org.apache.calcite.rel.core.Sort) {
        org.apache.calcite.rel.core.Sort sort = (org.apache.calcite.rel.core.Sort) cur;
        if (sort.fetch instanceof org.apache.calcite.rex.RexLiteral) {
          java.math.BigDecimal n =
              (java.math.BigDecimal)
                  ((org.apache.calcite.rex.RexLiteral) sort.fetch).getValue4();
          if (n != null) {
            return n.longValueExact();
          }
        }
      }
      List<RelNode> inputs = cur.getInputs();
      if (inputs.size() != 1) {
        return -1L;
      }
      cur = unwrap(inputs.get(0));
    }
    return -1L;
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

  /**
   * The join-key index we compute from the join condition is an index into the right-subtree
   * root's row type. But we wrap the scan (at the bottom of the subtree), whose row type
   * differs (JdbcAggregate, JdbcProject renumber columns). Translate by name: use the right-top
   * name and look it up in the scan row type. Returns {@code -1} if the name doesn't exist in
   * the scan.
   *
   * <p>Rationale: in the canonical PPL federation case, the right side is
   * {@code source=<table> | stats <agg> by <key>}. The {@code <key>} column appears at both the
   * scan and at the aggregate output with the same name, so name-based mapping is unambiguous.
   * Exotic aliasing at the top of the right subtree (where the key column name differs from the
   * scan's) is left as a no-op; the standard join path then handles the query.
   */
  @VisibleForTesting
  static int mapRightKeyToScan(RelNode rightTop, int rightTopIdx, JdbcTableScan scan) {
    String name = rightTop.getRowType().getFieldList().get(rightTopIdx).getName();
    List<RelDataTypeField> scanFields = scan.getRowType().getFieldList();
    for (int i = 0; i < scanFields.size(); i++) {
      if (scanFields.get(i).getName().equals(name)) {
        return i;
      }
    }
    return -1;
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
            // Operand is the generic Join base class (not a specific physical subtype) so the
            // rule fires on EnumerableHashJoin, EnumerableMergeJoin, and EnumerableNestedLoopJoin.
            // Semantic filtering by join type happens inside onMatch via
            // isJoinTypeCompatibleWithInListPushdown; operand-level restriction would incorrectly
            // exclude merge/NL plans where the rewrite is safe.
            .withOperandSupplier(b -> b.operand(Join.class).anyInputs())
            .withDescription("SideInputInListRule")
            .as(Config.class);

    @Override
    default SideInputInListRule toRule() {
      return new SideInputInListRule(this);
    }
  }
}
