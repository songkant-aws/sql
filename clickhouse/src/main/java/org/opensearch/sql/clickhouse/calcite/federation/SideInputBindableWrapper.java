/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.Prepare.PreparedResult;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.opensearch.sql.calcite.planner.logical.rules.BoundedJoinHintRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a {@link PreparedResult} so that, before Calcite binds the generated enumerable, we drain
 * the left side of a bounded {@link EnumerableHashJoin} and bind the distinct keys as an array
 * parameter on the right-side JDBC subtree. The right-side {@link JdbcSideInputFilter} emits
 * {@code WHERE key IN (?)} using this runtime-bound array.
 *
 * <p>This wrapper is installed via {@link
 * org.opensearch.sql.calcite.utils.SideInputBindableHookRegistry}. It is only activated when the
 * root plan contains a join whose right subtree already carries a {@link JdbcSideInputFilter} —
 * the filter is the sole, unambiguous signal that {@link SideInputInListRule} has fired, which
 * avoids the otherwise-brittle reliance on {@code bounded_left} hints that Volcano's {@code
 * EnumerableJoinRule.convert} strips during conversion.
 *
 * <p><b>Runtime binding contract:</b> the drained distinct keys are bound into the generated code
 * under the dynamic-param key {@code "?0"} (the index reserved by {@link SideInputInListRule}
 * when it constructs the {@link org.apache.calcite.rex.RexDynamicParam} inside {@link
 * JdbcSideInputFilter}). The wrapper exposes this via a {@link DataContext} delegate that returns
 * the {@code Object[]} of keys for {@code "?0"} and forwards every other lookup to the caller's
 * original {@code DataContext}.
 */
public final class SideInputBindableWrapper implements PreparedResult {

  private static final Logger LOG = LoggerFactory.getLogger(SideInputBindableWrapper.class);

  /**
   * Dynamic-parameter key used by generated code for {@code RexDynamicParam(index=0)}. Calcite's
   * {@code RexToLixTranslator.visitDynamicParam} synthesizes {@code DataContext.get("?" + index)}
   * calls, and {@link SideInputInListRule} pins the index to 0 for the IN-list array.
   */
  static final String IN_LIST_PARAM_KEY = "?0";

  private final PreparedResult delegate;
  private final RelRoot root;

  private SideInputBindableWrapper(PreparedResult delegate, RelRoot root) {
    this.delegate = delegate;
    this.root = root;
  }

  /**
   * Hook entrypoint for {@link org.opensearch.sql.calcite.utils.SideInputBindableHookRegistry}.
   * Wraps {@code prepared} only if the plan contains a join whose right subtree carries a {@link
   * JdbcSideInputFilter} (i.e. {@link SideInputInListRule} has already rewritten it). Otherwise
   * returns {@code prepared} as-is so unrelated queries take a no-op fast path.
   */
  public static PreparedResult maybeWrap(PreparedResult prepared, RelRoot root) {
    Join bounded = findBoundedJoin(root.rel);
    if (bounded == null) {
      return prepared;
    }
    return new SideInputBindableWrapper(prepared, root);
  }

  @Override
  public Bindable getBindable(Meta.CursorFactory cursorFactory) {
    Bindable<Object> inner = delegate.getBindable(cursorFactory);
    return (DataContext ctx) -> bindWithSideInput(inner, ctx);
  }

  private Enumerable<Object> bindWithSideInput(Bindable<Object> inner, DataContext ctx) {
    Join join = findBoundedJoin(root.rel);
    if (join == null) {
      // Plan shape changed between prepare and bind — degrade to the inner bindable.
      return inner.bind(ctx);
    }
    JdbcSideInputFilter filter = findSideInputFilter(join.getRight());
    if (filter == null) {
      // No side-input filter on the right after all — fall back to the generated code unmodified.
      return inner.bind(ctx);
    }

    // Determine the left-key column for distinct-key extraction. The filter's key column is the
    // *right* key; the corresponding *left* key is what we drain out of the left enumerable, so
    // we recover it from the join condition.
    int leftKeyIdx = extractLeftKeyIndex(join);
    if (leftKeyIdx < 0) {
      LOG.warn(
          "SideInputBindableWrapper: could not extract left-side key index from join condition;"
              + " falling back to un-pushed execution. condition={}",
          join.getCondition());
      return inner.bind(ctx);
    }

    // The dialect-specific JDBC array type tag (e.g. "Int64" for ClickHouse) is needed by
    // java.sql.Connection.createArrayOf. We resolve it from the right-side JDBC connection's
    // column metadata via the scan's row-type — Calcite's PplFederationDialect registry guards
    // that supportsArrayInListParam() is true before the rule fires, so DataSource must exist.
    DataSource rightDs = resolveRightDataSource(join);
    if (rightDs == null) {
      LOG.warn(
          "SideInputBindableWrapper: right-side DataSource unresolved; falling back to un-pushed"
              + " execution.");
      return inner.bind(ctx);
    }

    long maxSize = hintSize(join);
    if (maxSize < 0) {
      // Fallback drain ceiling: hints are typically lost during EnumerableJoinRule.convert, so
      // we use the same absolute ceiling as BoundedJoinHintRule. Matches SideInputInListRule's
      // METADATA_BOUND_CEILING.
      maxSize = 10_000L;
    }

    Enumerable<Object[]> leftEnum = execLeft(join.getLeft(), ctx);
    SideInputDrainEnumerable.Result drained =
        SideInputDrainEnumerable.drain(leftEnum, leftKeyIdx, maxSize);

    // Prefer binding the distinct keys as a real java.sql.Array so Calcite's
    // ResultSetEnumerable.setDynamicParam routes through PreparedStatement.setArray — the
    // driver-supported shape proven end-to-end by ClickHouseArrayBindSpikeIT. If we can't
    // get a Connection (or createArrayOf throws), fall back to the raw Object[] path, which
    // clickhouse-jdbc 0.6.5 accepts via setObject as a client-side-interpolated array literal.
    Object paramValue = wrapAsSqlArrayIfPossible(rightDs, drained.distinctKeys());
    DataContext bound = bindInListParam(ctx, paramValue);
    return inner.bind(bound);
  }

  /**
   * Attempts to wrap {@code distinctKeys} as a {@link java.sql.Array} using a {@link Connection}
   * obtained from {@code ds}. On any failure (no DataSource, {@link SQLException} from
   * {@link Connection#createArrayOf}, unsupported feature, etc.), returns the raw {@code Object[]}
   * unchanged — this preserves the working {@code setObject(Object[])} path as a safety net.
   *
   * <p>The SQL type name passed to {@code createArrayOf} is chosen from the runtime Java class of
   * the first non-null element. This is a heuristic for the feature's first cut: the exhaustive
   * mapping (Calcite {@link RelDataType} → dialect-specific SQL type name) is handled
   * dialect-side via {@link PplFederationDialect} in future tasks.
   */
  private static Object wrapAsSqlArrayIfPossible(DataSource ds, Object[] distinctKeys) {
    if (ds == null || distinctKeys == null || distinctKeys.length == 0) {
      return distinctKeys;
    }
    String sqlTypeName = inferSqlTypeName(distinctKeys);
    if (sqlTypeName == null) {
      return distinctKeys;
    }
    // Use a short-lived connection solely to construct the java.sql.Array. The array holds only
    // its values once created; closing the connection here does not invalidate the array for
    // subsequent use inside ResultSetEnumerable (which opens its own connection for the prepared
    // statement). This matches the pattern in ClickHouseArrayBindSpikeIT.
    try (Connection conn = ds.getConnection()) {
      return conn.createArrayOf(sqlTypeName, distinctKeys);
    } catch (SQLException | RuntimeException e) {
      LOG.debug(
          "SideInputBindableWrapper: createArrayOf({}) failed; falling back to Object[] bind",
          sqlTypeName,
          e);
      return distinctKeys;
    }
  }

  /**
   * Heuristic mapping from a runtime key's Java class to a ClickHouse SQL type name suitable for
   * {@link Connection#createArrayOf}. Returns {@code null} if no element has a supported type, in
   * which case the caller falls back to the {@code Object[]} path.
   */
  private static String inferSqlTypeName(Object[] distinctKeys) {
    for (Object v : distinctKeys) {
      if (v == null) {
        continue;
      }
      if (v instanceof Long) {
        return "Int64";
      }
      if (v instanceof Integer) {
        return "Int32";
      }
      if (v instanceof Short) {
        return "Int16";
      }
      if (v instanceof Byte) {
        return "Int8";
      }
      if (v instanceof Double) {
        return "Float64";
      }
      if (v instanceof Float) {
        return "Float32";
      }
      if (v instanceof String) {
        return "String";
      }
      return null;
    }
    return null;
  }

  /**
   * Executes the left side of the bounded join against the given {@link DataContext} and returns
   * an enumerable over its rows.
   *
   * <p>The left subtree comes out of the outer {@link
   * org.apache.calcite.plan.volcano.VolcanoPlanner} already converted to an {@link EnumerableRel}
   * (confirmed by the {@code ENUMERABLE} convention appearing on the rel's trait set), so there
   * is no need to re-plan it. We can't go through {@link
   * org.apache.calcite.tools.RelRunner#prepareStatement} either: {@code RelRunner} ultimately
   * routes into {@code CalcitePrepareImpl.prepareRel} &rarr; {@code Prepare.optimize}, which
   * retrieves the planner via {@code rel.getCluster().getPlanner()} and re-registers the tree on
   * that same {@link org.apache.calcite.plan.volcano.VolcanoPlanner}. Since every rel in the left
   * subtree is already registered with the outer planner, that second registration trips the
   * "already been registered: rel#N" assertion.
   *
   * <p>Instead, we compile the already-physical left rel directly with {@link
   * EnumerableInterpretable#toBindable} and bind it against the caller's {@link DataContext}.
   * This path uses only {@code cluster.getRexBuilder()} &mdash; it never touches the planner's
   * rel-registry &mdash; so it sidesteps the double-registration problem entirely.
   *
   * <p>Before compiling we {@linkplain #flattenSubsets flatten} the subtree so every node is a
   * concrete {@link EnumerableRel}: {@link RelSubset} and {@link HepRelVertex} are resolved to
   * their best/concrete inner rel and removed from the tree. {@code implementRoot} traverses
   * children by casting to {@code EnumerableRel}, and a raw {@code RelSubset} reaching it would
   * fail that cast.
   */
  Enumerable<Object[]> execLeft(RelNode left, DataContext ctx) {
    RelNode flattened = flattenSubsets(left);
    if (!(flattened instanceof EnumerableRel)) {
      throw new IllegalStateException(
          "SideInputBindableWrapper.execLeft: flattened left root is not an EnumerableRel (got "
              + flattened.getClass().getName()
              + "); side-input pushdown requires the left subtree to be physical after outer"
              + " planning. plan=\n"
              + org.apache.calcite.plan.RelOptUtil.toString(flattened));
    }
    try {
      // NB: must be a *mutable* map — EnumerableRelImplementor.stash() calls .put() on it to
      // register physical-scan singletons (e.g. CalciteEnumerableIndexScan stashes `this` so the
      // generated code can invoke scan() on the concrete node). A Collections.emptyMap() here
      // trips UnsupportedOperationException in AbstractMap.put, suppressed behind an otherwise-
      // opaque "Unable to implement ..." IllegalStateException from implementRoot.
      //
      // The populated map is "internal parameters": at runtime, the generated code looks up these
      // keys via DataContext.get(key). Calcite's public pipeline (CalciteConnectionImpl.enumerable)
      // merges Signature.internalParameters into a fresh DataContext before invoking bind — but
      // since we're driving the compile path ourselves, we must do that merge manually by
      // decorating `ctx` so get(name) falls back to the stash map. Without this merge the generated
      // code retrieves null from the DataContext and NPEs at "scan()" (the first stashed call).
      Map<String, Object> stash = new HashMap<>();
      Bindable<?> bindable =
          EnumerableInterpretable.toBindable(
              stash, null, (EnumerableRel) flattened, EnumerableRel.Prefer.ARRAY);
      DataContext enriched = withStash(ctx, stash);
      // Calcite's EnumerableInterpretable.box is package-private, so we inline its behaviour:
      // when the generated bindable already emits Object[] rows (the common case for multi-
      // field row types with Prefer.ARRAY), use it directly; otherwise, wrap each scalar row
      // in a singleton Object[]. Our left subtree always has a multi-column row type (project
      // out at least the join key + any other surfaced columns), so the fast path is taken.
      if (bindable instanceof ArrayBindable) {
        return ((ArrayBindable) bindable).bind(enriched);
      }
      @SuppressWarnings("unchecked")
      Bindable<Object> scalarBindable = (Bindable<Object>) bindable;
      Enumerable<Object> scalar = scalarBindable.bind(enriched);
      return scalar.select(v -> new Object[] {v});
    } catch (RuntimeException e) {
      throw new RuntimeException(
          "SideInputBindableWrapper.execLeft: failed to compile/run the left subtree via"
              + " EnumerableInterpretable; plan=\n"
              + org.apache.calcite.plan.RelOptUtil.toString(flattened),
          e);
    }
  }

  /**
   * Resolves every {@link RelSubset} and {@link HepRelVertex} in the subtree to its concrete
   * inner rel, returning a tree with no planner wrappers at any depth. Required before handing
   * the tree to {@link EnumerableInterpretable#toBindable}, which delegates to each rel's {@code
   * implement(..)} method &mdash; those implementations unconditionally cast child inputs to
   * {@link EnumerableRel}, and a surviving {@code RelSubset} would fail that cast.
   *
   * <p>The resulting tree still shares a {@link org.apache.calcite.plan.RelOptCluster} with the
   * outer plan; that is harmless because {@code toBindable} reads only {@code
   * cluster.getRexBuilder()} and never touches the planner's rel-registry, so no
   * "already been registered" collision arises.
   */
  private static RelNode flattenSubsets(RelNode root) {
    RelNode cur = unwrap(root);
    List<RelNode> inputs = cur.getInputs();
    if (inputs.isEmpty()) {
      return cur;
    }
    List<RelNode> newInputs = new ArrayList<>(inputs.size());
    boolean changed = false;
    for (RelNode in : inputs) {
      RelNode flat = flattenSubsets(in);
      if (flat != in) {
        changed = true;
      }
      newInputs.add(flat);
    }
    if (!changed) {
      return cur;
    }
    return cur.copy(cur.getTraitSet(), newInputs);
  }

  /**
   * Returns a {@link DataContext} that resolves keys from {@code stash} first (so the generated
   * code emitted by {@link EnumerableInterpretable#toBindable} can reach the {@code
   * internalParameters} map collected during compilation), and forwards every other lookup to
   * {@code outer}. This is the manual equivalent of what {@link
   * org.apache.calcite.jdbc.CalciteConnectionImpl#enumerable} does when it merges {@code
   * Signature.internalParameters} into a fresh {@link DataContext} before invoking {@code bind}.
   */
  private static DataContext withStash(DataContext outer, Map<String, Object> stash) {
    if (stash.isEmpty()) {
      return outer;
    }
    return new DataContext() {
      @Override
      public SchemaPlus getRootSchema() {
        return outer.getRootSchema();
      }

      @Override
      public JavaTypeFactory getTypeFactory() {
        return outer.getTypeFactory();
      }

      @Override
      public QueryProvider getQueryProvider() {
        return outer.getQueryProvider();
      }

      @Override
      public Object get(String name) {
        Object v = stash.get(name);
        if (v != null) {
          return v;
        }
        return outer.get(name);
      }
    };
  }

  /**
   * Returns a {@link DataContext} that resolves {@link #IN_LIST_PARAM_KEY} to the drained array
   * and forwards every other lookup to {@code outer}. The returned context is a thin decorator
   * and does not copy state.
   *
   * <p>{@code paramValue} may be either a {@link java.sql.Array} (preferred — Calcite's
   * {@code ResultSetEnumerable.setDynamicParam} routes it through
   * {@link java.sql.PreparedStatement#setArray}) or a raw {@code Object[]} (fallback — routed
   * through {@code setObject}). Both shapes are understood by clickhouse-jdbc 0.6.5.
   */
  private DataContext bindInListParam(DataContext outer, Object paramValue) {
    return new DataContext() {
      @Override
      public SchemaPlus getRootSchema() {
        return outer.getRootSchema();
      }

      @Override
      public JavaTypeFactory getTypeFactory() {
        return outer.getTypeFactory();
      }

      @Override
      public QueryProvider getQueryProvider() {
        return outer.getQueryProvider();
      }

      @Override
      public Object get(String name) {
        if (IN_LIST_PARAM_KEY.equals(name)) {
          return paramValue;
        }
        return outer.get(name);
      }
    };
  }

  /**
   * Recovers the left-side key column index for the bounded join. Mirrors the filter extraction
   * in {@link SideInputInListRule#extractRightKeyIndex} but for the *left* input.
   */
  private static int extractLeftKeyIndex(Join join) {
    if (!(join.getCondition() instanceof org.apache.calcite.rex.RexCall)) {
      return -1;
    }
    org.apache.calcite.rex.RexCall cond = (org.apache.calcite.rex.RexCall) join.getCondition();
    if (cond.getKind() != org.apache.calcite.sql.SqlKind.EQUALS
        || cond.getOperands().size() != 2) {
      return -1;
    }
    org.apache.calcite.rex.RexNode l = cond.getOperands().get(0);
    org.apache.calcite.rex.RexNode r = cond.getOperands().get(1);
    if (!(l instanceof org.apache.calcite.rex.RexInputRef)
        || !(r instanceof org.apache.calcite.rex.RexInputRef)) {
      return -1;
    }
    int leftFieldCount = join.getLeft().getRowType().getFieldCount();
    int li = ((org.apache.calcite.rex.RexInputRef) l).getIndex();
    int ri = ((org.apache.calcite.rex.RexInputRef) r).getIndex();
    if (li < leftFieldCount && ri >= leftFieldCount) {
      return li;
    }
    if (ri < leftFieldCount && li >= leftFieldCount) {
      return ri;
    }
    return -1;
  }

  /** Walks the right subtree for the first {@link JdbcSideInputFilter}, or {@code null}. */
  private static JdbcSideInputFilter findSideInputFilter(RelNode root) {
    RelNode stripped = unwrap(root);
    if (stripped instanceof JdbcSideInputFilter) {
      return (JdbcSideInputFilter) stripped;
    }
    for (RelNode child : stripped.getInputs()) {
      JdbcSideInputFilter found = findSideInputFilter(child);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /**
   * Resolves the {@link DataSource} for the JDBC right-side of {@code join}. Tries the
   * public-field path first ({@code scan.jdbcTable.jdbcSchema.getDataSource()}); falls back to
   * {@link org.apache.calcite.schema.Table#unwrap(Class)} if the public fields are somehow not
   * reachable (e.g. planner-visibility shenanigans in future Calcite upgrades).
   */
  static DataSource resolveRightDataSource(Join join) {
    RelNode stripped = unwrap(join.getRight());
    if (!(stripped instanceof JdbcToEnumerableConverter)) {
      return null;
    }
    JdbcTableScan scan = findDeepestJdbcTableScan(stripped);
    if (scan == null) {
      return null;
    }
    try {
      return scan.jdbcTable.jdbcSchema.getDataSource();
    } catch (Throwable t) {
      JdbcSchema schema = scan.getTable().unwrap(JdbcSchema.class);
      return schema != null ? schema.getDataSource() : null;
    }
  }

  /** Returns the dialect of the right-side JDBC subtree via its {@link JdbcConvention}. */
  static JdbcConvention resolveRightJdbcConvention(Join join) {
    RelNode stripped = unwrap(join.getRight());
    if (!(stripped instanceof JdbcToEnumerableConverter)) {
      return null;
    }
    JdbcTableScan scan = findDeepestJdbcTableScan(stripped);
    if (scan == null) {
      return null;
    }
    try {
      return (JdbcConvention) scan.getConvention();
    } catch (ClassCastException e) {
      return null;
    }
  }

  /**
   * Locates the first {@link Join} under {@code root} whose right subtree contains a {@link
   * JdbcSideInputFilter}. Detecting the target by the filter's presence (rather than by the
   * {@code bounded_left} hint) is necessary because Volcano's {@code EnumerableJoinRule.convert}
   * strips hints during the {@code LogicalJoin} -> {@code EnumerableHashJoin} conversion. The
   * filter, by contrast, is unambiguous and survives planning because it's the very artifact
   * {@link SideInputInListRule} inserts.
   */
  static Join findBoundedJoin(RelNode root) {
    RelNode cur = unwrap(root);
    if (cur instanceof Join) {
      Join j = (Join) cur;
      if (findSideInputFilter(j.getRight()) != null) {
        return j;
      }
    }
    for (RelNode child : cur.getInputs()) {
      Join found = findBoundedJoin(child);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /** Reads the {@code size} kv-option from the {@code bounded_left} hint, or {@code -1}. */
  static long hintSize(Join join) {
    for (RelHint h : join.getHints()) {
      if (BoundedJoinHintRule.HINT_NAME.equals(h.hintName)) {
        String sizeStr = h.kvOptions.get(BoundedJoinHintRule.HINT_SIZE_KEY);
        if (sizeStr == null) {
          return -1L;
        }
        try {
          return Long.parseLong(sizeStr);
        } catch (NumberFormatException e) {
          return -1L;
        }
      }
    }
    return -1L;
  }

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

  private static JdbcTableScan findDeepestJdbcTableScan(RelNode root) {
    RelNode cur = unwrap(root);
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

  // ---------- PreparedResult pass-throughs ----------

  @Override
  public String getCode() {
    return delegate.getCode();
  }

  @Override
  public boolean isDml() {
    return delegate.isDml();
  }

  @Override
  public TableModify.Operation getTableModOp() {
    return delegate.getTableModOp();
  }

  @Override
  public List<? extends List<String>> getFieldOrigins() {
    return delegate.getFieldOrigins();
  }

  @Override
  public RelDataType getParameterRowType() {
    return delegate.getParameterRowType();
  }
}
