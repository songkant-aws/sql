/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import java.util.List;
import javax.sql.DataSource;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverter;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.Prepare.PreparedResult;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Bindable;
import org.opensearch.sql.calcite.planner.logical.rules.BoundedJoinHintRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a {@link PreparedResult} so that, before Calcite binds the generated enumerable, we drain
 * the left side of a {@code bounded_left}-hinted {@link EnumerableHashJoin} and bind the distinct
 * keys as an array parameter on the right-side JDBC subtree. The right-side {@link
 * JdbcSideInputFilter} emits {@code WHERE key IN (?)} using this runtime-bound array.
 *
 * <p>This wrapper is installed via {@link
 * org.opensearch.sql.calcite.utils.SideInputBindableHookRegistry}. It is only activated when the
 * root plan contains a hinted {@link EnumerableHashJoin} whose right side descends into a {@link
 * JdbcToEnumerableConverter}; otherwise it returns the original prepared result untouched.
 *
 * <p><b>Task 12 scope:</b> the {@link #execLeft(RelNode, DataContext)} entrypoint is deliberately
 * a stub that throws {@link UnsupportedOperationException}. The end-to-end IT in Task 13 is the
 * forcing function for the real implementation; before then, testing the stub would only exercise
 * the throw path.
 */
public final class SideInputBindableWrapper implements PreparedResult {

  private static final Logger LOG = LoggerFactory.getLogger(SideInputBindableWrapper.class);

  private final PreparedResult delegate;
  private final RelRoot root;

  private SideInputBindableWrapper(PreparedResult delegate, RelRoot root) {
    this.delegate = delegate;
    this.root = root;
  }

  /**
   * Hook entrypoint for {@link org.opensearch.sql.calcite.utils.SideInputBindableHookRegistry}.
   * Wraps {@code prepared} only if the plan contains a {@code bounded_left} hinted join whose
   * right side is a {@link JdbcToEnumerableConverter}. Otherwise returns {@code prepared} as-is.
   */
  public static PreparedResult maybeWrap(PreparedResult prepared, RelRoot root) {
    if (findBoundedJoin(root.rel) == null) {
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
    // Task 13 will implement: drain the left via execLeft, bind the distinct keys onto the
    // DataContext under the dynamic-param index reserved by SideInputInListRule (index 0),
    // then delegate to inner.bind(ctx).
    execLeft(join.getLeft(), ctx);
    return inner.bind(ctx);
  }

  /**
   * Executes the left side of the bounded join against the given {@link DataContext} and returns
   * an enumerable over its rows. Task 13 will implement this by compiling the left {@link
   * RelNode} as a standalone bindable and invoking it against {@code ctx}; drain + distinct-key
   * extraction is handled by {@link SideInputDrainEnumerable}, and the right-side JDBC
   * {@link DataSource} is resolved via {@link #resolveRightDataSource(Join)}.
   */
  Enumerable<Object[]> execLeft(RelNode left, DataContext ctx) {
    throw new UnsupportedOperationException(
        "SideInputBindableWrapper.execLeft: v1 placeholder — Task 13 IT drives the final"
            + " implementation.");
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

  /** Locates the first {@code bounded_left}-hinted {@link Join} under {@code root}. */
  static Join findBoundedJoin(RelNode root) {
    if (root instanceof Join) {
      Join j = (Join) root;
      for (RelHint h : j.getHints()) {
        if (BoundedJoinHintRule.HINT_NAME.equals(h.hintName)) {
          return j;
        }
      }
    }
    for (RelNode child : root.getInputs()) {
      Join found = findBoundedJoin(unwrap(child));
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
