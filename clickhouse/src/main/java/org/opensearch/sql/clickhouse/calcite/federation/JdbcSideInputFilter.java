/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.adapter.jdbc.JdbcRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Filter RelNode that emits {@code <keyCol> IN (?)} against a JDBC source, where the sole
 * parameter is an array-typed {@link RexDynamicParam} bound at runtime from the drained left side
 * of a federation join.
 *
 * <p>Implements {@link JdbcRel} and is bound to a {@link JdbcConvention} (typically {@link
 * org.opensearch.sql.clickhouse.calcite.ClickHouseConvention}) so it participates in Calcite's
 * JDBC pushdown path under {@code JdbcToEnumerableConverter}, which only recognizes subtrees
 * whose root is a {@link JdbcRel} in a {@link JdbcConvention}.
 *
 * <p>Calcite 1.41 restricts {@code SqlStdOperatorTable.IN} in {@code RexCall} to {@code
 * RexSubQuery} nodes only (enforced by an assertion in {@code RexCall.<init>}). To work around
 * this, we define a custom {@link #ARRAY_IN_OP} with {@code SqlKind.OTHER} that unparsed as {@code
 * col IN (?)} — identical SQL output, no internal kind restriction.
 *
 * <p>The filter's condition is synthesized from {@code keyColumnIndex} and {@code arrayParam}, so
 * equality/hashCode/copy semantics are inherited from {@link Filter}.
 */
public final class JdbcSideInputFilter extends Filter implements JdbcRel {

  /**
   * Custom binary operator that unparsed as {@code <left> IN (<right>)}. Uses {@code
   * SqlKind.OTHER} to avoid the Calcite 1.41 assertion that restricts {@code SqlKind.IN} calls to
   * {@code RexSubQuery}.
   */
  public static final SqlSpecialOperator ARRAY_IN_OP =
      new SqlSpecialOperator(
          "ARRAY_IN",
          SqlKind.OTHER,
          30,
          /* leftAssoc= */ true,
          ReturnTypes.BOOLEAN_NULLABLE,
          null,
          OperandTypes.ANY_ANY) {

        @Override
        public SqlSyntax getSyntax() {
          return SqlSyntax.SPECIAL;
        }

        /** Emits {@code <operand0> IN (<operand1>)}. */
        @Override
        public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
          call.operand(0).unparse(writer, leftPrec, getLeftPrec());
          writer.keyword("IN");
          final SqlWriter.Frame frame = writer.startList("(", ")");
          call.operand(1).unparse(writer, 0, 0);
          writer.endList(frame);
        }
      };

  private final int keyColumnIndex;
  private final RexDynamicParam arrayParam;

  private JdbcSideInputFilter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      int keyColumnIndex,
      RexDynamicParam arrayParam) {
    super(
        cluster,
        traits,
        input,
        makeCondition(cluster.getRexBuilder(), keyColumnIndex, arrayParam, input));
    assert getConvention() instanceof JdbcConvention
        : "JdbcSideInputFilter must be in a JdbcConvention; got " + getConvention();
    this.keyColumnIndex = keyColumnIndex;
    this.arrayParam = arrayParam;
  }

  /**
   * Factory method. Pins {@code convention} onto the resulting filter's trait set so Calcite's
   * JDBC pushdown path ({@code JdbcToEnumerableConverter}) recognizes the subtree as pushable.
   */
  public static JdbcSideInputFilter create(
      RelNode input,
      int keyColumnIndex,
      RexDynamicParam arrayParam,
      JdbcConvention convention) {
    RelTraitSet traits = input.getTraitSet().replace(convention);
    return new JdbcSideInputFilter(
        input.getCluster(), traits, input, keyColumnIndex, arrayParam);
  }

  private static RexNode makeCondition(
      RexBuilder builder, int keyCol, RexDynamicParam param, RelNode input) {
    RelDataType boolType = builder.getTypeFactory().createSqlType(
        org.apache.calcite.sql.type.SqlTypeName.BOOLEAN);
    RexInputRef keyRef = RexInputRef.of(keyCol, input.getRowType());
    return builder.makeCall(boolType, ARRAY_IN_OP,
        com.google.common.collect.ImmutableList.of(keyRef, param));
  }

  public int getKeyColumnIndex() {
    return keyColumnIndex;
  }

  public RexDynamicParam getArrayParam() {
    return arrayParam;
  }

  /**
   * {@inheritDoc}
   *
   * <p>The {@code ignoredCondition} argument is intentionally ignored: the condition is always
   * rebuilt from {@code keyColumnIndex} and {@code arrayParam} so the IN-list semantics are
   * preserved across planner copies. The incoming {@code traitSet} is preserved as-is so the
   * JDBC convention supplied by the planner (or by {@link #create}) is retained.
   */
  @Override
  public JdbcSideInputFilter copy(
      RelTraitSet traitSet, RelNode input, RexNode ignoredCondition) {
    return new JdbcSideInputFilter(
        input.getCluster(), traitSet, input, keyColumnIndex, arrayParam);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("keyCol", keyColumnIndex)
        .item("param", arrayParam.getIndex());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Mirrors Calcite's built-in {@code JdbcRules.JdbcFilter.implement}: the JDBC implementor's
   * dispatch (via {@link JdbcImplementor#implement(RelNode)}) reuses the base {@code
   * RelToSqlConverter.visit(Filter)} logic, which will unparse our {@link #ARRAY_IN_OP}-based
   * condition as {@code <keyCol> IN (?)} against the child's SQL projection.
   */
  @Override
  public SqlImplementor.Result implement(JdbcImplementor implementor) {
    return implementor.implement(this);
  }

  /**
   * Produce a {@link SqlNode} for this filter in the given {@code dialect}. Used by tests and by
   * JDBC unparse.
   */
  public SqlNode toSqlNode(SqlDialect dialect) {
    RelToSqlConverter conv = new RelToSqlConverter(dialect);
    SqlImplementor.Result result = conv.visitRoot(this);
    return result.asStatement();
  }
}
