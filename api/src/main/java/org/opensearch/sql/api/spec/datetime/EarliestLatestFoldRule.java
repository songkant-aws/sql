/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.opensearch.sql.utils.DateTimeUtils;

/**
 * Rewrites scalar {@code earliest(literalExpr, ts)} / {@code latest(literalExpr, ts)} UDF calls
 * into engine-agnostic timestamp comparisons:
 *
 * <pre>
 *   earliest('-7d', ts)           →  ts >= TIMESTAMP_LITERAL
 *   latest('2024-01-15', ts)      →  ts <= TIMESTAMP_LITERAL
 * </pre>
 *
 * <p>The first argument must be a string literal — either the relative-time DSL
 * ({@code -7d}, {@code @d}, {@code -1h@d}, …) or an absolute timestamp
 * ({@code 2024-01-15 12:00:00}). PPL's {@code STRING_TIMESTAMP} operand checker
 * already enforces the literal-string shape upstream of this rule, so a non-literal
 * first arg is the only fall-through path; in that case the original UDF call is left
 * unchanged for runtime evaluation.
 *
 * <h2>Why fold here</h2>
 *
 * <p>The SQL plugin registers EARLIEST/LATEST as Calcite UDFs whose runtime
 * implementation parses the literal once per row, evaluates the relative-time DSL
 * against {@code FunctionProperties.getQueryStartClock()}, and compares two
 * {@link ZonedDateTime} values. That works on the Lucene-served path where the
 * timestamp column flows through the {@code EXPR_TIMESTAMP} UDT (whose Java carrier
 * is a string), but it requires every downstream backend to either re-implement the
 * UDF or accept a per-row UDF dispatch.
 *
 * <p>Folding to a comparison at plan time eliminates that requirement. The rewritten
 * predicate is the standard SQL shape {@code timestamp >= timestamp_literal}, which
 * every backend (DataFusion, the legacy V2 enumerable path, future engines) can
 * evaluate as a native columnar comparison without any per-engine adapter wiring.
 * It also lets the relative-time DSL parser stay in one place
 * ({@link DateTimeUtils#getRelativeZonedDateTime}) instead of being ported to each
 * runtime.
 *
 * <h2>Type alignment with the UDT layer</h2>
 *
 * <p>If the column reference is still wrapped in an {@code EXPR_TIMESTAMP} UDT
 * (because the source schema went through {@code OpenSearchTypeFactory} rather than
 * a plain Calcite-typed schema), we wrap it in a {@code CAST AS TIMESTAMP} so the
 * comparison's two sides agree on a non-UDT timestamp type. The cast is the
 * plan-time analogue of what {@code ExprValue.timestampValue()} did at runtime
 * inside the UDF — it normalises the UDT's variable Java carrier (string or long)
 * into the typed timestamp the comparison expects.
 *
 * <p>This rule and {@link DatetimeUdtNormalizeRule} operate on disjoint RexCall
 * subsets (datetime-UDT-typed return values vs. the BOOLEAN-typed EARLIEST/LATEST
 * call), so they can run in either order without affecting the result. The column
 * reference on the second operand is a {@code RexInputRef} whose type comes from
 * the source RowType and is therefore unaffected by either rule.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class EarliestLatestFoldRule extends RelHomogeneousShuttle {

  static final EarliestLatestFoldRule INSTANCE = new EarliestLatestFoldRule();

  private static final String EARLIEST_NAME = "EARLIEST";
  private static final String LATEST_NAME = "LATEST";

  /** Calcite TIMESTAMP precision matching OpenSearch {@code date} mapping (millis). */
  private static final int TIMESTAMP_PRECISION_MILLIS = 3;

  @Override
  public RelNode visit(RelNode other) {
    RelNode visited = super.visit(other);
    return visited.accept(new FoldShuttle(visited.getCluster().getRexBuilder()));
  }

  private static final class FoldShuttle extends RexShuttle {
    private final RexBuilder rexBuilder;

    FoldShuttle(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      RexCall recursed = (RexCall) super.visitCall(call);

      String opName = recursed.getOperator().getName();
      boolean isEarliest = EARLIEST_NAME.equalsIgnoreCase(opName);
      boolean isLatest = LATEST_NAME.equalsIgnoreCase(opName);
      if (!isEarliest && !isLatest) {
        return recursed;
      }
      if (recursed.getOperands().size() != 2) {
        // Aggregate forms route through MIN/MAX in CalciteRelNodeVisitor; this branch
        // would only fire for a malformed scalar shape we don't recognise. Leave the
        // call unchanged so existing error reporting stays intact.
        return recursed;
      }

      RexNode exprArg = recursed.getOperands().get(0);
      if (!(exprArg instanceof RexLiteral literal)) {
        // Non-literal first arg — preserve the original UDF call as a runtime fallback.
        // The PPL grammar today only accepts a string literal here, so this branch is
        // defensive coverage rather than a supported user shape.
        return recursed;
      }
      String expression = literal.getValueAs(String.class);
      if (expression == null) {
        return recursed;
      }

      ZonedDateTime resolved;
      try {
        ZonedDateTime base =
            ZonedDateTime.ofInstant(Clock.systemUTC().instant(), ZoneOffset.UTC);
        resolved = DateTimeUtils.getRelativeZonedDateTime(expression, base);
      } catch (RuntimeException e) {
        // Parse failure — fall through to the UDF so the runtime path produces the
        // same error message the user would have seen previously.
        return recursed;
      }

      RexNode tsArg = recursed.getOperands().get(1);
      RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
      RelDataType plainTimestamp =
          typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.TIMESTAMP, TIMESTAMP_PRECISION_MILLIS),
              tsArg.getType().isNullable());

      RexNode tsLiteral =
          rexBuilder.makeTimestampLiteral(
              TimestampString.fromMillisSinceEpoch(resolved.toInstant().toEpochMilli()),
              TIMESTAMP_PRECISION_MILLIS);

      // Align the column side with the literal's plain TIMESTAMP type. If the column
      // is already plain TIMESTAMP, makeCast is a structural no-op; if it's the
      // EXPR_TIMESTAMP UDT (VARCHAR-carrier) the cast normalises it into a real
      // timestamp so the comparison is well-typed downstream.
      RexNode tsAligned;
      if (tsArg.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP) {
        tsAligned = tsArg;
      } else {
        tsAligned = rexBuilder.makeCast(plainTimestamp, tsArg);
      }

      return rexBuilder.makeCall(
          isEarliest
              ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL
              : SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
          tsAligned,
          tsLiteral);
    }
  }
}
