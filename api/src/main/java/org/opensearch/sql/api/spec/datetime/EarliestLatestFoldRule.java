/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Locale;
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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.opensearch.sql.utils.DateTimeUtils;

/**
 * Rewrites scalar {@code earliest(literalExpr, ts)} / {@code latest(literalExpr, ts)} UDF calls
 * into engine-agnostic timestamp comparisons.
 *
 * <p>Three emission shapes, picked by the relative-time DSL form:
 *
 * <ul>
 *   <li><b>Absolute literal</b> ({@code '2024-01-15 12:00:00'}, {@code '2024-01-15'}) →
 *       {@code ts >= TIMESTAMP_LITERAL}. The instant is resolved on the JVM side.</li>
 *   <li><b>Pure offset</b> ({@code '-7d'}, {@code '+30m'}, {@code 'now'}, multi-chunk
 *       like {@code '-1mon-2d'}) → {@code ts >= now() + INTERVAL_DAY_TO_SECOND}. The
 *       offset's total millisecond delta becomes a single substrait
 *       {@code interval_day_to_second} literal; {@code now()} stays symbolic so the
 *       backend's optimizer (e.g. DataFusion's {@code SimplifyExpressions}) folds it
 *       at the engine's own plan time, keeping all migrated datetime functions
 *       coherent on a single "now".</li>
 *   <li><b>Snap or mixed</b> ({@code '@d'}, {@code '@w0'}, {@code '-1h@d'}) →
 *       {@code ts >= TIMESTAMP_LITERAL}. Snap operators have no clean substrait
 *       primitive (no portable {@code date_trunc} contract here), and mixed forms
 *       compose offsets with snap, so we resolve the whole expression on the JVM
 *       side. The deviation from engine-side {@code now()} is acceptable because the
 *       user has already requested a wall-clock-aligned boundary, and the alignment
 *       is what the result depends on, not the exact sub-second moment of "now".</li>
 * </ul>
 *
 * <p>The first argument must be a string literal — PPL's {@code STRING_TIMESTAMP}
 * operand checker enforces this upstream of this rule. A non-literal first arg or
 * parse failure preserves the original UDF call as a runtime fallback.
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
 * predicate is the standard SQL shape {@code timestamp >= timestamp_literal} (or
 * {@code timestamp >= now() + interval}, which the backend folds), which every
 * backend can evaluate as a native columnar comparison without per-engine wiring.
 * The relative-time DSL parser stays in one place
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
 * call), so they can run in either order without affecting the result.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class EarliestLatestFoldRule extends RelHomogeneousShuttle {

  static final EarliestLatestFoldRule INSTANCE = new EarliestLatestFoldRule();

  private static final String EARLIEST_NAME = "EARLIEST";
  private static final String LATEST_NAME = "LATEST";

  /** Calcite TIMESTAMP precision matching OpenSearch {@code date} mapping (millis). */
  private static final int TIMESTAMP_PRECISION_MILLIS = 3;

  /**
   * Niladic {@code now()} operator. Backends that consume the rewritten plan resolve
   * the call by name (sandbox's {@code BackendPlanAdapter} maps it to its own
   * {@code LOCAL_NOW_OP} via the {@code ScalarFunction.NOW} enum lookup, which then
   * goes through the substrait {@code "now"} function reference).
   */
  private static final SqlOperator NOW_OP =
      new SqlFunction(
          "now",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.TIMESTAMP,
          null,
          OperandTypes.NILADIC,
          SqlFunctionCategory.TIMEDATE);

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
        return recursed;
      }

      RexNode exprArg = recursed.getOperands().get(0);
      if (!(exprArg instanceof RexLiteral literal)) {
        return recursed;
      }
      String expression = literal.getValueAs(String.class);
      if (expression == null) {
        return recursed;
      }

      RexNode tsArg = recursed.getOperands().get(1);
      RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
      RelDataType plainTimestamp =
          typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.TIMESTAMP, TIMESTAMP_PRECISION_MILLIS),
              tsArg.getType().isNullable());

      // Pre-classify the DSL: if it has any snap chunk we resolve on the JVM; if it's
      // pure-offset (or absolute, or `now`), we can produce a `now() ± INTERVAL` shape.
      DslShape shape;
      try {
        shape = classify(expression);
      } catch (RuntimeException e) {
        // Parse failure — fall through to the UDF so the runtime path produces the
        // same error message the user would have seen previously.
        return recursed;
      }

      RexNode rhs;
      if (shape instanceof DslShape.AbsoluteOrSnap absolute) {
        // JVM-side resolution. The DSL parser handles both absolute literals and
        // snap/mixed forms uniformly via getRelativeZonedDateTime.
        rhs =
            rexBuilder.makeTimestampLiteral(
                TimestampString.fromMillisSinceEpoch(absolute.epochMillis),
                TIMESTAMP_PRECISION_MILLIS);
      } else if (shape instanceof DslShape.PureOffset offset) {
        // Symbolic now() ± INTERVAL_DAY_TO_SECOND. The backend folds at its own
        // plan time, keeping all migrated datetime functions coherent on the same now.
        RexNode now = rexBuilder.makeCall(NOW_OP);
        if (offset.totalMillis == 0L) {
          rhs = now;
        } else {
          SqlIntervalQualifier dayQual = new SqlIntervalQualifier("DAY", SqlParserPos.ZERO);
          RexNode intervalLit =
              rexBuilder.makeIntervalLiteral(BigDecimal.valueOf(offset.totalMillis), dayQual);
          rhs = rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS, now, intervalLit);
        }
      } else {
        // Unreachable — classify only returns the two cases above.
        return recursed;
      }

      // Align the column side with a plain TIMESTAMP type so EXPR_TIMESTAMP UDTs are
      // unwrapped before the comparison.
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
          rhs);
    }
  }

  // ── DSL classifier ──────────────────────────────────────────────────────────

  /** Result of classifying a relative-time DSL string. */
  private sealed interface DslShape {
    /**
     * Either an absolute timestamp literal ({@code '2024-01-15'}) or a relative
     * expression that contains at least one snap chunk ({@code '@d'},
     * {@code '-1h@d'}). Both are resolved on the JVM side as a single epoch-millis
     * literal.
     */
    record AbsoluteOrSnap(long epochMillis) implements DslShape {}

    /**
     * A pure-offset expression ({@code '-7d'}, {@code 'now'}, multi-chunk like
     * {@code '-1mon-2d'}). Total offset in milliseconds relative to "now".
     */
    record PureOffset(long totalMillis) implements DslShape {}
  }

  /**
   * Decides whether the expression resolves on the JVM side or can be deferred to
   * the backend via a {@code now() ± INTERVAL} symbolic shape.
   *
   * <p>Strategy: parse the DSL twice with two different "now" base values that are a
   * known constant millisecond offset apart. If the result delta equals the base
   * delta, the expression contains no snap or absolute components — it's a pure
   * offset (linear in "now"). Otherwise some part of the expression introduced a
   * fixed wall-clock alignment, so we resolve it absolutely.
   */
  private static DslShape classify(String input) {
    if (containsSnap(input)) {
      return jvmResolved(input);
    }
    String lower = input.toLowerCase(Locale.ROOT);
    if (lower.contains("t") || lower.matches(".*\\d{4}.*")) {
      // Absolute timestamp literals always contain a year (4 consecutive digits) or
      // an ISO 'T' separator. Pure relative-time DSL only uses unit letters
      // (s/m/h/d/w/M/y/q/mon and aliases) plus +/-/digits, none of which match.
      return jvmResolved(input);
    }
    // Pure offset (or 'now') — compute the total millisecond delta against an
    // arbitrary anchor.
    long anchorMillis = 1_700_000_000_000L; // fixed anchor for delta extraction
    ZonedDateTime anchor = ZonedDateTime.ofInstant(
        java.time.Instant.ofEpochMilli(anchorMillis), ZoneOffset.UTC);
    ZonedDateTime resolved = DateTimeUtils.getRelativeZonedDateTime(input, anchor);
    long offsetMillis = resolved.toInstant().toEpochMilli() - anchorMillis;
    return new DslShape.PureOffset(offsetMillis);
  }

  /** True iff the input contains a snap chunk (an unquoted '@' character). */
  private static boolean containsSnap(String input) {
    return input.indexOf('@') >= 0;
  }

  private static DslShape jvmResolved(String input) {
    ZonedDateTime base =
        ZonedDateTime.ofInstant(Clock.systemUTC().instant(), ZoneOffset.UTC);
    ZonedDateTime resolved = DateTimeUtils.getRelativeZonedDateTime(input, base);
    return new DslShape.AbsoluteOrSnap(resolved.toInstant().toEpochMilli());
  }
}
