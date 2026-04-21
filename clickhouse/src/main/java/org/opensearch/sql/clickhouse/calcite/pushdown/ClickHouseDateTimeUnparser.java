/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Single source of truth for rewriting PPL datetime UDF calls into ClickHouse-native SQL at
 * unparse time. Keyed by uppercase operator name. {@link
 * org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect#supportsFunction} consults this
 * map; {@link org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect#unparseCall} delegates
 * to the matching handler.
 *
 * <p>Keeping the whitelist and the rewrite in the same Map prevents the error mode "approved
 * the function but forgot to add an unparse handler".
 */
public final class ClickHouseDateTimeUnparser {

  @FunctionalInterface
  interface UnparseHandler {
    void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec);
  }

  private static final Map<String, UnparseHandler> HANDLERS = new LinkedHashMap<>();

  static {
    HANDLERS.put("DATE_FORMAT", rename("formatDateTime"));
    HANDLERS.put("NOW", rename("now"));
    HANDLERS.put("UNIX_TIMESTAMP", rename("toUnixTimestamp"));
    HANDLERS.put("FROM_UNIXTIME", rename("fromUnixTimestamp"));
    HANDLERS.put("YEAR", rename("toYear"));
    HANDLERS.put("MONTH", rename("toMonth"));
    HANDLERS.put("DAY", rename("toDayOfMonth"));
    HANDLERS.put("DAYOFMONTH", rename("toDayOfMonth"));
    HANDLERS.put("HOUR", rename("toHour"));
    HANDLERS.put("MINUTE", rename("toMinute"));
    HANDLERS.put("SECOND", rename("toSecond"));
    HANDLERS.put("DATE_ADD", intervalArithmetic(/* subtract */ false));
    HANDLERS.put("DATE_SUB", intervalArithmetic(/* subtract */ true));
  }

  private ClickHouseDateTimeUnparser() {}

  public static boolean hasHandler(String upperName) {
    return HANDLERS.containsKey(upperName);
  }

  public static int handlerCount() {
    return HANDLERS.size();
  }

  public static void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    String key = call.getOperator().getName().toUpperCase(Locale.ROOT);
    UnparseHandler h = HANDLERS.get(key);
    if (h == null) {
      call.getOperator().unparse(writer, call, leftPrec, rightPrec);
      return;
    }
    h.unparse(writer, call, leftPrec, rightPrec);
  }

  /** Test-only: minimal SqlOperator for unit harness. Never registered in real planning. */
  public static SqlOperator operatorFor(String upperName) {
    return new SqlFunction(
        upperName,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  /**
   * Build a handler that renames the op to {@code chFunctionName} and forwards every operand
   * unchanged. Arity-agnostic: works for zero-arg (e.g. {@code now()}), unary
   * (e.g. {@code toYear(ts)}), and n-ary signatures.
   */
  private static UnparseHandler rename(String chFunctionName) {
    return (writer, call, l, r) -> emitFunCall(writer, chFunctionName, call.getOperandList());
  }

  /**
   * Emit a ClickHouse function call with mixed-case name preserved.
   *
   * <p>Uses {@link SqlWriter#print} + {@link SqlWriter#startList} rather than
   * {@code keyword()/startFunCall()} so {@code SqlPrettyWriter}'s keyword case policy (default
   * uppercase) does not mangle ClickHouse function names like {@code formatDateTime},
   * {@code toYear}, {@code addDays}.
   */
  private static void emitFunCall(
      SqlWriter writer, String chFunctionName, Iterable<? extends SqlNode> args) {
    writer.print(chFunctionName);
    writer.setNeedWhitespace(false);
    SqlWriter.Frame frame = writer.startList("(", ")");
    boolean first = true;
    for (SqlNode arg : args) {
      if (!first) {
        writer.sep(",");
      }
      first = false;
      arg.unparse(writer, 0, 0);
    }
    writer.endList(frame);
  }

  /**
   * Build a handler for {@code DATE_ADD}/{@code DATE_SUB} that dispatches on the interval unit
   * of operand 1 to the correct ClickHouse {@code add*}/{@code subtract*} function
   * (e.g. {@code addDays}, {@code subtractHours}).
   *
   * <p>ClickHouse's unit-specific datetime arithmetic functions take a plain numeric magnitude,
   * not a SQL {@code INTERVAL}. Example: {@code addDays(ts, 3)}, not
   * {@code addDays(ts, INTERVAL 3 DAY)}.
   *
   * <p>If operand 1 is not a {@link SqlIntervalLiteral} or its {@link TimeUnit} is not mapped,
   * the handler falls back to the operator's default unparse so downstream JDBC code-gen can
   * reject the call and Volcano re-costs to Enumerable.
   */
  private static UnparseHandler intervalArithmetic(boolean subtract) {
    return (writer, call, leftPrec, rightPrec) -> {
      if (call.operandCount() == 2 && call.operand(1) instanceof SqlIntervalLiteral) {
        SqlIntervalLiteral lit = (SqlIntervalLiteral) call.operand(1);
        Object raw = lit.getValue();
        if (raw instanceof SqlIntervalLiteral.IntervalValue) {
          SqlIntervalLiteral.IntervalValue iv = (SqlIntervalLiteral.IntervalValue) raw;
          SqlIntervalQualifier qualifier = iv.getIntervalQualifier();
          TimeUnit unit = qualifier.getStartUnit();
          String chFn = chArithmeticFn(unit, subtract);
          if (chFn != null) {
            // Emit: chFn(operand0, <signed magnitude>) — bare numeric, no INTERVAL keyword.
            String magnitude = iv.getIntervalLiteral();
            String signed = iv.getSign() < 0 ? "-" + magnitude : magnitude;
            writer.print(chFn);
            writer.setNeedWhitespace(false);
            SqlWriter.Frame frame = writer.startList("(", ")");
            call.operand(0).unparse(writer, 0, 0);
            writer.sep(",");
            writer.print(signed);
            writer.endList(frame);
            return;
          }
        }
      }
      // Unknown unit or non-interval operand: delegate to default unparse so JDBC code-gen
      // aborts and Volcano re-costs this subtree to Enumerable.
      call.getOperator().unparse(writer, call, leftPrec, rightPrec);
    };
  }

  /**
   * Map an ANSI {@link TimeUnit} to the corresponding ClickHouse add/subtract function name,
   * or {@code null} if the unit is unsupported by our mapping (caller should fall back).
   */
  private static String chArithmeticFn(TimeUnit unit, boolean subtract) {
    String prefix = subtract ? "subtract" : "add";
    if (unit == null) {
      return null;
    }
    switch (unit) {
      case YEAR:
        return prefix + "Years";
      case MONTH:
        return prefix + "Months";
      case WEEK:
        return prefix + "Weeks";
      case DAY:
        return prefix + "Days";
      case HOUR:
        return prefix + "Hours";
      case MINUTE:
        return prefix + "Minutes";
      case SECOND:
        return prefix + "Seconds";
      default:
        return null;
    }
  }
}
