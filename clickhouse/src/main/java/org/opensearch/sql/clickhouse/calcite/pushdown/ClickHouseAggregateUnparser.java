/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Single source of truth for rewriting PPL aggregate UDF calls into ClickHouse-native SQL at
 * unparse time. Keyed by uppercase operator name. {@link
 * org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect#supportsAggregateFunction} consults
 * this map; {@link org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect#unparseCall}
 * delegates to the matching handler.
 *
 * <p>Same Map-as-single-source-of-truth pattern as {@link ClickHouseDateTimeUnparser}: keeping
 * the whitelist and the rewrite in the same Map prevents the error mode "approved the function
 * but forgot to add an unparse handler".
 */
public final class ClickHouseAggregateUnparser {

  @FunctionalInterface
  interface UnparseHandler {
    void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec);
  }

  private static final Map<String, UnparseHandler> HANDLERS = new LinkedHashMap<>();

  static {
    HANDLERS.put("DISTINCT_COUNT_APPROX", rename("uniq"));
  }

  private ClickHouseAggregateUnparser() {}

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
        ReturnTypes.BIGINT,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  /**
   * Build a handler that renames the op to {@code chFunctionName} and forwards every operand
   * unchanged. Arity-agnostic.
   */
  private static UnparseHandler rename(String chFunctionName) {
    return (writer, call, l, r) -> emitFunCall(writer, chFunctionName, call.getOperandList());
  }

  /**
   * Emit a ClickHouse function call with mixed-case name preserved.
   *
   * <p>Uses {@link SqlWriter#print} + {@link SqlWriter#startList} rather than
   * {@code keyword()/startFunCall()} so {@code SqlPrettyWriter}'s keyword case policy (default
   * uppercase) does not mangle ClickHouse function names like {@code uniq},
   * {@code quantilesExactWeighted}, {@code argMax}.
   *
   * <p>NOTE: do not regress to {@code writer.startFunCall(chFunctionName)} —
   * {@code SqlPrettyWriter.keyword} uppercases CH function names.
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
}
