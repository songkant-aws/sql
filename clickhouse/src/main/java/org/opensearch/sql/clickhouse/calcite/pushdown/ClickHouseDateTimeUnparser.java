/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import java.util.LinkedHashMap;
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
    HANDLERS.put("DATE_ADD", rename("addSeconds"));
    HANDLERS.put("DATE_SUB", rename("subtractSeconds"));
  }

  private ClickHouseDateTimeUnparser() {}

  public static boolean hasHandler(String upperName) {
    return HANDLERS.containsKey(upperName);
  }

  public static int handlerCount() {
    return HANDLERS.size();
  }

  public static void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    String key = call.getOperator().getName().toUpperCase();
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

  private static UnparseHandler rename(String chFunctionName) {
    return (writer, call, l, r) -> {
      // Emit the function name verbatim via print() rather than keyword()/startFunCall() so
      // SqlPrettyWriter's keyword case policy (default uppercase) does not mangle the
      // mixed-case ClickHouse function names (e.g. formatDateTime, toYear, addSeconds).
      writer.print(chFunctionName);
      writer.setNeedWhitespace(false);
      SqlWriter.Frame frame = writer.startList("(", ")");
      boolean first = true;
      for (SqlNode arg : call.getOperandList()) {
        if (!first) {
          writer.sep(",");
        }
        first = false;
        arg.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    };
  }
}
