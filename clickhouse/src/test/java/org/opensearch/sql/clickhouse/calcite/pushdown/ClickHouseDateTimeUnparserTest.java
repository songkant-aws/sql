/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect;

public class ClickHouseDateTimeUnparserTest {

  @Test
  public void registers_all_high_frequency_datetime_ops() {
    // Whitelist size pins the scope — if someone adds or removes a handler,
    // this test changes and they must justify it in the PR.
    assertEquals(13, ClickHouseDateTimeUnparser.handlerCount(),
        "expected 13 datetime handlers (DATE_FORMAT, NOW, UNIX_TIMESTAMP,"
            + " FROM_UNIXTIME, YEAR, MONTH, DAY, DAYOFMONTH, HOUR, MINUTE,"
            + " SECOND, DATE_ADD, DATE_SUB)");
  }

  @Test
  public void unparses_date_format_to_formatDateTime() {
    String sql = unparseCall("DATE_FORMAT", "ts_col", "'%Y-%m-%d'");
    // Lock down: CH-native, case-correct function name AND arguments in order.
    // Regressing to startFunCall() would uppercase the keyword and fail startsWith.
    assertTrue(sql.startsWith("formatDateTime("),
        "expected sql to start with formatDateTime( but got: " + sql);
    assertTrue(sql.contains("ts_col"),
        "expected first arg ts_col but got: " + sql);
    assertTrue(sql.contains("'%Y-%m-%d'"),
        "expected format literal '%Y-%m-%d' but got: " + sql);
    assertFalse(sql.contains("DATE_FORMAT"),
        "must rewrite away DATE_FORMAT but got: " + sql);
  }

  @Test
  public void unparses_year_to_toYear() {
    String sql = unparseCall("YEAR", "ts_col");
    // Pin the mixed-case function name via startsWith — regression to startFunCall
    // would uppercase "toYear" and fail.
    assertTrue(sql.startsWith("toYear("),
        "expected sql to start with toYear( but got: " + sql);
    assertTrue(sql.contains("ts_col"),
        "expected arg ts_col but got: " + sql);
  }

  @Test
  public void unparses_now_to_now() {
    String sql = unparseCall("NOW");
    // Zero-arg CH function: exact shape "now()".
    assertTrue(sql.startsWith("now("),
        "expected sql to start with now( but got: " + sql);
    assertTrue(sql.endsWith(")"),
        "expected sql to end with ) but got: " + sql);
  }

  @Test
  public void unknown_op_returns_null_handler() {
    assertFalse(ClickHouseDateTimeUnparser.hasHandler("NOT_A_DATETIME_FN"));
  }

  @Test
  public void unparse_unregistered_op_delegates_to_default() {
    // Exercise the fallback branch where hasHandler == false: unparse() must
    // delegate to the op's own unparse() rather than emitting nothing or NPE.
    SqlWriter w = new SqlPrettyWriter(ClickHouseSqlDialect.INSTANCE);
    SqlOperator op = SqlStdOperatorTable.CURRENT_DATE;
    SqlBasicCall call = new SqlBasicCall(op, new SqlNode[0], SqlParserPos.ZERO);
    ClickHouseDateTimeUnparser.unparse(w, call, 0, 0);
    String sql = w.toString();
    // Default unparse of CURRENT_DATE is the keyword CURRENT_DATE (no parens).
    assertTrue(sql.toUpperCase().contains("CURRENT_DATE"),
        "expected default unparse to emit CURRENT_DATE but got: " + sql);
  }

  @Test
  public void unparses_date_add_day_to_addDays() {
    String sql = unparseIntervalCall("DATE_ADD", "ts_col", 3, TimeUnit.DAY);
    assertTrue(sql.startsWith("addDays("),
        "expected sql to start with addDays( but got: " + sql);
    assertTrue(sql.contains("ts_col"),
        "expected first arg ts_col but got: " + sql);
    assertTrue(sql.contains("3"),
        "expected numeric interval value 3 but got: " + sql);
    assertFalse(sql.contains("INTERVAL"),
        "must not emit INTERVAL keyword (CH takes bare numeric): " + sql);
    assertFalse(sql.contains("addSeconds"),
        "must dispatch on unit, not hard-code seconds: " + sql);
  }

  @Test
  public void unparses_date_add_month_to_addMonths() {
    String sql = unparseIntervalCall("DATE_ADD", "ts_col", 2, TimeUnit.MONTH);
    assertTrue(sql.startsWith("addMonths("),
        "expected sql to start with addMonths( but got: " + sql);
    assertTrue(sql.contains("2"),
        "expected numeric interval value 2 but got: " + sql);
    assertFalse(sql.contains("addSeconds"),
        "must dispatch on unit, not hard-code seconds: " + sql);
  }

  @Test
  public void unparses_date_sub_hour_to_subtractHours() {
    String sql = unparseIntervalCall("DATE_SUB", "ts_col", 5, TimeUnit.HOUR);
    assertTrue(sql.startsWith("subtractHours("),
        "expected sql to start with subtractHours( but got: " + sql);
    assertTrue(sql.contains("5"),
        "expected numeric interval value 5 but got: " + sql);
    assertFalse(sql.contains("subtractSeconds"),
        "must dispatch on unit, not hard-code seconds: " + sql);
  }

  @Test
  public void unparses_date_add_unknown_unit_falls_back() {
    // MICROSECOND is not in our CH add/subtract mapping. Handler must fall back
    // to default unparse (which for our test-only SqlFunction produces the
    // uppercase op name), so callers/Volcano can re-cost to Enumerable.
    String sql = unparseIntervalCall("DATE_ADD", "ts_col", 7, TimeUnit.MICROSECOND);
    assertFalse(sql.contains("addSeconds"),
        "must not hard-code seconds for unknown unit: " + sql);
    assertFalse(sql.contains("addMicroseconds"),
        "unknown unit must not be mapped to a CH function: " + sql);
    assertTrue(sql.toUpperCase().contains("DATE_ADD"),
        "fallback must emit the original op name DATE_ADD but got: " + sql);
  }

  // Minimal unparse harness (identifier arguments only).
  private static String unparseCall(String opName, String... argSqlFragments) {
    SqlWriter w = new SqlPrettyWriter(ClickHouseSqlDialect.INSTANCE);
    SqlOperator op = ClickHouseDateTimeUnparser.operatorFor(opName);
    SqlNode[] args = new SqlNode[argSqlFragments.length];
    for (int i = 0; i < argSqlFragments.length; i++) {
      args[i] = new SqlIdentifier(argSqlFragments[i], SqlParserPos.ZERO);
    }
    SqlBasicCall call = new SqlBasicCall(op, args, SqlParserPos.ZERO);
    ClickHouseDateTimeUnparser.unparse(w, call, 0, 0);
    return w.toString();
  }

  // Unparse harness for DATE_ADD / DATE_SUB with (identifier, INTERVAL N UNIT).
  private static String unparseIntervalCall(
      String opName, String columnName, int magnitude, TimeUnit unit) {
    SqlWriter w = new SqlPrettyWriter(ClickHouseSqlDialect.INSTANCE);
    SqlOperator op = ClickHouseDateTimeUnparser.operatorFor(opName);
    SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(unit, null, SqlParserPos.ZERO);
    SqlNode intervalLit =
        SqlLiteral.createInterval(1, Integer.toString(magnitude), qualifier, SqlParserPos.ZERO);
    SqlNode[] args = new SqlNode[] {
        new SqlIdentifier(columnName, SqlParserPos.ZERO),
        intervalLit,
    };
    SqlBasicCall call = new SqlBasicCall(op, args, SqlParserPos.ZERO);
    ClickHouseDateTimeUnparser.unparse(w, call, 0, 0);
    return w.toString();
  }
}
