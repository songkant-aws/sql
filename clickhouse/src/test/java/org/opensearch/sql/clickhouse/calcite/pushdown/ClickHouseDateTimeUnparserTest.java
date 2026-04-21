/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
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
    assertTrue(sql.contains("formatDateTime"),
        "expected formatDateTime but got: " + sql);
    assertFalse(sql.contains("DATE_FORMAT"),
        "must rewrite away DATE_FORMAT but got: " + sql);
  }

  @Test
  public void unparses_year_to_toYear() {
    String sql = unparseCall("YEAR", "ts_col");
    assertTrue(sql.contains("toYear"), "expected toYear but got: " + sql);
  }

  @Test
  public void unparses_now_to_now() {
    String sql = unparseCall("NOW");
    assertTrue(sql.contains("now"), "expected now() but got: " + sql);
  }

  @Test
  public void unknown_op_returns_null_handler() {
    assertFalse(ClickHouseDateTimeUnparser.hasHandler("NOT_A_DATETIME_FN"));
  }

  // Minimal unparse harness
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
}
