/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect;

public class ClickHouseAggregateUnparserTest {

  @Test
  public void distinct_count_approx_unparses_to_uniq() {
    SqlPrettyWriter w = new SqlPrettyWriter(ClickHouseSqlDialect.INSTANCE);
    SqlNode[] args = new SqlNode[] {new SqlIdentifier("x", SqlParserPos.ZERO)};
    SqlBasicCall call =
        new SqlBasicCall(
            ClickHouseAggregateUnparser.operatorFor("DISTINCT_COUNT_APPROX"),
            args,
            SqlParserPos.ZERO);
    ClickHouseAggregateUnparser.unparse(w, call, 0, 0);

    String sql = w.toString();
    // Lock down: CH-native case-preserved function name. Regressing to
    // startFunCall() would uppercase "uniq" and fail startsWith.
    assertTrue(sql.startsWith("uniq("),
        "expected sql to start with uniq( but got: " + sql);
    // Arg must follow the opening paren.
    int openParen = sql.indexOf('(');
    assertTrue(openParen >= 0 && sql.indexOf("x", openParen) > openParen,
        "expected arg x after opening paren but got: " + sql);
    assertFalse(sql.contains("DISTINCT_COUNT_APPROX"),
        "must rewrite away DISTINCT_COUNT_APPROX but got: " + sql);
  }

  @Test
  public void has_handler_true_for_distinct_count_approx() {
    assertTrue(ClickHouseAggregateUnparser.hasHandler("DISTINCT_COUNT_APPROX"));
  }

  @Test
  public void has_handler_false_for_unknown() {
    assertFalse(ClickHouseAggregateUnparser.hasHandler("NOT_AN_AGG"));
  }

  @Test
  public void unparse_unregistered_op_delegates_to_default() {
    // Exercise the fallback branch where hasHandler == false: unparse() must
    // delegate to the op's own unparse() rather than emitting nothing or NPE.
    SqlWriter w = new SqlPrettyWriter(ClickHouseSqlDialect.INSTANCE);
    SqlOperator op = SqlStdOperatorTable.COUNT;
    SqlNode[] args = new SqlNode[] {new SqlIdentifier("y", SqlParserPos.ZERO)};
    SqlBasicCall call = new SqlBasicCall(op, args, SqlParserPos.ZERO);
    ClickHouseAggregateUnparser.unparse(w, call, 0, 0);
    String sql = w.toString();
    assertTrue(sql.contains("COUNT("),
        "expected default unparse to emit COUNT( but got: " + sql);
  }
}
