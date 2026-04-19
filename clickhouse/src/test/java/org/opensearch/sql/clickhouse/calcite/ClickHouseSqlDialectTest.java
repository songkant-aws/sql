/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.jupiter.api.Test;

public class ClickHouseSqlDialectTest {
  @Test
  public void quotes_identifiers_with_backticks() {
    SqlPrettyWriter w = new SqlPrettyWriter(ClickHouseSqlDialect.INSTANCE);
    new SqlIdentifier("foo", SqlParserPos.ZERO).unparse(w, 0, 0);
    assertEquals("`foo`", w.toString());
  }

  @Test
  public void supports_standard_aggregates() {
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsAggregateFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsAggregateFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsAggregateFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.AVG));
  }
}
