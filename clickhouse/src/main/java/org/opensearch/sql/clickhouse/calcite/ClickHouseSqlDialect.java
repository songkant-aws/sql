/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.calcite;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;

public class ClickHouseSqlDialect extends SqlDialect {
  public static final SqlDialect.Context CTX =
      SqlDialect.EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.UNKNOWN)
          .withDatabaseProductName("ClickHouse")
          .withIdentifierQuoteString("`")
          .withCaseSensitive(true)
          .withNullCollation(NullCollation.LOW);

  public static final ClickHouseSqlDialect INSTANCE = new ClickHouseSqlDialect();

  private ClickHouseSqlDialect() {
    super(CTX);
  }

  @Override
  public boolean supportsAggregateFunction(SqlKind kind) {
    return kind == SqlKind.COUNT || kind == SqlKind.SUM || kind == SqlKind.AVG
        || kind == SqlKind.MIN || kind == SqlKind.MAX;
  }

  public boolean supportsAggregateFunction(org.apache.calcite.sql.SqlOperator op) {
    return supportsAggregateFunction(op.getKind());
  }

  @Override
  public boolean supportsFunction(
      org.apache.calcite.sql.SqlOperator op,
      org.apache.calcite.rel.type.RelDataType type,
      java.util.List<org.apache.calcite.rel.type.RelDataType> paramTypes) {
    SqlKind k = op.getKind();
    switch (k) {
      case AND: case OR: case NOT:
      case EQUALS: case NOT_EQUALS:
      case LESS_THAN: case LESS_THAN_OR_EQUAL:
      case GREATER_THAN: case GREATER_THAN_OR_EQUAL:
      case IS_NULL: case IS_NOT_NULL:
      case PLUS: case MINUS: case TIMES: case DIVIDE: case MOD:
      case CAST: case COALESCE: case CASE:
      case LIKE:
      case COUNT: case SUM: case AVG: case MIN: case MAX:
        return true;
      default:
        break;
    }
    String name = op.getName().toUpperCase();
    switch (name) {
      case "SUBSTRING":
      case "LOWER":
      case "UPPER":
      case "LENGTH":
      case "TRIM":
      case "CONCAT":
      case "DATE_TRUNC":
        return true;
      default:
        return false;
    }
  }
}
