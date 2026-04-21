/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.calcite;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.opensearch.sql.clickhouse.calcite.federation.PplFederationDialect;
import org.opensearch.sql.clickhouse.calcite.federation.PplFederationDialectRegistry;

public class ClickHouseSqlDialect extends SqlDialect {
  public static final SqlDialect.Context CTX =
      SqlDialect.EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.UNKNOWN)
          .withDatabaseProductName("ClickHouse")
          .withIdentifierQuoteString("`")
          .withCaseSensitive(true)
          .withNullCollation(NullCollation.LOW);

  public static final ClickHouseSqlDialect INSTANCE = new ClickHouseSqlDialect();

  static {
    PplFederationDialectRegistry.register(
        INSTANCE,
        new PplFederationDialect() {
          @Override
          public long getInListPushdownThreshold() {
            return 10_000L;
          }

          @Override
          public boolean supportsArrayInListParam() {
            return true;
          }
        });
  }

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
      case CEIL: case FLOOR:
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
      case "ABS":
      case "SQRT":
      case "EXP":
      case "LN":
      case "LOG10":
      case "POWER":
      case "ROUND":
      case "SIGN":
      case "SIN":
      case "COS":
      case "TAN":
      case "ATAN":
      case "ATAN2":
      case "POSITION":
      case "CHAR_LENGTH":
      case "REPLACE":
      case "REVERSE":
        return true;
      default:
        return false;
    }
  }

  /**
   * ClickHouse rejects ANSI {@code OFFSET ... FETCH NEXT ... ROWS ONLY} with
   * "Code: 628. DB::Exception: Can not use OFFSET FETCH clause without ORDER BY." It accepts
   * {@code LIMIT <count> [OFFSET <skip>]} just fine — which is also what Calcite's own bundled
   * {@code org.apache.calcite.sql.dialect.ClickHouseSqlDialect} generates. Default
   * {@link SqlDialect#unparseOffsetFetch} emits ANSI syntax, so override to the LIMIT form.
   * (We don't inherit from Calcite's CH dialect because this module owns a leaner
   * customization — just override the one method we need.)
   */
  @Override
  public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
    unparseFetchUsingLimit(writer, offset, fetch);
  }
}
