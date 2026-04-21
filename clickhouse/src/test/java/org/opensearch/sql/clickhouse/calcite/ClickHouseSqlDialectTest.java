/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
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

  /**
   * Pins the override of {@link org.apache.calcite.sql.SqlDialect#unparseOffsetFetch} so a
   * future maintainer cannot silently revert to the ANSI {@code OFFSET ... FETCH NEXT ... ROWS
   * ONLY} form. ClickHouse rejects ANSI OFFSET FETCH without ORDER BY (Code 628); we must emit
   * {@code LIMIT <count> OFFSET <skip>} instead. This is the unit-level guard for what is
   * otherwise only exercised via the binary-mode IT against a live CH server.
   */
  @Test
  public void unparse_offset_fetch_emits_limit_offset_not_ansi_fetch() {
    SqlPrettyWriter w = new SqlPrettyWriter(ClickHouseSqlDialect.INSTANCE);
    SqlNode offset = SqlLiteral.createExactNumeric("5", SqlParserPos.ZERO);
    SqlNode fetch = SqlLiteral.createExactNumeric("10", SqlParserPos.ZERO);

    ClickHouseSqlDialect.INSTANCE.unparseOffsetFetch(w, offset, fetch);

    String out = w.toString();
    assertTrue(out.contains("LIMIT"), "expected LIMIT in output but was: " + out);
    assertTrue(out.contains("OFFSET"), "expected OFFSET in output but was: " + out);
    assertFalse(
        out.toUpperCase().contains("FETCH NEXT"),
        "ClickHouse rejects ANSI FETCH NEXT; output must not emit it. Was: " + out);
  }

  /**
   * The LIMIT form must also hold when offset is absent (Calcite passes null for missing
   * offset). ClickHouse accepts {@code LIMIT n} without OFFSET.
   */
  @Test
  public void unparse_offset_fetch_without_offset_emits_limit_only() {
    SqlPrettyWriter w = new SqlPrettyWriter(ClickHouseSqlDialect.INSTANCE);
    SqlNode fetch = SqlLiteral.createExactNumeric("10", SqlParserPos.ZERO);

    ClickHouseSqlDialect.INSTANCE.unparseOffsetFetch(w, null, fetch);

    String out = w.toString();
    assertTrue(out.contains("LIMIT"), "expected LIMIT in output but was: " + out);
    assertFalse(
        out.toUpperCase().contains("FETCH NEXT"),
        "ClickHouse rejects ANSI FETCH NEXT; output must not emit it. Was: " + out);
  }

  @Test
  public void supports_scalar_math_functions() {
    org.apache.calcite.rel.type.RelDataTypeFactory tf =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    org.apache.calcite.rel.type.RelDataType dbl = tf.createSqlType(
        org.apache.calcite.sql.type.SqlTypeName.DOUBLE);
    java.util.List<org.apache.calcite.rel.type.RelDataType> oneDouble = java.util.List.of(dbl);
    java.util.List<org.apache.calcite.rel.type.RelDataType> twoDouble = java.util.List.of(dbl, dbl);

    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.ABS, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.CEIL, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.FLOOR, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.SQRT, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.EXP, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.LN, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.LOG10, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.POWER, dbl, twoDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.ROUND, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.SIGN, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.SIN, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.COS, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.TAN, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.ATAN, dbl, oneDouble));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.ATAN2, dbl, twoDouble));
  }

  @Test
  public void supports_scalar_string_functions() {
    org.apache.calcite.rel.type.RelDataTypeFactory tf =
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    org.apache.calcite.rel.type.RelDataType str = tf.createSqlType(
        org.apache.calcite.sql.type.SqlTypeName.VARCHAR);
    org.apache.calcite.rel.type.RelDataType intT = tf.createSqlType(
        org.apache.calcite.sql.type.SqlTypeName.INTEGER);
    java.util.List<org.apache.calcite.rel.type.RelDataType> oneStr = java.util.List.of(str);
    java.util.List<org.apache.calcite.rel.type.RelDataType> twoStr = java.util.List.of(str, str);
    java.util.List<org.apache.calcite.rel.type.RelDataType> threeStr =
        java.util.List.of(str, str, str);

    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.POSITION, intT, twoStr));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.CHAR_LENGTH, intT, oneStr));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlStdOperatorTable.REPLACE, str, threeStr));
    assertTrue(ClickHouseSqlDialect.INSTANCE.supportsFunction(
        org.apache.calcite.sql.fun.SqlLibraryOperators.REVERSE, str, oneStr));
  }
}
