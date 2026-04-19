/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.exception.ClickHouseSchemaException;
import org.opensearch.sql.data.type.ExprCoreType;

public class ClickHouseTypeMapperTest {
  @Test
  public void maps_supported_types() {
    assertEquals(ExprCoreType.INTEGER, ClickHouseTypeMapper.resolve("Int32", "INTEGER"));
    assertEquals(ExprCoreType.LONG, ClickHouseTypeMapper.resolve("Int64", "LONG"));
    assertEquals(ExprCoreType.LONG, ClickHouseTypeMapper.resolve("UInt32", "LONG"));
    assertEquals(ExprCoreType.FLOAT, ClickHouseTypeMapper.resolve("Float32", "FLOAT"));
    assertEquals(ExprCoreType.DOUBLE, ClickHouseTypeMapper.resolve("Float64", "DOUBLE"));
    assertEquals(ExprCoreType.STRING, ClickHouseTypeMapper.resolve("String", "STRING"));
    assertEquals(ExprCoreType.BOOLEAN, ClickHouseTypeMapper.resolve("Bool", "BOOLEAN"));
    assertEquals(ExprCoreType.DATE, ClickHouseTypeMapper.resolve("Date", "DATE"));
    assertEquals(ExprCoreType.DATE, ClickHouseTypeMapper.resolve("Date32", "DATE"));
    assertEquals(ExprCoreType.TIMESTAMP, ClickHouseTypeMapper.resolve("DateTime", "TIMESTAMP"));
    assertEquals(
        ExprCoreType.TIMESTAMP,
        ClickHouseTypeMapper.resolve("DateTime64(3)", "TIMESTAMP"));
  }

  @Test
  public void rejects_uint64_decimal_nullable_array_etc() {
    for (String t :
        new String[] {
          "UInt64", "Int128", "UInt128", "Int256", "UInt256",
          "Decimal(10,2)", "Decimal64(4)", "Enum8('a'=1)",
          "LowCardinality(String)", "Nullable(Int32)", "Array(Int32)",
          "Tuple(Int32, String)", "Map(String, Int32)", "UUID", "IPv4", "IPv6"
        }) {
      assertThrows(
          ClickHouseSchemaException.class,
          () -> ClickHouseTypeMapper.resolve(t, "STRING"),
          "Should reject " + t);
    }
  }

  @Test
  public void rejects_mismatched_expr_type() {
    assertThrows(
        ClickHouseSchemaException.class,
        () -> ClickHouseTypeMapper.resolve("Int32", "STRING"));
  }
}
