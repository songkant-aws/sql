/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Timestamp;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the pure helper {@link SideInputBindableWrapper#inferSqlTypeName} — the mapping
 * from a runtime key array's Java class to the ClickHouse SQL type name used by {@code
 * Connection.createArrayOf}.
 */
class SideInputBindableWrapperTest {

  @Test
  void longArrayMapsToInt64() {
    assertEquals("Int64", SideInputBindableWrapper.inferSqlTypeName(new Long[] {1L, 2L, 3L}));
  }

  @Test
  void integerArrayMapsToInt32() {
    assertEquals("Int32", SideInputBindableWrapper.inferSqlTypeName(new Integer[] {1, 2, 3}));
  }

  @Test
  void shortArrayMapsToInt16() {
    assertEquals(
        "Int16",
        SideInputBindableWrapper.inferSqlTypeName(
            new Short[] {(short) 1, (short) 2, (short) 3}));
  }

  @Test
  void byteArrayMapsToInt8() {
    assertEquals(
        "Int8",
        SideInputBindableWrapper.inferSqlTypeName(
            new Byte[] {(byte) 1, (byte) 2, (byte) 3}));
  }

  @Test
  void doubleArrayMapsToFloat64() {
    assertEquals(
        "Float64", SideInputBindableWrapper.inferSqlTypeName(new Double[] {1.0, 2.0, 3.0}));
  }

  @Test
  void floatArrayMapsToFloat32() {
    assertEquals(
        "Float32",
        SideInputBindableWrapper.inferSqlTypeName(new Float[] {1.0f, 2.0f, 3.0f}));
  }

  @Test
  void stringArrayMapsToString() {
    assertEquals(
        "String", SideInputBindableWrapper.inferSqlTypeName(new String[] {"a", "b", "c"}));
  }

  @Test
  void leadingNullsAreSkippedAndFirstNonNullDrivesTheType() {
    // Confirms the helper skips nulls and picks the runtime type of the first non-null element.
    assertEquals(
        "Int64", SideInputBindableWrapper.inferSqlTypeName(new Object[] {null, 1L, 2L}));
  }

  @Test
  void emptyArrayReturnsNull() {
    // Empty arrays have no element from which to infer a type — callers must fall back.
    assertNull(SideInputBindableWrapper.inferSqlTypeName(new Object[0]));
  }

  @Test
  void unsupportedTypeReturnsNull() {
    // java.sql.Timestamp is not in the supported set; helper must return null so the caller
    // bypasses the createArrayOf path and falls back to the Object[] bind.
    Timestamp ts = new Timestamp(0L);
    assertNull(SideInputBindableWrapper.inferSqlTypeName(new Timestamp[] {ts}));
  }

  @Test
  void allNullArrayReturnsNull() {
    // No non-null element -> null return so the caller falls back to the Object[] path.
    assertNull(SideInputBindableWrapper.inferSqlTypeName(new Object[] {null, null}));
  }
}
