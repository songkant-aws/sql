/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.sql.Timestamp;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.calcite.ClickHouseConvention;

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

  // -------- hasSideInputMarker --------

  @Test
  void hasSideInputMarkerFindsArrayInInsideJdbcSideInputFilter() {
    // The straight-line happy case: ARRAY_IN is the condition of a JdbcSideInputFilter, detection
    // must succeed.
    RelBuilder builder = newBuilder();
    RelNode scan = builder.scan("events").build();
    ClickHouseConvention convention =
        ClickHouseConvention.of("tm1", Expressions.constant(null, JdbcSchema.class));
    RexDynamicParam param = arrayParam(builder.getRexBuilder(), builder.getTypeFactory());
    JdbcSideInputFilter filter = JdbcSideInputFilter.create(scan, /*keyCol=*/ 0, param, convention);
    assertTrue(SideInputBindableWrapper.hasSideInputMarker(filter));
  }

  @Test
  void hasSideInputMarkerFindsArrayInInsideMergedPlainFilter() {
    // The bug repro: FilterMergeRule has collapsed a JdbcSideInputFilter + user filter into a
    // plain LogicalFilter whose condition is AND(ARRAY_IN(...), <user pred>). Class-based
    // detection misses this shape; operator-based detection must still find the marker.
    RelBuilder builder = newBuilder();
    RelNode scan = builder.scan("events").build();
    RexBuilder rex = builder.getRexBuilder();
    RelDataTypeFactory tf = builder.getTypeFactory();
    RexDynamicParam param = arrayParam(rex, tf);

    RexNode arrayIn =
        rex.makeCall(
            tf.createSqlType(SqlTypeName.BOOLEAN),
            JdbcSideInputFilter.ARRAY_IN_OP,
            ImmutableList.of(RexInputRef.of(0, scan.getRowType()), param));
    RexNode userPred =
        rex.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            RexInputRef.of(1, scan.getRowType()),
            rex.makeExactLiteral(java.math.BigDecimal.ZERO));
    RexNode merged =
        rex.makeCall(SqlStdOperatorTable.AND, ImmutableList.of(arrayIn, userPred));

    RelNode plainFilter =
        RelFactories.DEFAULT_FILTER_FACTORY.createFilter(
            scan, merged, com.google.common.collect.ImmutableSet.of());
    assertTrue(SideInputBindableWrapper.hasSideInputMarker(plainFilter));
  }

  @Test
  void hasSideInputMarkerReturnsFalseOnUnrelatedFilter() {
    // A filter with only user predicates (no ARRAY_IN call) must not match — detection should be
    // specific to our marker, not to any filter.
    RelBuilder builder = newBuilder();
    RelNode scan = builder.scan("events").build();
    RexBuilder rex = builder.getRexBuilder();
    RexNode userPred =
        rex.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            RexInputRef.of(1, scan.getRowType()),
            rex.makeExactLiteral(java.math.BigDecimal.ZERO));
    RelNode plainFilter =
        RelFactories.DEFAULT_FILTER_FACTORY.createFilter(
            scan, userPred, com.google.common.collect.ImmutableSet.of());
    assertFalse(SideInputBindableWrapper.hasSideInputMarker(plainFilter));
  }

  private static RelBuilder newBuilder() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add(
        "events",
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory f) {
            return f.builder()
                .add("user_id", SqlTypeName.BIGINT)
                .add("v", SqlTypeName.DOUBLE)
                .build();
          }
        });
    FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(rootSchema).build();
    return RelBuilder.create(config);
  }

  private static RexDynamicParam arrayParam(RexBuilder rex, RelDataTypeFactory tf) {
    return rex.makeDynamicParam(tf.createArrayType(tf.createSqlType(SqlTypeName.BIGINT), -1), 0);
  }
}
