/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.calcite.ClickHouseConvention;
import org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect;

public class JdbcSideInputFilterTest {

  @Test
  public void emitsInQuestionMarkForArrayParam() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
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
    RelBuilder builder = RelBuilder.create(config);

    RelDataTypeFactory tf = builder.getTypeFactory();
    RexDynamicParam param =
        builder
            .getRexBuilder()
            .makeDynamicParam(tf.createArrayType(tf.createSqlType(SqlTypeName.BIGINT), -1), 0);

    RelNode scan = builder.scan("events").build();

    // A ClickHouseConvention (which extends JdbcConvention) is required so the filter
    // participates in Calcite's JDBC pushdown path under JdbcToEnumerableConverter. The
    // expression is irrelevant here — it's only evaluated at codegen time, which the unit
    // test does not exercise. Tests elsewhere (see ClickHouseConventionTest) cover the
    // codegen-time resolution invariants.
    ClickHouseConvention convention =
        ClickHouseConvention.of(
            "jdbc_side_input_filter_test", Expressions.constant(null, JdbcSchema.class));

    JdbcSideInputFilter filter =
        JdbcSideInputFilter.create(scan, /*keyCol=*/ 0, param, convention);

    assertNotNull(filter);
    assertEquals(0, filter.getKeyColumnIndex());
    assertEquals(param, filter.getArrayParam());
    // Spec invariant: the filter must be a JdbcRel in a JdbcConvention so Calcite's JDBC
    // pushdown path (JdbcToEnumerableConverter) picks up this subtree.
    assertTrue(
        filter.getConvention() instanceof JdbcConvention,
        "Expected filter.getConvention() to be a JdbcConvention, got: " + filter.getConvention());

    // SQL-generation smoke test via ClickHouseSqlDialect
    SqlNode node = filter.toSqlNode(ClickHouseSqlDialect.INSTANCE);
    String sql =
        new SqlPrettyWriter(
                SqlPrettyWriter.config().withDialect(ClickHouseSqlDialect.INSTANCE))
            .format(node);
    // ARRAY_IN_OP.unparse must emit the literal 'IN (?)' form: that is the whole point of this
    // filter (bypass Calcite 1.41's Sarg/SEARCH rewrite of plain IN-lists). If this fails,
    // investigate the unparse path; do not weaken this assertion.
    assertTrue(sql.contains("IN (?)"), "expected 'IN (?)' in rendered SQL, got: " + sql);
    assertTrue(
        sql.toLowerCase().contains("user_id"), "Expected user_id ref in SQL: " + sql);
  }
}
