/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    JdbcSideInputFilter filter = JdbcSideInputFilter.create(scan, /*keyCol=*/ 0, param);

    assertNotNull(filter);
    assertEquals(0, filter.getKeyColumnIndex());
    assertEquals(param, filter.getArrayParam());

    // SQL-generation smoke test via ClickHouseSqlDialect
    SqlNode node = filter.toSqlNode(ClickHouseSqlDialect.INSTANCE);
    String sql =
        new SqlPrettyWriter(
                SqlPrettyWriter.config().withDialect(ClickHouseSqlDialect.INSTANCE))
            .format(node);
    // Accept either 'IN (?)' or 'IN ?' — both are unparse variants Calcite may produce.
    // Also accept SEARCH-based sarg form Calcite 1.41 may emit internally.
    boolean hasIn =
        sql.contains("IN (?)") || sql.contains("IN ?") || sql.toUpperCase().contains("SEARCH");
    assertTrue(hasIn, "Expected 'IN (?)' or 'IN ?' or SEARCH in SQL: " + sql);
    assertTrue(
        sql.toLowerCase().contains("user_id"), "Expected user_id ref in SQL: " + sql);
  }
}
