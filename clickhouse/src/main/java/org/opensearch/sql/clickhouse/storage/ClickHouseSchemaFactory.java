/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.calcite.adapter.jdbc.ClickHouseJdbcSchemaBuilder;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.sql.clickhouse.calcite.ClickHouseConvention;
import org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;

public final class ClickHouseSchemaFactory {
  private ClickHouseSchemaFactory() {}

  /**
   * Build a Calcite Schema whose sub-schemas correspond to CH databases, each sub-schema holding
   * JdbcTable instances backed by the given DataSource and a per-datasource ClickHouseConvention.
   *
   * <p>{@code parentSchema} is the {@link SchemaPlus} under which the returned schema will be
   * mounted (by {@code ClickHouseSchema.getSubSchemaMap}); it is used to construct the convention's
   * runtime expression via {@link Schemas#subSchemaExpression}.
   */
  public static Schema build(
      SchemaPlus parentSchema,
      String datasourceName,
      DataSource dataSource,
      ClickHouseTableSpec.Schema spec) {
    Expression expression =
        Schemas.subSchemaExpression(parentSchema, datasourceName, JdbcSchema.class);
    ClickHouseConvention convention = ClickHouseConvention.of(datasourceName, expression);
    final Map<String, Schema> subs = new LinkedHashMap<>();
    for (ClickHouseTableSpec.Database db : spec.getDatabases()) {
      subs.put(
          db.getName(),
          ClickHouseJdbcSchemaBuilder.build(
              dataSource,
              ClickHouseSqlDialect.INSTANCE,
              convention,
              db.getName(),
              spec,
              db.getTables()));
    }
    return new AbstractSchema() {
      @Override
      protected Map<String, Schema> getSubSchemaMap() {
        return subs;
      }
    };
  }
}
