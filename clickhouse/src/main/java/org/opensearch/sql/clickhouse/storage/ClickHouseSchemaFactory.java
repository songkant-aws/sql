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
import org.apache.calcite.schema.Wrapper;
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
   * runtime expression via {@link Schemas#subSchemaExpression}. The convention's expression is
   * {@code Schemas.subSchemaExpression(parentSchema, datasourceName, JdbcSchema.class)}. Calcite
   * codegen resolves that expression via {@code parentSchema.getSubSchema(datasourceName)
   * .unwrap(JdbcSchema.class)}, which is handled by {@code SchemaPlusImpl.unwrap} — that delegate
   * succeeds only if the sub-schema we return here {@code implements Wrapper}. So the
   * per-datasource outer schema must be a {@link Wrapper} that exposes one of its inner
   * {@link JdbcSchema} delegates (all inner schemas wrap the same {@link DataSource}, so any one
   * suffices for the codegen {@code .unwrap(DataSource.class)} step that follows).
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
    // Hold a reference to any one of the per-database JdbcSchemas so the outer schema can
    // expose it via unwrap(JdbcSchema.class). All per-db schemas share the same DataSource,
    // so which one we pick doesn't matter to codegen's subsequent .unwrap(DataSource.class).
    JdbcSchema anyDelegate = null;
    for (ClickHouseTableSpec.Database db : spec.getDatabases()) {
      AbstractSchema perDbWrapper =
          ClickHouseJdbcSchemaBuilder.build(
              dataSource,
              ClickHouseSqlDialect.INSTANCE,
              convention,
              db.getName(),
              spec,
              db.getTables());
      subs.put(db.getName(), perDbWrapper);
      if (anyDelegate == null) {
        // perDbWrapper is a WrappingSchema; its Wrapper.unwrap returns the inner JdbcSchema.
        anyDelegate = ((Wrapper) perDbWrapper).unwrap(JdbcSchema.class);
      }
    }
    return new OuterWrapper(subs, anyDelegate);
  }

  /**
   * Outer per-datasource wrapper. {@code implements Wrapper} so Calcite's {@code
   * SchemaPlusImpl.unwrap(JdbcSchema.class)} — called by the convention's runtime expression —
   * delegates here and gets a real {@link JdbcSchema} back. Without the {@code Wrapper} marker,
   * Calcite throws {@code ClassCastException: not a class ... JdbcSchema}.
   */
  private static final class OuterWrapper extends AbstractSchema implements Wrapper {
    private final Map<String, Schema> subs;
    private final JdbcSchema delegate; // any one of the per-db JdbcSchemas; all share ds.

    OuterWrapper(Map<String, Schema> subs, JdbcSchema delegate) {
      this.subs = subs;
      this.delegate = delegate;
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
      return subs;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
      if (delegate != null && clazz.isInstance(delegate)) {
        return clazz.cast(delegate);
      }
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      return null;
    }
  }
}
