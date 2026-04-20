/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.calcite.adapter.jdbc;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlDialect;
import org.opensearch.sql.clickhouse.config.ClickHouseColumnSpec;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;
import org.opensearch.sql.clickhouse.type.ClickHouseTypeMapper;

/**
 * Builds a Calcite {@link Schema} whose tables are {@link JdbcTable} instances. Lives in the
 * {@code org.apache.calcite.adapter.jdbc} package so it can invoke the package-private {@code
 * JdbcTable} constructor.
 *
 * <p>The returned schema {@code implements} {@link Wrapper} so Calcite's {@code SchemaPlusImpl
 * .unwrap(Class)} — which only delegates to the wrapped schema's {@code unwrap} when the wrapped
 * schema is a {@link Wrapper} — routes {@code parentSchema.getSubSchema(name).unwrap(JdbcSchema
 * .class)} into our override, yielding the inner {@link JdbcSchema}. Calcite's codegen then does
 * a second {@code .unwrap(DataSource.class)} on the {@link JdbcSchema} (itself a {@link Wrapper})
 * and reaches the real {@link javax.sql.DataSource}.
 */
public final class ClickHouseJdbcSchemaBuilder {
  private ClickHouseJdbcSchemaBuilder() {}

  public static AbstractSchema build(
      DataSource ds,
      SqlDialect dialect,
      JdbcConvention convention,
      String catalog,
      ClickHouseTableSpec.Schema ignored, // row type is read lazily via metadata
      List<ClickHouseTableSpec> tableSpecs) {
    final JdbcSchema delegate = new JdbcSchema(ds, dialect, convention, catalog, null);
    final Map<String, Table> tables = new LinkedHashMap<>();
    for (ClickHouseTableSpec tbl : tableSpecs) {
      // Validate every (ch_type, expr_type) pair — throw early on unsupported columns.
      for (ClickHouseColumnSpec col : tbl.getColumns()) {
        ClickHouseTypeMapper.resolve(col.getChType(), col.getExprType());
      }
      tables.put(
          tbl.getName(),
          new JdbcTable(delegate, catalog, null, tbl.getName(), Schema.TableType.TABLE));
    }
    return new WrappingSchema(tables, delegate);
  }

  /**
   * Named inner class so the returned schema can both extend {@link AbstractSchema} (for the
   * curated table map) AND implement {@link Wrapper} (so Calcite's {@code SchemaPlusImpl.unwrap}
   * delegates to our {@link #unwrap} override). Anonymous classes cannot implement extra
   * interfaces, which is why this is hoisted out.
   */
  private static final class WrappingSchema extends AbstractSchema implements Wrapper {
    private final Map<String, Table> tables;
    private final JdbcSchema delegate;

    WrappingSchema(Map<String, Table> tables, JdbcSchema delegate) {
      this.tables = tables;
      this.delegate = delegate;
    }

    @Override
    protected Map<String, Table> getTableMap() {
      return tables;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
      // Entry point reached via SchemaPlusImpl.unwrap → Wrapper.unwrapOrThrow → this.unwrap.
      // Expose the inner JdbcSchema so the next .unwrap(DataSource.class) step (on the
      // JdbcSchema itself, which also implements Wrapper) can reach the real DataSource.
      if (clazz.isInstance(delegate)) {
        return clazz.cast(delegate);
      }
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      return null;
    }
  }
}
