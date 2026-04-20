/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.CalciteSchemaProvider;

/**
 * Calcite {@link AbstractSchema} that exposes one Calcite sub-schema per registered ClickHouse
 * datasource. Sub-schema map is resolved lazily on each access; individual sub-schemas are obtained
 * from each CH datasource's {@code StorageEngine} via {@link CalciteSchemaProvider} so {@code core}
 * stays decoupled from the {@code clickhouse} module.
 *
 * <p>A null {@code dataSourceService} is tolerated (returns an empty map) so plan-only test
 * harnesses that build a {@code FrameworkConfig} without a live service still succeed.
 *
 * <p>Use {@link #install} rather than {@code rootSchema.add(...)} directly — the schema instance
 * needs a reference to its own {@link SchemaPlus} parent in order to construct the per-datasource
 * {@code JdbcConvention} expression.
 */
@Getter
public class ClickHouseSchema extends AbstractSchema {
  public static final String CLICKHOUSE_SCHEMA_NAME = "ClickHouse";

  private final DataSourceService dataSourceService;

  /**
   * Set by {@link #install}; read by {@link #getSubSchemaMap}. {@code volatile} covers the
   * happens-before between {@code install()} (called from the main thread inside {@code
   * QueryService.buildFrameworkConfig}) and the planner threads that later dereference it.
   */
  private volatile SchemaPlus schemaPlus;

  public ClickHouseSchema(DataSourceService dataSourceService) {
    this.dataSourceService = dataSourceService;
  }

  /**
   * Create a {@link ClickHouseSchema}, mount it under {@code rootSchema} as {@link
   * #CLICKHOUSE_SCHEMA_NAME}, and capture the returned {@link SchemaPlus} so sub-schema
   * construction can build per-datasource convention expressions against it.
   *
   * @return the mounted {@link SchemaPlus} (convenience for callers).
   */
  public static SchemaPlus install(SchemaPlus rootSchema, DataSourceService dataSourceService) {
    ClickHouseSchema node = new ClickHouseSchema(dataSourceService);
    SchemaPlus added = rootSchema.add(CLICKHOUSE_SCHEMA_NAME, node);
    node.schemaPlus = added;
    return added;
  }

  /**
   * Package-private setter so tests that need to inspect {@link #getSubSchemaMap} against a
   * manually-constructed instance (rather than going through {@link #install}) can provide the
   * required {@link SchemaPlus} reference after mounting the node.
   */
  void setSchemaPlus(SchemaPlus schemaPlus) {
    this.schemaPlus = schemaPlus;
  }

  @Override
  protected Map<String, Schema> getSubSchemaMap() {
    if (dataSourceService == null || schemaPlus == null) {
      return Map.of();
    }
    Map<String, Schema> result = new LinkedHashMap<>();
    for (DataSourceMetadata md : dataSourceService.getDataSourceMetadata(true)) {
      if (!DataSourceType.CLICKHOUSE.equals(md.getConnector())) {
        continue;
      }
      DataSource ds = dataSourceService.getDataSource(md.getName());
      if (ds.getStorageEngine() instanceof CalciteSchemaProvider provider) {
        result.put(md.getName(), provider.asCalciteSchema(md.getName(), schemaPlus));
      }
    }
    return result;
  }
}
