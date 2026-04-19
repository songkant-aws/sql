/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.schema.Schema;
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
 */
@Getter
@AllArgsConstructor
public class ClickHouseSchema extends AbstractSchema {
  public static final String CLICKHOUSE_SCHEMA_NAME = "ClickHouse";

  private final DataSourceService dataSourceService;

  @Override
  protected Map<String, Schema> getSubSchemaMap() {
    if (dataSourceService == null) {
      return Map.of();
    }
    Map<String, Schema> result = new LinkedHashMap<>();
    for (DataSourceMetadata md : dataSourceService.getDataSourceMetadata(true)) {
      if (!DataSourceType.CLICKHOUSE.equals(md.getConnector())) {
        continue;
      }
      DataSource ds = dataSourceService.getDataSource(md.getName());
      if (ds.getStorageEngine() instanceof CalciteSchemaProvider provider) {
        result.put(md.getName(), provider.asCalciteSchema(md.getName()));
      }
    }
    return result;
  }
}
