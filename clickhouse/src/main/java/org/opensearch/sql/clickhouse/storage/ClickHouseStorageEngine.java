/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.clickhouse.client.ClickHouseClient;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

@RequiredArgsConstructor
public class ClickHouseStorageEngine
    implements StorageEngine, org.opensearch.sql.storage.CalciteSchemaProvider {
  private final ClickHouseDataSourceConfig config;
  private final ClickHouseClient client;

  public ClickHouseDataSourceConfig getConfig() {
    return config;
  }

  public ClickHouseClient getClient() {
    return client;
  }

  @Override
  public Table getTable(DataSourceSchemaName schemaName, String tableName) {
    String db = schemaName.getSchemaName();
    Optional<ClickHouseTableSpec> spec =
        config.getSchema().getDatabases().stream()
            .filter(d -> d.getName().equals(db))
            .flatMap(d -> d.getTables().stream())
            .filter(t -> t.getName().equals(tableName))
            .findFirst();
    if (spec.isEmpty()) {
      throw new SemanticCheckException(
          "Table " + db + "." + tableName + " is not declared for ClickHouse datasource");
    }
    return new ClickHouseTable(db, tableName, spec.get());
  }

  public org.apache.calcite.schema.Schema asCalciteSchema(String datasourceName) {
    return ClickHouseSchemaFactory.build(datasourceName, client.getDataSource(), config.getSchema());
  }
}
