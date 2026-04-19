/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.clickhouse.client.ClickHouseClient;
import org.opensearch.sql.clickhouse.client.ClickHouseClientFactory;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.DataSourceFactory;

@RequiredArgsConstructor
public class ClickHouseStorageFactory implements DataSourceFactory {
  private final Settings settings;

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.CLICKHOUSE;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    Map<String, String> props = metadata.getProperties();
    ClickHouseDataSourceConfig cfg = ClickHouseDataSourceConfig.parse(props);
    ClickHouseClient client = ClickHouseClientFactory.create(cfg);
    try (java.sql.Connection conn = client.getDataSource().getConnection();
         java.sql.Statement st = conn.createStatement();
         java.sql.ResultSet rs = st.executeQuery("SELECT 1")) {
      if (!rs.next()) {
        throw new org.opensearch.sql.clickhouse.exception.ClickHouseConnectionException(
            "SELECT 1 returned no rows", null);
      }
    } catch (java.sql.SQLException e) {
      client.close();
      throw new org.opensearch.sql.clickhouse.exception.ClickHouseConnectionException(
          "Failed to connect to ClickHouse: " + e.getMessage(), e);
    }
    return new DataSource(
        metadata.getName(),
        DataSourceType.CLICKHOUSE,
        new ClickHouseStorageEngine(cfg, client));
  }
}
