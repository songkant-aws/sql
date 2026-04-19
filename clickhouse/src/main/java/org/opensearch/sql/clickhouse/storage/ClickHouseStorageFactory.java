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
    return new DataSource(
        metadata.getName(),
        DataSourceType.CLICKHOUSE,
        new ClickHouseStorageEngine(cfg, client));
  }
}
