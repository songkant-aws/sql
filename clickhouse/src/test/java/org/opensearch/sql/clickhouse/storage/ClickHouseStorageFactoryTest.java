/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;

public class ClickHouseStorageFactoryTest {
  @Test
  public void factory_returns_CLICKHOUSE_type() {
    ClickHouseStorageFactory f = new ClickHouseStorageFactory(mock(Settings.class));
    assertEquals(DataSourceType.CLICKHOUSE, f.getDataSourceType());
  }

  @Test
  public void creates_datasource_with_storage_engine() {
    ClickHouseStorageFactory f = new ClickHouseStorageFactory(mock(Settings.class));
    DataSourceMetadata md = new DataSourceMetadata.Builder()
        .setName("my_ch")
        .setConnector(DataSourceType.CLICKHOUSE)
        .setProperties(Map.of(
            "clickhouse.uri", "jdbc:clickhouse://h:8123/default",
            "clickhouse.auth.type", "basic",
            "clickhouse.auth.username", "u",
            "clickhouse.auth.password", "p",
            "clickhouse.schema", "{\"databases\":[]}"))
        .build();
    DataSource ds = f.createDataSource(md);
    assertNotNull(ds.getStorageEngine());
    assertEquals(DataSourceType.CLICKHOUSE, ds.getConnectorType());
  }
}
