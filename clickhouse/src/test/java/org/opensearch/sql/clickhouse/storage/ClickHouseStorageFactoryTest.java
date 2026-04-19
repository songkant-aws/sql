/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;

public class ClickHouseStorageFactoryTest {
  @Test
  public void factory_returns_CLICKHOUSE_type() {
    ClickHouseStorageFactory f = new ClickHouseStorageFactory(mock(Settings.class));
    assertEquals(DataSourceType.CLICKHOUSE, f.getDataSourceType());
  }

  @Test
  public void createDataSource_probes_connection_and_wraps_failure() {
    ClickHouseStorageFactory f = new ClickHouseStorageFactory(mock(Settings.class));
    DataSourceMetadata md = new DataSourceMetadata.Builder()
        .setName("my_ch")
        .setConnector(DataSourceType.CLICKHOUSE)
        .setProperties(Map.of(
            "clickhouse.uri", "jdbc:clickhouse://127.0.0.1:1/default",
            "clickhouse.auth.type", "basic",
            "clickhouse.auth.username", "u",
            "clickhouse.auth.password", "p",
            "clickhouse.schema", "{\"databases\":[]}"))
        .build();
    assertThrows(
        org.opensearch.sql.clickhouse.exception.ClickHouseConnectionException.class,
        () -> f.createDataSource(md));
  }
}
