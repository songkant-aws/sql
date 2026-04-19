/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.clickhouse.client.ClickHouseClient;
import org.opensearch.sql.clickhouse.config.ClickHouseColumnSpec;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;
import org.opensearch.sql.exception.SemanticCheckException;

public class ClickHouseStorageEngineTest {
  @Test
  public void getTable_returns_clickhouse_table_for_declared_table() {
    ClickHouseTableSpec col = new ClickHouseTableSpec(
        "events",
        List.of(new ClickHouseColumnSpec("id", "Int64", "LONG")));
    ClickHouseTableSpec.Database db = new ClickHouseTableSpec.Database(
        "analytics", List.of(col));
    ClickHouseDataSourceConfig cfg = ClickHouseDataSourceConfig.builder()
        .uri("jdbc:clickhouse://h:8123/default")
        .authType("basic").username("u").password("p")
        .poolMaxSize(10).poolMinIdle(2)
        .rateLimitQps(50).rateLimitConcurrent(20)
        .slowQueryThresholdMs(5000)
        .schema(new ClickHouseTableSpec.Schema(List.of(db)))
        .build();

    ClickHouseStorageEngine engine =
        new ClickHouseStorageEngine(cfg, mock(ClickHouseClient.class));
    assertNotNull(engine.getTable(
        new DataSourceSchemaName("my_ch", "analytics"), "events"));
  }

  @Test
  public void getTable_throws_for_unknown_table() {
    ClickHouseDataSourceConfig cfg = ClickHouseDataSourceConfig.builder()
        .uri("jdbc:clickhouse://h:8123/default")
        .authType("basic").username("u").password("p")
        .poolMaxSize(10).poolMinIdle(2)
        .rateLimitQps(50).rateLimitConcurrent(20)
        .slowQueryThresholdMs(5000)
        .schema(new ClickHouseTableSpec.Schema(List.of()))
        .build();

    ClickHouseStorageEngine engine =
        new ClickHouseStorageEngine(cfg, mock(ClickHouseClient.class));
    assertThrows(
        SemanticCheckException.class,
        () -> engine.getTable(new DataSourceSchemaName("my_ch", "x"), "y"));
  }
}
