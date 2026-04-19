/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;

public class HikariClickHouseClientTest {
  @Test
  public void hikari_pool_honors_config() {
    ClickHouseDataSourceConfig cfg = ClickHouseDataSourceConfig.builder()
        .uri("jdbc:clickhouse://host:8123/default")
        .authType("basic").username("u").password("p")
        .poolMaxSize(12).poolMinIdle(3)
        .rateLimitQps(50).rateLimitConcurrent(20)
        .slowQueryThresholdMs(5000)
        .socketTimeoutMs(15000)
        .schema(new ClickHouseTableSpec.Schema(java.util.List.of()))
        .build();
    try (HikariClickHouseClient c = HikariClickHouseClient.create(cfg)) {
      HikariDataSource ds = (HikariDataSource) c.getDataSource();
      assertNotNull(ds);
      assertEquals(12, ds.getMaximumPoolSize());
      assertEquals(3, ds.getMinimumIdle());
      assertEquals("SELECT 1", ds.getConnectionTestQuery());
      assertEquals("jdbc:clickhouse://host:8123/default", ds.getJdbcUrl());
      // auth applied via dataSourceProperties
      assertEquals("u", ds.getDataSourceProperties().getProperty("user"));
    }
  }
}
