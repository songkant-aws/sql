/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class ClickHouseDataSourceConfigTest {
  @Test
  public void parses_required_and_optional_fields() {
    Map<String, String> props = Map.of(
        "clickhouse.uri", "jdbc:clickhouse://h:8123/default",
        "clickhouse.auth.type", "basic",
        "clickhouse.auth.username", "u",
        "clickhouse.auth.password", "p",
        "clickhouse.pool.max_size", "20",
        "clickhouse.schema",
        "{\"databases\":[{\"name\":\"db\",\"tables\":[{\"name\":\"t\",\"columns\":["
            + "{\"name\":\"c\",\"ch_type\":\"Int64\",\"expr_type\":\"LONG\"}]}]}]}");
    ClickHouseDataSourceConfig cfg = ClickHouseDataSourceConfig.parse(props);
    assertEquals("jdbc:clickhouse://h:8123/default", cfg.getUri());
    assertEquals("basic", cfg.getAuthType());
    assertEquals("u", cfg.getUsername());
    assertEquals("p", cfg.getPassword());
    assertEquals(20, cfg.getPoolMaxSize());
    assertEquals(1, cfg.getSchema().getDatabases().size());
    assertEquals("t", cfg.getSchema().getDatabases().get(0).getTables().get(0).getName());
  }

  @Test
  public void rejects_missing_uri() {
    Map<String, String> props = Map.of("clickhouse.auth.type", "basic");
    assertThrows(IllegalArgumentException.class, () -> ClickHouseDataSourceConfig.parse(props));
  }

  @Test
  public void rejects_basic_auth_without_credentials() {
    Map<String, String> props = Map.of(
        "clickhouse.uri", "jdbc:clickhouse://h:8123/default",
        "clickhouse.auth.type", "basic");
    assertThrows(IllegalArgumentException.class, () -> ClickHouseDataSourceConfig.parse(props));
  }

  @Test
  public void rejects_jwt_auth_without_token() {
    Map<String, String> props = Map.of(
        "clickhouse.uri", "jdbc:clickhouse://h:8123/default",
        "clickhouse.auth.type", "jwt");
    assertThrows(IllegalArgumentException.class, () -> ClickHouseDataSourceConfig.parse(props));
  }

  @Test
  public void pool_defaults_applied() {
    Map<String, String> props = Map.of(
        "clickhouse.uri", "jdbc:clickhouse://h:8123/default",
        "clickhouse.auth.type", "basic",
        "clickhouse.auth.username", "u",
        "clickhouse.auth.password", "p",
        "clickhouse.schema", "{\"databases\":[]}");
    ClickHouseDataSourceConfig cfg = ClickHouseDataSourceConfig.parse(props);
    assertEquals(10, cfg.getPoolMaxSize());
    assertEquals(2, cfg.getPoolMinIdle());
    assertEquals(50, cfg.getRateLimitQps());
    assertEquals(20, cfg.getRateLimitConcurrent());
    assertTrue(cfg.getSchema().getDatabases().isEmpty());
  }
}
