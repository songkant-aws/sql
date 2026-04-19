/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;

public class ClickHouseAuthProviderTest {
  private ClickHouseDataSourceConfig.ClickHouseDataSourceConfigBuilder baseBuilder() {
    return ClickHouseDataSourceConfig.builder()
        .uri("jdbc:clickhouse://h:8123/default")
        .poolMaxSize(10).poolMinIdle(2)
        .rateLimitQps(50).rateLimitConcurrent(20)
        .slowQueryThresholdMs(5000)
        .schema(new ClickHouseTableSpec.Schema(java.util.List.of()));
  }

  @Test
  public void basic_auth_sets_user_and_password() {
    ClickHouseDataSourceConfig cfg = baseBuilder()
        .authType("basic").username("u").password("p").build();
    Properties p = ClickHouseAuthProvider.build(cfg);
    assertEquals("u", p.getProperty("user"));
    assertEquals("p", p.getProperty("password"));
    assertNull(p.getProperty("access_token"));
  }

  @Test
  public void jwt_auth_sets_access_token_only() {
    ClickHouseDataSourceConfig cfg = baseBuilder()
        .authType("jwt").token("tok").build();
    Properties p = ClickHouseAuthProvider.build(cfg);
    assertEquals("tok", p.getProperty("access_token"));
    assertNull(p.getProperty("user"));
    assertNull(p.getProperty("password"));
  }

  @Test
  public void tls_sets_ssl_properties() {
    ClickHouseDataSourceConfig cfg = baseBuilder()
        .authType("basic").username("u").password("p")
        .tlsEnabled(true)
        .trustStorePath("/t.jks").trustStorePassword("pw").build();
    Properties p = ClickHouseAuthProvider.build(cfg);
    assertEquals("true", p.getProperty("ssl"));
    assertEquals("strict", p.getProperty("sslmode"));
    assertTrue(p.containsKey("trust_store"));
  }

  @Test
  public void socket_timeout_and_compress_flags_set() {
    ClickHouseDataSourceConfig cfg = baseBuilder()
        .authType("basic").username("u").password("p")
        .socketTimeoutMs(15000).compress(true).build();
    Properties p = ClickHouseAuthProvider.build(cfg);
    assertEquals("15000", p.getProperty("socket_timeout"));
    assertEquals("true", p.getProperty("compress"));
  }
}
