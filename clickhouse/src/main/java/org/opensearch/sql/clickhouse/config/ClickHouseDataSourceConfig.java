/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
@AllArgsConstructor
public class ClickHouseDataSourceConfig {
  public static final String URI = "clickhouse.uri";
  public static final String AUTH_TYPE = "clickhouse.auth.type";
  public static final String AUTH_USERNAME = "clickhouse.auth.username";
  public static final String AUTH_PASSWORD = "clickhouse.auth.password";
  public static final String AUTH_TOKEN = "clickhouse.auth.token";
  public static final String TLS_ENABLED = "clickhouse.tls.enabled";
  public static final String TLS_TRUST_STORE_PATH = "clickhouse.tls.trust_store_path";
  public static final String TLS_TRUST_STORE_PASSWORD = "clickhouse.tls.trust_store_password";
  public static final String JDBC_SOCKET_TIMEOUT = "clickhouse.jdbc.socket_timeout";
  public static final String JDBC_COMPRESS = "clickhouse.jdbc.compress";
  public static final String POOL_MAX_SIZE = "clickhouse.pool.max_size";
  public static final String POOL_MIN_IDLE = "clickhouse.pool.min_idle";
  public static final String RATE_LIMIT_QPS = "clickhouse.rate_limit.qps";
  public static final String RATE_LIMIT_CONCURRENT = "clickhouse.rate_limit.concurrent";
  public static final String SLOW_QUERY_THRESHOLD_MS = "clickhouse.slow_query.threshold_ms";
  public static final String SCHEMA = "clickhouse.schema";

  private final String uri;
  private final String authType;
  private final String username;
  private final String password;
  private final String token;
  private final boolean tlsEnabled;
  private final String trustStorePath;
  private final String trustStorePassword;
  private final int socketTimeoutMs;
  private final boolean compress;
  private final int poolMaxSize;
  private final int poolMinIdle;
  private final int rateLimitQps;
  private final int rateLimitConcurrent;
  private final int slowQueryThresholdMs;
  private final ClickHouseTableSpec.Schema schema;

  public static ClickHouseDataSourceConfig parse(Map<String, String> props) {
    String uri = require(props, URI);
    String authType = props.getOrDefault(AUTH_TYPE, "basic");
    String username = props.get(AUTH_USERNAME);
    String password = props.get(AUTH_PASSWORD);
    String token = props.get(AUTH_TOKEN);

    if ("basic".equalsIgnoreCase(authType)) {
      if (isBlank(username) || isBlank(password)) {
        throw new IllegalArgumentException(
            "clickhouse.auth.type=basic requires auth.username and auth.password");
      }
    } else if ("jwt".equalsIgnoreCase(authType)) {
      if (isBlank(token)) {
        throw new IllegalArgumentException("clickhouse.auth.type=jwt requires auth.token");
      }
    } else {
      throw new IllegalArgumentException("Unsupported clickhouse.auth.type: " + authType);
    }

    ClickHouseTableSpec.Schema schema;
    try {
      String schemaJson = props.getOrDefault(SCHEMA, "{\"databases\":[]}");
      schema = new ObjectMapper().readValue(schemaJson, ClickHouseTableSpec.Schema.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid clickhouse.schema JSON: " + e.getMessage(), e);
    }

    return ClickHouseDataSourceConfig.builder()
        .uri(uri)
        .authType(authType)
        .username(username)
        .password(password)
        .token(token)
        .tlsEnabled(Boolean.parseBoolean(props.getOrDefault(TLS_ENABLED, "false")))
        .trustStorePath(props.get(TLS_TRUST_STORE_PATH))
        .trustStorePassword(props.get(TLS_TRUST_STORE_PASSWORD))
        .socketTimeoutMs(parseInt(props, JDBC_SOCKET_TIMEOUT, 30_000))
        .compress(Boolean.parseBoolean(props.getOrDefault(JDBC_COMPRESS, "false")))
        .poolMaxSize(parseInt(props, POOL_MAX_SIZE, 10))
        .poolMinIdle(parseInt(props, POOL_MIN_IDLE, 2))
        .rateLimitQps(parseInt(props, RATE_LIMIT_QPS, 50))
        .rateLimitConcurrent(parseInt(props, RATE_LIMIT_CONCURRENT, 20))
        .slowQueryThresholdMs(parseInt(props, SLOW_QUERY_THRESHOLD_MS, 5000))
        .schema(schema == null
            ? new ClickHouseTableSpec.Schema(java.util.List.of())
            : (schema.getDatabases() == null
                ? new ClickHouseTableSpec.Schema(java.util.List.of())
                : schema))
        .build();
  }

  private static String require(Map<String, String> props, String key) {
    String v = props.get(key);
    if (isBlank(v)) {
      throw new IllegalArgumentException("Missing required property: " + key);
    }
    return v;
  }

  private static int parseInt(Map<String, String> props, String key, int def) {
    String v = props.get(key);
    if (v == null) return def;
    try {
      return Integer.parseInt(v);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be an integer, got: " + v);
    }
  }

  private static boolean isBlank(String s) {
    return s == null || s.trim().isEmpty();
  }
}
