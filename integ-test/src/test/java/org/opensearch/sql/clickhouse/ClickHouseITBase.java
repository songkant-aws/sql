/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse;

import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.testcontainers.clickhouse.ClickHouseContainer;

/**
 * Common base for ClickHouse integration tests. Supports two modes:
 *
 * <ul>
 *   <li><b>Testcontainers (default)</b>: a Dockerized {@link ClickHouseContainer} is started as a
 *       class rule. Requires Docker on the test host.
 *   <li><b>Binary mode</b>: activated by the system property {@code useClickhouseBinary=true}
 *       (wired by {@code integ-test/build.gradle}'s {@code startClickhouse} task). Connects to the
 *       local binary started on the fixed ports {@code 18123} (HTTP) and {@code 19000} (TCP) with
 *       user {@code default} and empty password, per {@code
 *       integ-test/src/test/resources/clickhouse/users.xml}.
 * </ul>
 *
 * Subclasses call {@link #chJdbcUrl()}, {@link #chUser()}, and {@link #chPassword()} instead of
 * reaching through {@link #CH}.
 */
public abstract class ClickHouseITBase extends PPLIntegTestCase {

  /**
   * ClickHouseContainer instance in Testcontainers mode; a no-op resource in binary mode (the
   * gradle task already owns the server lifecycle).
   */
  @ClassRule
  public static final TestRule CH =
      "true".equals(System.getProperty("useClickhouseBinary"))
          ? new ExternalResource() {
            // no-op: clickhouse lifecycle is managed by gradle's startClickhouse / stopClickhouse
          }
          : new ClickHouseContainer("clickhouse/clickhouse-server:24.3");

  /**
   * JDBC URL that resolves to the active ClickHouse instance. Always includes {@code compress=0}
   * because the ClickHouse JDBC 0.6.5 default (LZ4) requires extra classpath deps and adds no value
   * for IT correctness.
   */
  protected static String chJdbcUrl() {
    if ("true".equals(System.getProperty("useClickhouseBinary"))) {
      return "jdbc:ch://127.0.0.1:18123/default?compress=0";
    }
    return ((ClickHouseContainer) CH).getJdbcUrl() + "?compress=0";
  }

  protected static String chUser() {
    if ("true".equals(System.getProperty("useClickhouseBinary"))) {
      return "default";
    }
    return ((ClickHouseContainer) CH).getUsername();
  }

  protected static String chPassword() {
    if ("true".equals(System.getProperty("useClickhouseBinary"))) {
      // Must match src/test/resources/clickhouse/users.xml. A non-blank password
      // is required because the connector rejects basic auth with blank password.
      return "test";
    }
    return ((ClickHouseContainer) CH).getPassword();
  }
}
