/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.testcontainers.DockerClientFactory;
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
 * <p>On CI hosts without Docker (e.g. Windows GitHub-hosted runners) AND without binary mode, all
 * subclass tests are skipped via {@code Assume} rather than failing — this lets the surrounding
 * {@code :integ-test:integTest} job stay green instead of failing on every {@code ClickHouse*IT}'s
 * {@code @ClassRule} init.
 *
 * <p>Subclasses call {@link #chJdbcUrl()}, {@link #chUser()}, and {@link #chPassword()} instead of
 * reaching through {@link #CH}.
 */
public abstract class ClickHouseITBase extends PPLIntegTestCase {

  /** True when running with the external binary-mode server managed by gradle. */
  private static final boolean BINARY_MODE =
      "true".equals(System.getProperty("useClickhouseBinary"));

  /**
   * True when neither binary mode nor a usable Docker daemon is available. In that case the {@link
   * #CH} class rule is a no-op and {@link #skipIfNoClickhouse()} aborts every test via {@code
   * Assume}. This is the only configuration that yields a SKIP rather than a FAIL, and it is
   * intentional — a green-but-skipped state tells CI "there is nothing to test here" instead of
   * "something regressed."
   *
   * <p>NB: the skip check MUST run per-test ({@code @Before}), not per-class
   * ({@code @BeforeClass}). Aborting via {@code Assume} in {@code @BeforeClass} prevents the
   * framework's per-test {@code initClient} from populating the static REST client, but JUnit still
   * walks the class hierarchy and executes every {@code @AfterClass} — including the parent's
   * {@code cleanUpIndices} which dereferences the null client and turns every intended SKIP into a
   * FAIL wrapped as {@code TestCouldNotBeSkippedException}. Running {@code Assume} in
   * {@code @Before} lets {@code initClient} run first, so cleanup works regardless of skip state.
   */
  private static final boolean CLICKHOUSE_UNAVAILABLE = !BINARY_MODE && !isDockerAvailable();

  private static boolean isDockerAvailable() {
    try {
      // Testcontainers' own liveness probe; returns false (does not throw) when the daemon
      // is absent or unresponsive on this host.
      return DockerClientFactory.instance().isDockerAvailable();
    } catch (Throwable ignored) {
      return false;
    }
  }

  /**
   * ClickHouseContainer instance in Testcontainers mode; a no-op resource in binary mode (the
   * gradle task already owns the server lifecycle) OR when Docker is unavailable (tests will be
   * skipped in {@link #skipIfNoClickhouse()}).
   */
  @ClassRule
  public static final TestRule CH =
      (BINARY_MODE || CLICKHOUSE_UNAVAILABLE)
          ? new ExternalResource() {
            // no-op: in binary mode the gradle task owns lifecycle; in the unavailable case
            // every @Test is skipped before it runs so we never need a real server.
          }
          : new ClickHouseContainer("clickhouse/clickhouse-server:24.3")
              .withPassword("test")
              // clickhouse-jdbc 0.6.5 defaults to LZ4 compression; suppress it so the
              // internal Testcontainers JDBC health-check does not need LZ4 on the classpath.
              .withUrlParam("compress", "0");

  @Before
  public void skipIfNoClickhouse() {
    org.junit.Assume.assumeFalse(
        "ClickHouse is unavailable on this host (no Docker daemon and useClickhouseBinary=false);"
            + " skipping ClickHouse IT",
        CLICKHOUSE_UNAVAILABLE);
  }

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
