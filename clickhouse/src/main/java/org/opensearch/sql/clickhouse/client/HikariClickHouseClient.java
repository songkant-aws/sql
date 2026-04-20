/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.client;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.opensearch.sql.clickhouse.auth.ClickHouseAuthProvider;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;

public class HikariClickHouseClient implements ClickHouseClient {
  private final HikariDataSource hikari;

  private HikariClickHouseClient(HikariDataSource hikari) {
    this.hikari = hikari;
  }

  public static HikariClickHouseClient create(ClickHouseDataSourceConfig cfg) {
    HikariConfig hc = new HikariConfig();
    // OpenSearch plugins run under an isolated classloader that does not load
    // ServiceLoader entries for JDBC drivers, so `DriverManager.getDriver(url)`
    // returns "No suitable driver" for `jdbc:ch://...`. Setting driverClassName
    // forces Hikari to Class.forName the driver instead of going through
    // DriverManager.
    hc.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
    hc.setJdbcUrl(cfg.getUri());
    hc.setMaximumPoolSize(cfg.getPoolMaxSize());
    hc.setMinimumIdle(cfg.getPoolMinIdle());
    hc.setConnectionTimeout(5_000);
    hc.setIdleTimeout(10L * 60_000L);
    hc.setMaxLifetime(30L * 60_000L);
    hc.setLeakDetectionThreshold(60_000);
    hc.setConnectionTestQuery("SELECT 1");
    hc.setPoolName("clickhouse-" + Integer.toHexString(System.identityHashCode(cfg)));
    hc.setDataSourceProperties(ClickHouseAuthProvider.build(cfg));
    // Defer connection probing until explicit use; the storage factory will run its own
    // SELECT 1 probe at registration (M2.5), so Hikari's fail-fast is redundant.
    hc.setInitializationFailTimeout(-1);
    return new HikariClickHouseClient(new HikariDataSource(hc));
  }

  @Override
  public DataSource getDataSource() {
    return hikari;
  }

  @Override
  public void close() {
    hikari.close();
  }
}
