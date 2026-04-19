/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.client;

import javax.sql.DataSource;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;

/** Stub. Real HikariCP-backed impl lands in M2. */
public final class ClickHouseClientFactory {
  private ClickHouseClientFactory() {}

  public static ClickHouseClient create(ClickHouseDataSourceConfig config) {
    return new ClickHouseClient() {
      @Override public DataSource getDataSource() {
        throw new UnsupportedOperationException("ClickHouseClientFactory stub — wired in M2");
      }
      @Override public void close() {}
    };
  }
}
