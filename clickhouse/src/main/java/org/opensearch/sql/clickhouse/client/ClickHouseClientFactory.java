/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.client;

import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;

/** Factory for creating ClickHouseClient instances backed by HikariCP. */
public final class ClickHouseClientFactory {
  private ClickHouseClientFactory() {}

  public static ClickHouseClient create(ClickHouseDataSourceConfig config) {
    return HikariClickHouseClient.create(config);
  }
}
