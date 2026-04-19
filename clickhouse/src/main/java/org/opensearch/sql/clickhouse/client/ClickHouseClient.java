/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.client;

import javax.sql.DataSource;

/** Placeholder; full implementation (HikariCP + rate limits) comes in M2. */
public interface ClickHouseClient extends AutoCloseable {
  DataSource getDataSource();

  @Override
  void close();
}
