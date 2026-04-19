/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.exception;

public class ClickHouseConnectionException extends ClickHouseException {
  public ClickHouseConnectionException(String message, Throwable cause) {
    super("CH_CONN_001", message, cause);
  }
}
