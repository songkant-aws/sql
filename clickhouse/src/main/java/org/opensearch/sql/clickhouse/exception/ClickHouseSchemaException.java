/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.exception;

public class ClickHouseSchemaException extends ClickHouseException {
  public ClickHouseSchemaException(String message) {
    super("CH_SCHEMA_001", message);
  }
}
