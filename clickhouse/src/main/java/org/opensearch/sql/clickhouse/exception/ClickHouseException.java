/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.exception;

import lombok.Getter;

@Getter
public class ClickHouseException extends RuntimeException {
  private final String errorCode;

  public ClickHouseException(String errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public ClickHouseException(String errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }
}
