/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.auth;

import java.util.Properties;
import org.opensearch.sql.clickhouse.config.ClickHouseDataSourceConfig;

public final class ClickHouseAuthProvider {
  private ClickHouseAuthProvider() {}

  public static Properties build(ClickHouseDataSourceConfig cfg) {
    Properties p = new Properties();
    if ("basic".equalsIgnoreCase(cfg.getAuthType())) {
      p.setProperty("user", cfg.getUsername());
      p.setProperty("password", cfg.getPassword());
    } else if ("jwt".equalsIgnoreCase(cfg.getAuthType())) {
      p.setProperty("access_token", cfg.getToken());
    }
    if (cfg.isTlsEnabled()) {
      p.setProperty("ssl", "true");
      p.setProperty("sslmode", "strict");
      if (cfg.getTrustStorePath() != null) {
        p.setProperty("trust_store", cfg.getTrustStorePath());
      }
      if (cfg.getTrustStorePassword() != null) {
        p.setProperty("key_store_password", cfg.getTrustStorePassword());
      }
    }
    if (cfg.getSocketTimeoutMs() > 0) {
      p.setProperty("socket_timeout", Integer.toString(cfg.getSocketTimeoutMs()));
    }
    if (cfg.isCompress()) {
      p.setProperty("compress", "true");
    }
    return p;
  }
}
