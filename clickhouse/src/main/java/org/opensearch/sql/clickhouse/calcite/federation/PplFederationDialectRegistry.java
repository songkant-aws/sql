/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.calcite.sql.SqlDialect;

/**
 * Side-table lookup mapping {@link SqlDialect} instances to their {@link PplFederationDialect}
 * capabilities. Avoids subclassing core Calcite dialect classes.
 */
public final class PplFederationDialectRegistry {
  private static final ConcurrentMap<SqlDialect, PplFederationDialect> OVERRIDES =
      new ConcurrentHashMap<>();

  private PplFederationDialectRegistry() {}

  public static void register(SqlDialect dialect, PplFederationDialect caps) {
    OVERRIDES.put(dialect, caps);
  }

  public static PplFederationDialect forDialect(SqlDialect dialect) {
    return OVERRIDES.getOrDefault(dialect, PplFederationDialect.DEFAULT);
  }
}
