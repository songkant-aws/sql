/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

/**
 * Federation-specific capabilities a SQL dialect exposes for the PPL IN-list sideways pushdown
 * feature. Dialects opt in by registering an implementation via {@link
 * PplFederationDialectRegistry}.
 */
public interface PplFederationDialect {
  /** Maximum IN-list size (value count) this dialect handles efficiently. */
  long getInListPushdownThreshold();

  /**
   * Whether the dialect supports a single array-typed {@code PreparedStatement} parameter for
   * {@code WHERE col IN (?)} semantics. If false, the optimiser falls back to literal-string
   * substitution for the IN list or disables the optimisation entirely.
   */
  boolean supportsArrayInListParam();

  PplFederationDialect DEFAULT =
      new PplFederationDialect() {
        @Override
        public long getInListPushdownThreshold() {
          return 1_000L;
        }

        @Override
        public boolean supportsArrayInListParam() {
          return false;
        }
      };
}
