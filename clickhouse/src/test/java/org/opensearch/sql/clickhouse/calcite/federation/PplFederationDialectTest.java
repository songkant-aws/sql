/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.calcite.sql.SqlDialect;
import org.junit.jupiter.api.Test;

public class PplFederationDialectTest {

  @Test
  public void defaultsAreConservative() {
    PplFederationDialect d = PplFederationDialect.DEFAULT;
    assertEquals(1_000L, d.getInListPushdownThreshold());
    assertFalse(d.supportsArrayInListParam());
  }

  @Test
  public void registryReturnsDefaultWhenNoOverride() {
    SqlDialect unknown = SqlDialect.DatabaseProduct.UNKNOWN.getDialect();
    PplFederationDialect d = PplFederationDialectRegistry.forDialect(unknown);
    assertEquals(1_000L, d.getInListPushdownThreshold());
  }
}
