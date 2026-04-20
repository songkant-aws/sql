/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect;

public class ClickHouseSqlDialectFederationTest {

  @Test
  public void registryReturnsClickHouseCapabilities() {
    PplFederationDialect caps =
        PplFederationDialectRegistry.forDialect(ClickHouseSqlDialect.INSTANCE);
    assertEquals(10_000L, caps.getInListPushdownThreshold());
    assertTrue(caps.supportsArrayInListParam());
  }
}
