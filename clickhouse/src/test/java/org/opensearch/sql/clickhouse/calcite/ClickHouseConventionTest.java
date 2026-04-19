/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.junit.jupiter.api.Test;

public class ClickHouseConventionTest {
  @Test
  public void convention_exposes_dialect_and_unique_name() {
    ClickHouseConvention c1 = ClickHouseConvention.of("ds_a");
    ClickHouseConvention c2 = ClickHouseConvention.of("ds_b");
    assertNotNull(c1);
    assertNotNull(c2);
    assertEquals(ClickHouseSqlDialect.INSTANCE, c1.dialect);
    org.junit.jupiter.api.Assertions.assertNotEquals(c1.getName(), c2.getName());
    org.junit.jupiter.api.Assertions.assertTrue(c1 instanceof JdbcConvention);
  }
}
