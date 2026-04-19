/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.calcite;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.schema.impl.AbstractSchema;

public class ClickHouseConvention extends JdbcConvention {
  private ClickHouseConvention(String name) {
    super(ClickHouseSqlDialect.INSTANCE, new org.apache.calcite.linq4j.tree.ConstantExpression(
        String.class, name), name);
  }

  public static ClickHouseConvention of(String datasourceName) {
    return new ClickHouseConvention("CLICKHOUSE_" + datasourceName + "_" + System.nanoTime());
  }
}
