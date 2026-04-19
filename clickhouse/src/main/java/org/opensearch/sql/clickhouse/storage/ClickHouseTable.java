/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.clickhouse.config.ClickHouseColumnSpec;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;

/**
 * ClickHouse-backed Table — metadata only. The Calcite pushdown scan path does NOT use this class;
 * it resolves tables via {@code ClickHouseSchema} → per-datasource Calcite sub-schema → JdbcTable.
 * This class exists so the OpenSearch SQL plugin's non-Calcite paths (analyzer symbol lookup,
 * v2 execution fallback) can still see CH tables.
 */
@RequiredArgsConstructor
public class ClickHouseTable implements Table {
  @Getter private final String database;
  @Getter private final String name;
  private final ClickHouseTableSpec spec;

  @Override
  public Map<String, ExprType> getFieldTypes() {
    Map<String, ExprType> types = new LinkedHashMap<>();
    for (ClickHouseColumnSpec c : spec.getColumns()) {
      types.put(c.getName(), ExprCoreType.valueOf(c.getExprType()));
    }
    return types;
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    throw new UnsupportedOperationException(
        "ClickHouse tables are executed via Calcite; the v2 path is not supported.");
  }
}
