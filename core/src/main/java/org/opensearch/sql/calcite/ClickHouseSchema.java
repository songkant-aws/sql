/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.sql.datasource.DataSourceService;

/**
 * Calcite {@link AbstractSchema} that exposes one Calcite sub-schema per registered ClickHouse
 * datasource. Sub-schema map is resolved lazily on each access; individual sub-schemas are obtained
 * from each CH datasource's {@code StorageEngine} via a narrow interface so {@code core} stays
 * decoupled from the {@code clickhouse} module.
 *
 * <p>This stub returns an empty sub-schema map. Wiring to CH datasources is added in a later
 * milestone.
 */
@Getter
@AllArgsConstructor
public class ClickHouseSchema extends AbstractSchema {
  public static final String CLICKHOUSE_SCHEMA_NAME = "ClickHouse";

  private final DataSourceService dataSourceService;

  @Override
  protected Map<String, Schema> getSubSchemaMap() {
    // M0: stub — real lookup added in M4.
    return Map.of();
  }
}
