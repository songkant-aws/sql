/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Set;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.CalciteSchemaProvider;
import org.opensearch.sql.storage.StorageEngine;

public class ClickHouseSchemaTest {

  @Test
  public void null_data_source_service_returns_empty_sub_schema_map() {
    ClickHouseSchema schema = new ClickHouseSchema(null);
    assertNotNull(schema.getSubSchemaMap());
    assertTrue(schema.getSubSchemaMap().isEmpty());
  }

  @Test
  public void no_registered_ch_datasources_returns_empty_sub_schema_map() {
    DataSourceService dss = mock(DataSourceService.class);
    DataSourceMetadata openSearchMd = mock(DataSourceMetadata.class);
    when(openSearchMd.getConnector()).thenReturn(DataSourceType.OPENSEARCH);
    when(dss.getDataSourceMetadata(true)).thenReturn(Set.of(openSearchMd));

    SchemaPlus root = CalciteSchema.createRootSchema(true, false).plus();
    ClickHouseSchema schema = ClickHouseSchema.install(root, dss);
    assertTrue(schema.getSubSchemaMap().isEmpty());
  }

  @Test
  public void sub_schema_map_contains_one_entry_per_registered_ch_datasource() {
    DataSourceService dss = mock(DataSourceService.class);
    DataSourceMetadata chMd = mock(DataSourceMetadata.class);
    when(chMd.getName()).thenReturn("my_ch");
    when(chMd.getConnector()).thenReturn(DataSourceType.CLICKHOUSE);
    when(dss.getDataSourceMetadata(true)).thenReturn(Set.of(chMd));

    Schema chSubSchema = mock(Schema.class);
    StorageEngine engine =
        mock(StorageEngine.class, withSettings().extraInterfaces(CalciteSchemaProvider.class));
    // Match any SchemaPlus — the instance is produced inside install().
    when(((CalciteSchemaProvider) engine).asCalciteSchema(eq("my_ch"), any(SchemaPlus.class)))
        .thenReturn(chSubSchema);

    DataSource ds = mock(DataSource.class);
    when(ds.getStorageEngine()).thenReturn(engine);
    when(dss.getDataSource("my_ch")).thenReturn(ds);

    SchemaPlus root = CalciteSchema.createRootSchema(true, false).plus();
    ClickHouseSchema schema = ClickHouseSchema.install(root, dss);

    assertEquals(1, schema.getSubSchemaMap().size());
    assertTrue(schema.getSubSchemaMap().containsKey("my_ch"));
    assertEquals(chSubSchema, schema.getSubSchemaMap().get("my_ch"));
  }
}
