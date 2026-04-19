/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Map;
import java.util.Set;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.CalciteSchemaProvider;
import org.opensearch.sql.storage.StorageEngine;

public class ClickHousePlanSmokeTest {

  @Test
  public void ch_sub_schema_is_visible_to_rel_builder() {
    // 1. Mock DataSourceService so ClickHouseSchema discovers one CH datasource "my_ch".
    DataSourceService dss = mock(DataSourceService.class);
    DataSourceMetadata md = mock(DataSourceMetadata.class);
    when(md.getName()).thenReturn("my_ch");
    when(md.getConnector()).thenReturn(DataSourceType.CLICKHOUSE);
    when(dss.getDataSourceMetadata(true)).thenReturn(Set.of(md));

    // 2. Build a synthetic CH sub-schema with one database "analytics" and one table "events".
    SchemaPlus chSchemaRoot = CalciteSchema.createRootSchema(true, false).plus();
    chSchemaRoot.add(
        "analytics",
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of(
                "events",
                new AbstractTable() {
                  @Override
                  public RelDataType getRowType(RelDataTypeFactory tf) {
                    return tf.builder().add("id", tf.createSqlType(SqlTypeName.BIGINT)).build();
                  }
                });
          }
        });

    // 3. Wire the synthetic sub-schema into a mocked CalciteSchemaProvider-bearing StorageEngine.
    StorageEngine engine =
        mock(StorageEngine.class, withSettings().extraInterfaces(CalciteSchemaProvider.class));
    when(((CalciteSchemaProvider) engine).asCalciteSchema("my_ch")).thenReturn(chSchemaRoot);

    DataSource ds = mock(DataSource.class);
    when(ds.getStorageEngine()).thenReturn(engine);
    when(dss.getDataSource("my_ch")).thenReturn(ds);

    // 4. Register ClickHouseSchema under a planning root.
    SchemaPlus root = CalciteSchema.createRootSchema(true, false).plus();
    root.add(ClickHouseSchema.CLICKHOUSE_SCHEMA_NAME, new ClickHouseSchema(dss));

    FrameworkConfig cfg = Frameworks.newConfigBuilder().defaultSchema(root).build();
    RelBuilder rb = RelBuilder.create(cfg);

    // 5. RelBuilder.scan("ClickHouse","my_ch","analytics","events") must resolve the table.
    RelNode scan =
        rb.scan(ClickHouseSchema.CLICKHOUSE_SCHEMA_NAME, "my_ch", "analytics", "events").build();

    String plan = RelOptUtil.toString(scan);
    assertTrue(plan.contains("events"), () -> "Plan should reference events table:\n" + plan);
  }

  @Test
  public void schema_instance_is_ignored_when_data_source_service_is_null() {
    // Direct scan under a manually-registered ClickHouseSchema with no backing service is a
    // no-op (empty sub-schema map). Ensures the class is safe to instantiate in Framework setup.
    SchemaPlus root = CalciteSchema.createRootSchema(true, false).plus();
    Schema schema = new ClickHouseSchema(null);
    root.add(ClickHouseSchema.CLICKHOUSE_SCHEMA_NAME, schema);
    // No assertion needed — absence of exception is the signal.
  }
}
