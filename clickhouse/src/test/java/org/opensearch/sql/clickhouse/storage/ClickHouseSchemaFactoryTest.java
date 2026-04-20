/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.storage;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.List;
import javax.sql.DataSource;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.config.ClickHouseColumnSpec;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;

public class ClickHouseSchemaFactoryTest {
  @Test
  public void builds_schema_with_sub_schema_per_database_and_table_per_spec() {
    ClickHouseTableSpec.Schema spec =
        new ClickHouseTableSpec.Schema(
            List.of(
                new ClickHouseTableSpec.Database(
                    "analytics",
                    List.of(
                        new ClickHouseTableSpec(
                            "events",
                            List.of(
                                new ClickHouseColumnSpec("id", "Int64", "LONG"),
                                new ClickHouseColumnSpec("name", "String", "STRING")))))));

    DataSource ds = mock(DataSource.class);
    SchemaPlus parent = CalciteSchema.createRootSchema(true, false).plus();
    Schema schema = ClickHouseSchemaFactory.build(parent, "my_ch", ds, spec);
    assertNotNull(schema);
    // Expect a sub-schema "analytics"
    Schema analytics = schema.getSubSchema("analytics");
    assertNotNull(analytics);
    // Expect a table "events"
    org.apache.calcite.schema.Table events = analytics.getTable("events");
    assertNotNull(events);
  }
}
