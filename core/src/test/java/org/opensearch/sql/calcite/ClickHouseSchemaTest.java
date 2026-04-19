/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.datasource.DataSourceService;

public class ClickHouseSchemaTest {
  @Test
  public void schema_exposes_expected_constants_and_empty_subschema_map_when_no_ch_datasources() {
    DataSourceService ds = mock(DataSourceService.class);
    ClickHouseSchema schema = new ClickHouseSchema(ds);
    assertNotNull(schema);
    assertTrue(schema.getSubSchemaMap() == null || schema.getSubSchemaMap().isEmpty());
  }
}
