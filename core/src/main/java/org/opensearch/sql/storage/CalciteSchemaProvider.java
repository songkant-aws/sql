/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Storage engines that expose a Calcite Schema (for per-query sub-schema registration) implement
 * this interface. The Calcite engine queries this via {@code instanceof} when building the planner
 * root schema. Keeps {@code core} free of dependencies on connector-specific modules.
 *
 * <p>{@code parentSchema} is the {@link SchemaPlus} under which the returned sub-schema will be
 * mounted (e.g. the {@code "ClickHouse"} schema). Implementations that build a per-datasource
 * Calcite {@link org.apache.calcite.adapter.jdbc.JdbcConvention} need this reference to construct
 * the convention's expression via {@link org.apache.calcite.schema.Schemas#subSchemaExpression}.
 */
public interface CalciteSchemaProvider {
  Schema asCalciteSchema(String datasourceName, SchemaPlus parentSchema);
}
