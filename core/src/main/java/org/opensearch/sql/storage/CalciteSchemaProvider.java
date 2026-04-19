/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage;

import org.apache.calcite.schema.Schema;

/**
 * Storage engines that expose a Calcite Schema (for per-query sub-schema registration) implement
 * this interface. The Calcite engine queries this via {@code instanceof} when building the planner
 * root schema. Keeps {@code core} free of dependencies on connector-specific modules.
 */
public interface CalciteSchemaProvider {
  Schema asCalciteSchema(String datasourceName);
}
