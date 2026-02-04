/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.graph.GraphStorage;

/** Storage engine for different storage to provide data access API implementation. */
public interface StorageEngine {

  /** Get {@link Table} from storage engine. */
  Table getTable(DataSourceSchemaName dataSourceSchemaName, String tableName);

  /**
   * Get list of datasource related functions.
   *
   * @return FunctionResolvers of datasource functions.
   */
  default Collection<FunctionResolver> getFunctions() {
    return Collections.emptyList();
  }

  /** Optional graph storage for graph traversal. */
  default Optional<GraphStorage> graphStorage() {
    return Optional.empty();
  }
}
