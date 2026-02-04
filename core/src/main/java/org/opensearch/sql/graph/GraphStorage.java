/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.graph;

/** Graph storage abstraction for graph traversal. */
public interface GraphStorage {
  GraphSearchResponse search(GraphSearchRequest request);
}
