/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.graph;

import java.util.List;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;

/** Graph search response including schema and results. */
public record GraphSearchResponse(ExecutionEngine.Schema schema, List<ExprValue> results) {}
