/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import java.util.Iterator;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.graph.GraphSearchRequest;
import org.opensearch.sql.graph.GraphSearchResponse;
import org.opensearch.sql.graph.GraphSearchSchema;
import org.opensearch.sql.graph.GraphStorage;

/** Physical operator for graph lookup. */
@ToString
@EqualsAndHashCode(callSuper = false)
public class GraphLookupOperator extends PhysicalPlan {

  @Getter private final GraphStorage graphStorage;
  @Getter private final GraphSearchRequest request;

  private Iterator<ExprValue> iterator;
  private ExecutionEngine.Schema schema;

  public GraphLookupOperator(GraphStorage graphStorage, GraphSearchRequest request) {
    this.graphStorage = graphStorage;
    this.request = request;
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitGraphLookup(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return List.of();
  }

  @Override
  public void open() {
    super.open();
    GraphSearchResponse response = graphStorage.search(request);
    this.schema = response.schema() == null ? GraphSearchSchema.schema() : response.schema();
    this.iterator = response.results().iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator != null && iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return schema == null ? GraphSearchSchema.schema() : schema;
  }
}
