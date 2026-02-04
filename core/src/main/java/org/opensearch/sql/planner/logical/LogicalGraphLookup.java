/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.graph.GraphSearchRequest;
import org.opensearch.sql.graph.GraphStorage;

/** Logical plan for graph lookup command. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalGraphLookup extends LogicalPlan {

  private final GraphStorage graphStorage;
  private final GraphSearchRequest request;

  public LogicalGraphLookup(GraphStorage graphStorage, GraphSearchRequest request) {
    super(Collections.emptyList());
    this.graphStorage = graphStorage;
    this.request = request;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitGraphLookup(this, context);
  }
}
