/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** AST node representing graph lookup command. */
@ToString
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class GraphLookup extends UnresolvedPlan {

  private UnresolvedPlan child;

  private final Map<String, UnresolvedExpression> arguments;

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitGraphLookup(this, context);
  }

  @Override
  public List<? extends Node> getChild() {
    return child == null ? ImmutableList.of() : ImmutableList.of(child);
  }
}
