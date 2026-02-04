/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rule;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rel.GraphLookupRel;
import org.opensearch.sql.graph.GraphSearchRequest;
import org.opensearch.sql.graph.GraphSearchSchema;

/** Push down simple filters into graphlookup. */
@Value.Enclosing
public class GraphLookupFilterPushdownRule extends RelRule<GraphLookupFilterPushdownRule.Config> {

  protected GraphLookupFilterPushdownRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    GraphLookupRel graph = call.rel(1);

    PushdownResult result = PushdownResult.from(filter.getCondition(), graph);
    if (!result.changed) {
      return;
    }

    GraphLookupRel newGraph = graph.withRequest(result.request);
    if (result.residual == null) {
      call.transformTo(newGraph);
    } else {
      RelNode newFilter = filter.copy(filter.getTraitSet(), newGraph, result.residual);
      call.transformTo(newFilter);
    }
  }

  @Value.Immutable
  public interface Config extends RelRule.Config {
    GraphLookupFilterPushdownRule.Config DEFAULT =
        ImmutableGraphLookupFilterPushdownRule.Config.builder()
            .build()
            .withOperandSupplier(
                b ->
                    b.operand(Filter.class)
                        .oneInput(b1 -> b1.operand(GraphLookupRel.class).anyInputs()));

    @Override
    default GraphLookupFilterPushdownRule toRule() {
      return new GraphLookupFilterPushdownRule(this);
    }
  }

  private static class PushdownResult {
    private final GraphSearchRequest request;
    private final RexNode residual;
    private final boolean changed;

    private PushdownResult(GraphSearchRequest request, RexNode residual, boolean changed) {
      this.request = request;
      this.residual = residual;
      this.changed = changed;
    }

    static PushdownResult from(RexNode condition, GraphLookupRel graph) {
      GraphSearchRequest request = graph.getRequest();
      List<RexNode> residualConjuncts = new ArrayList<>();
      boolean changed = false;

      int maxDepth = request.getMaxDepth();
      String edgeFilter = request.getEdgeFilter();
      String nodeFilter = request.getNodeFilter();

      for (RexNode conjunct : RelOptUtil.conjunctions(condition)) {
        Optional<PushdownCondition> pushdown = PushdownCondition.from(conjunct, graph, request);
        if (pushdown.isEmpty()) {
          residualConjuncts.add(conjunct);
          continue;
        }

        PushdownCondition pc = pushdown.get();
        changed = true;
        if (pc.edgeFilter != null) {
          edgeFilter = combine(edgeFilter, pc.edgeFilter);
        }
        if (pc.nodeFilter != null) {
          nodeFilter = combine(nodeFilter, pc.nodeFilter);
        }
        if (pc.maxDepth != null) {
          maxDepth = Math.min(maxDepth, pc.maxDepth);
          if (pc.keepResidual) {
            residualConjuncts.add(conjunct);
          }
        }
      }

      GraphSearchRequest updated = request;
      if (!Objects.equals(edgeFilter, request.getEdgeFilter())) {
        updated = updated.withEdgeFilter(edgeFilter);
      }
      if (!Objects.equals(nodeFilter, request.getNodeFilter())) {
        updated = updated.withNodeFilter(nodeFilter);
      }
      if (maxDepth != request.getMaxDepth()) {
        updated = updated.withMaxDepth(maxDepth);
      }

      RexNode residual =
          residualConjuncts.isEmpty()
              ? null
              : RexUtil.composeConjunction(graph.getCluster().getRexBuilder(), residualConjuncts);
      return new PushdownResult(updated, residual, changed);
    }

    private static String combine(String existing, String next) {
      if (existing == null || existing.isBlank()) {
        return next;
      }
      if (next == null || next.isBlank()) {
        return existing;
      }
      return "(" + existing + ") AND (" + next + ")";
    }
  }

  private static class PushdownCondition {
    private final String edgeFilter;
    private final String nodeFilter;
    private final Integer maxDepth;
    private final boolean keepResidual;

    private PushdownCondition(
        String edgeFilter, String nodeFilter, Integer maxDepth, boolean keepResidual) {
      this.edgeFilter = edgeFilter;
      this.nodeFilter = nodeFilter;
      this.maxDepth = maxDepth;
      this.keepResidual = keepResidual;
    }

    static Optional<PushdownCondition> from(
        RexNode node, GraphLookupRel graph, GraphSearchRequest request) {
      if (!(node instanceof RexCall call)) {
        return Optional.empty();
      }

      SqlKind kind = call.getOperator().getKind();
      if (kind == SqlKind.AND) {
        return Optional.empty();
      }

      RexNode left = call.operands.get(0);
      RexNode right = call.operands.size() > 1 ? call.operands.get(1) : null;

      if (left instanceof RexLiteral && right instanceof RexInputRef) {
        RexNode tmp = left;
        left = right;
        right = tmp;
      }

      if (!(left instanceof RexInputRef inputRef) || !(right instanceof RexLiteral literal)) {
        return Optional.empty();
      }

      String fieldName = graph.getRowType().getFieldList().get(inputRef.getIndex()).getName();

      if (GraphSearchSchema.FIELD_DEPTH.equalsIgnoreCase(fieldName)) {
        return pushDepth(kind, literal);
      }

      if (GraphSearchSchema.FIELD_EDGE_SRC.equalsIgnoreCase(fieldName)) {
        return pushEdgeFilter(kind, literal, request.getConnectFromField());
      }
      if (GraphSearchSchema.FIELD_EDGE_DST.equalsIgnoreCase(fieldName)) {
        return pushEdgeFilter(kind, literal, request.getConnectToField());
      }
      if (GraphSearchSchema.FIELD_EDGE_CHUNK_ID.equalsIgnoreCase(fieldName)) {
        return pushEdgeFilter(kind, literal, request.getEdgeChunkField());
      }

      if (GraphSearchSchema.FIELD_NODE_ID.equalsIgnoreCase(fieldName)) {
        return pushNodeFilter(kind, literal, request, request.getNodeIdField());
      }
      if (GraphSearchSchema.FIELD_NODE_CHUNK_ID.equalsIgnoreCase(fieldName)) {
        return pushNodeFilter(kind, literal, request, request.getNodeChunkField());
      }

      return Optional.empty();
    }

    private static Optional<PushdownCondition> pushDepth(SqlKind kind, RexLiteral literal) {
      Integer value = literalToInt(literal);
      if (value == null) {
        return Optional.empty();
      }
      if (kind == SqlKind.LESS_THAN) {
        return Optional.of(new PushdownCondition(null, null, Math.max(0, value - 1), false));
      }
      if (kind == SqlKind.LESS_THAN_OR_EQUAL) {
        return Optional.of(new PushdownCondition(null, null, value, false));
      }
      if (kind == SqlKind.EQUALS) {
        return Optional.of(new PushdownCondition(null, null, value, true));
      }
      return Optional.empty();
    }

    private static Optional<PushdownCondition> pushEdgeFilter(
        SqlKind kind, RexLiteral literal, String fieldName) {
      if (kind != SqlKind.EQUALS) {
        return Optional.empty();
      }
      String clause = fieldName + ":" + literalToQueryValue(literal);
      return Optional.of(new PushdownCondition(clause, null, null, false));
    }

    private static Optional<PushdownCondition> pushNodeFilter(
        SqlKind kind, RexLiteral literal, GraphSearchRequest request, String fieldName) {
      if (request.getNodeIndex() == null) {
        return Optional.empty();
      }
      if (kind != SqlKind.EQUALS) {
        return Optional.empty();
      }
      String clause = fieldName + ":" + literalToQueryValue(literal);
      return Optional.of(new PushdownCondition(null, clause, null, false));
    }

    private static Integer literalToInt(RexLiteral literal) {
      Object value = literal.getValue2();
      if (value instanceof Number number) {
        return number.intValue();
      }
      return null;
    }

    private static String literalToQueryValue(RexLiteral literal) {
      SqlTypeName type = literal.getTypeName();
      Object value = literal.getValue2();
      if (value == null) {
        return "null";
      }
      if (type == SqlTypeName.CHAR || type == SqlTypeName.VARCHAR) {
        String text = value.toString();
        String escaped = text.replace("\\", "\\\\").replace("\"", "\\\"");
        return "\"" + escaped + "\"";
      }
      return value.toString().toLowerCase(Locale.ROOT);
    }
  }
}
