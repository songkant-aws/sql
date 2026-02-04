/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rel;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import lombok.ToString;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.GotoExpressionKind;
import org.apache.calcite.linq4j.tree.GotoStatement;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.graph.GraphSearchRequest;
import org.opensearch.sql.graph.GraphSearchResponse;
import org.opensearch.sql.graph.GraphSearchSchema;
import org.opensearch.sql.graph.GraphStorage;

/** Calcite enumerable rel for graph lookup command. */
@ToString
public class GraphLookupRel extends AbstractRelNode implements EnumerableRel {

  private @Nullable RelNode input;
  private final GraphStorage graphStorage;
  private final GraphSearchRequest request;
  private final RelDataType rowType;

  public GraphSearchRequest getRequest() {
    return request;
  }

  public GraphStorage getGraphStorage() {
    return graphStorage;
  }

  public @Nullable RelNode getInput() {
    return input;
  }

  public GraphLookupRel withRequest(GraphSearchRequest newRequest) {
    return new GraphLookupRel(getCluster(), input, graphStorage, newRequest, rowType);
  }

  public GraphLookupRel(
      RelOptCluster cluster,
      @Nullable RelNode input,
      GraphStorage graphStorage,
      GraphSearchRequest request,
      RelDataType rowType) {
    super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE));
    this.input = input;
    this.graphStorage = graphStorage;
    this.request = request;
    this.rowType = rowType;
  }

  @Override
  public RelDataType deriveRowType() {
    return rowType;
  }

  @Override
  public List<RelNode> getInputs() {
    return input == null ? List.of() : List.of(input);
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p) {
    if (ordinalInParent != 0) {
      throw new IllegalArgumentException("GraphLookupRel only supports a single input");
    }
    this.input = p;
  }

  @Override
  public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
    if (input == null) {
      return EnumerableRel.super.passThroughTraits(required);
    }
    RelTraitSet inputTraits = input.getTraitSet().replace(EnumerableConvention.INSTANCE);
    return Pair.of(required, List.of(inputTraits));
  }

  @Override
  public @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      RelTraitSet childTraits, int childId) {
    if (childId != 0) {
      return EnumerableRel.super.deriveTraits(childTraits, childId);
    }
    RelTraitSet traitSet = getTraitSet().replace(EnumerableConvention.INSTANCE);
    return Pair.of(traitSet, List.of(childTraits.replace(EnumerableConvention.INSTANCE)));
  }

  @Override
  public DeriveMode getDeriveMode() {
    return EnumerableRel.super.getDeriveMode();
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());
    Expression scanOperator = implementor.stash(this, GraphLookupRel.class);
    if (input == null) {
      return implementor.result(
          physType, Blocks.toBlock(Expressions.call(scanOperator, "enumerable")));
    }

    Result inputResult = implementor.visitChild(this, 0, (EnumerableRel) input, Prefer.ARRAY);
    BlockStatement rewritten = wrapInputEnumerable(inputResult.block, scanOperator);
    return implementor.result(physType, rewritten);
  }

  public Enumerable<@Nullable Object> enumerable() {
    GraphSearchResponse response = graphStorage.search(request);
    List<ExprValue> results = response.results();
    return toEnumerable(results);
  }

  public Enumerable<@Nullable Object> enumerable(Enumerable<Object[]> inputEnumerable) {
    if (!request.hasStartWithField()) {
      return enumerable();
    }

    String fieldName = request.getStartWithField();
    int fieldIndex = resolveInputFieldIndex(fieldName);
    if (fieldIndex < 0) {
      throw new IllegalStateException(
          String.format("start_with field %s is not present in input", fieldName));
    }

    Set<String> startWiths = new LinkedHashSet<>();
    Enumerator<Object[]> enumerator = inputEnumerable.enumerator();
    try {
      while (enumerator.moveNext()) {
        Object[] row = enumerator.current();
        if (row == null || fieldIndex >= row.length) {
          continue;
        }
        Object value = row[fieldIndex];
        if (value != null) {
          startWiths.add(Objects.toString(value));
        }
      }
    } finally {
      enumerator.close();
    }

    if (startWiths.isEmpty()) {
      return Linq4j.emptyEnumerable();
    }

    GraphSearchRequest resolvedRequest = request.withStartWiths(startWiths);
    GraphSearchResponse response = graphStorage.search(resolvedRequest);
    return toEnumerable(response.results());
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    RelNode newInput = inputs == null || inputs.isEmpty() ? null : inputs.get(0);
    return new GraphLookupRel(getCluster(), newInput, graphStorage, request, rowType);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter writer = pw;
    if (input != null) {
      writer = writer.input("input", input);
    }
    return writer
        .item("from", request.getEdgeIndex())
        .item("start_with", request.hasStartWithField() ? null : request.getStartWith())
        .item("start_with_field", request.hasStartWithField() ? request.getStartWithField() : null)
        .item("max_depth", request.getMaxDepth())
        .item("direction", request.getDirection())
        .item("edge_filter", request.getEdgeFilter())
        .item("node_filter", request.getNodeFilter());
  }

  private int resolveInputFieldIndex(String fieldName) {
    if (input == null) {
      return -1;
    }
    List<RelDataTypeField> fields = input.getRowType().getFieldList();
    for (int i = 0; i < fields.size(); i++) {
      if (fields.get(i).getName().equalsIgnoreCase(fieldName)) {
        return i;
      }
    }
    return -1;
  }

  private Enumerable<@Nullable Object> toEnumerable(List<ExprValue> results) {
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object> enumerator() {
        return new GraphLookupEnumerator(results.iterator());
      }
    };
  }

  private static BlockStatement wrapInputEnumerable(BlockStatement block, Expression scanOperator) {
    List<Statement> statements = block.statements;
    if (statements.isEmpty()) {
      Expression emptyInput = Expressions.call(Linq4j.class, "emptyEnumerable");
      return Blocks.toBlock(Expressions.call(scanOperator, "enumerable", emptyInput));
    }
    Statement last = statements.get(statements.size() - 1);
    if (!(last instanceof GotoStatement gotoStatement)) {
      Expression emptyInput = Expressions.call(Linq4j.class, "emptyEnumerable");
      return Blocks.toBlock(Expressions.call(scanOperator, "enumerable", emptyInput));
    }
    if (gotoStatement.kind != GotoExpressionKind.Return || gotoStatement.expression == null) {
      Expression emptyInput = Expressions.call(Linq4j.class, "emptyEnumerable");
      return Blocks.toBlock(Expressions.call(scanOperator, "enumerable", emptyInput));
    }
    Expression inputExpr = gotoStatement.expression;
    Expression call = Expressions.call(scanOperator, "enumerable", inputExpr);
    BlockBuilder builder = new BlockBuilder();
    for (int i = 0; i < statements.size() - 1; i++) {
      builder.add(statements.get(i));
    }
    builder.add(Expressions.return_(gotoStatement.labelTarget, call));
    return builder.toBlock();
  }

  private static class GraphLookupEnumerator implements Enumerator<Object> {
    private final Iterator<ExprValue> iterator;
    private ExprValue current;

    private GraphLookupEnumerator(Iterator<ExprValue> iterator) {
      this.iterator = iterator;
    }

    @Override
    public Object current() {
      return GraphSearchSchema.fieldNames().stream()
          .map(
              field ->
                  current.tupleValue().getOrDefault(field, ExprNullValue.of()).valueForCalcite())
          .toArray();
    }

    @Override
    public boolean moveNext() {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      }
      return false;
    }

    @Override
    public void reset() {
      throw new UnsupportedOperationException("Graph lookup enumerator reset is unsupported");
    }

    @Override
    public void close() {
      // no-op
    }
  }
}
