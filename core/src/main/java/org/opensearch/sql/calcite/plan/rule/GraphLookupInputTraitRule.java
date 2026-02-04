/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rule;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rel.GraphLookupRel;

/** Ensure graphlookup input is converted to Enumerable convention. */
@Value.Enclosing
public class GraphLookupInputTraitRule extends RelRule<GraphLookupInputTraitRule.Config> {

  protected GraphLookupInputTraitRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    GraphLookupRel graph = call.rel(0);
    RelNode input = graph.getInput();
    if (input == null) {
      return;
    }
    if (EnumerableConvention.INSTANCE.equals(input.getConvention())) {
      return;
    }

    RelNode convertedInput =
        convert(input, input.getTraitSet().replace(EnumerableConvention.INSTANCE));
    if (convertedInput == input) {
      return;
    }

    GraphLookupRel newGraph =
        (GraphLookupRel) graph.copy(graph.getTraitSet(), List.of(convertedInput));
    call.transformTo(newGraph);
  }

  @Value.Immutable
  public interface Config extends RelRule.Config {
    GraphLookupInputTraitRule.Config DEFAULT =
        ImmutableGraphLookupInputTraitRule.Config.builder()
            .build()
            .withOperandSupplier(b -> b.operand(GraphLookupRel.class).anyInputs());

    @Override
    default GraphLookupInputTraitRule toRule() {
      return new GraphLookupInputTraitRule(this);
    }
  }
}
