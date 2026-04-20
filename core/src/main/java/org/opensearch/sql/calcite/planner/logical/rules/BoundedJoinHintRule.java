/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.planner.logical.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.hint.RelHint;
import org.immutables.value.Value;

/**
 * HEP rule that attaches a {@code bounded_left} hint (with numeric {@code size} option) to any
 * {@link Join} whose left side has a statically-provable row-count upper bound &le; a ceiling.
 *
 * <p>This rule does <em>not</em> decide whether IN-list pushdown actually fires &mdash; that choice
 * is made in a later Volcano-phase rule against the actual right-side JDBC dialect's threshold.
 * Attaching the hint is cheap and non-destructive.
 */
@Value.Enclosing
public final class BoundedJoinHintRule extends RelRule<BoundedJoinHintRule.Config> {

  /** Ceiling used for hint attachment; should be &ge; the largest dialect-specific threshold. */
  static final long CEILING = 10_000L;

  public static final BoundedJoinHintRule INSTANCE = Config.DEFAULT.toRule();

  private BoundedJoinHintRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    if (join.getHints().stream().anyMatch(h -> h.hintName.equals("bounded_left"))) {
      return;
    }
    // strip() unwraps planner-internal decorators (e.g. HepRelVertex) before analysis.
    Optional<Long> bound = BoundedCardinalityExtractor.extract(join.getLeft().stripped());
    if (bound.isEmpty() || bound.get() > CEILING) {
      return;
    }
    RelHint hint =
        RelHint.builder("bounded_left").hintOption("size", bound.get().toString()).build();
    List<RelHint> newHints = new ArrayList<>(join.getHints());
    newHints.add(hint);
    call.transformTo(join.withHints(newHints));
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        ImmutableBoundedJoinHintRule.Config.builder()
            .build()
            .withOperandSupplier(b -> b.operand(Join.class).anyInputs());

    @Override
    default BoundedJoinHintRule toRule() {
      return new BoundedJoinHintRule(this);
    }
  }
}
