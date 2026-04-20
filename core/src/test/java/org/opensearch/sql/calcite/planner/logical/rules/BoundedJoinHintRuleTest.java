/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.planner.logical.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BoundedJoinHintRuleTest {

  private RelBuilder builder;

  @BeforeEach
  public void setUp() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add(
        "L",
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory f) {
            return f.builder().add("id", SqlTypeName.BIGINT).build();
          }
        });
    rootSchema.add(
        "R",
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory f) {
            return f.builder().add("rid", SqlTypeName.BIGINT).add("v", SqlTypeName.DOUBLE).build();
          }
        });
    FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(rootSchema).build();
    builder = RelBuilder.create(config);
    // Register bounded_left as a valid join hint so withHints() is not silently filtered.
    HintStrategyTable hintTable =
        HintStrategyTable.builder().hintStrategy("bounded_left", HintPredicates.JOIN).build();
    builder.getCluster().setHintStrategies(hintTable);
  }

  private Join runRule(RelNode plan) {
    HepProgramBuilder pb = new HepProgramBuilder();
    pb.addRuleInstance(BoundedJoinHintRule.INSTANCE);
    HepPlanner planner = new HepPlanner(pb.build());
    planner.setRoot(plan);
    RelNode out = planner.findBestExp();
    return findJoin(out);
  }

  private static Join findJoin(RelNode n) {
    // Unwrap HepRelVertex (and any other decorator) before checking/traversing.
    RelNode stripped = n.stripped();
    if (stripped instanceof Join) return (Join) stripped;
    for (RelNode c : stripped.getInputs()) {
      Join j = findJoin(c);
      if (j != null) return j;
    }
    return null;
  }

  @Test
  public void boundedLeftGetsHint() {
    RelNode plan =
        builder
            .scan("L")
            .limit(0, 100)
            .scan("R")
            .join(
                org.apache.calcite.rel.core.JoinRelType.INNER,
                builder.equals(builder.field(2, 0, "id"), builder.field(2, 1, "rid")))
            .build();
    Join joined = runRule(plan);
    assertTrue(joined.getHints().stream().anyMatch(h -> h.hintName.equals("bounded_left")));
    assertEquals(
        "100",
        joined.getHints().stream()
            .filter(h -> h.hintName.equals("bounded_left"))
            .findFirst()
            .get()
            .kvOptions
            .get("size"));
  }

  @Test
  public void unboundedLeftHasNoHint() {
    RelNode plan =
        builder
            .scan("L")
            .scan("R")
            .join(
                org.apache.calcite.rel.core.JoinRelType.INNER,
                builder.equals(builder.field(2, 0, "id"), builder.field(2, 1, "rid")))
            .build();
    Join joined = runRule(plan);
    assertTrue(joined.getHints().stream().noneMatch(h -> h.hintName.equals("bounded_left")));
  }

  @Test
  public void boundAboveCeilingIsNotAttached() {
    RelNode plan =
        builder
            .scan("L")
            .limit(0, 20_000)
            .scan("R")
            .join(
                org.apache.calcite.rel.core.JoinRelType.INNER,
                builder.equals(builder.field(2, 0, "id"), builder.field(2, 1, "rid")))
            .build();
    Join joined = runRule(plan);
    assertTrue(joined.getHints().stream().noneMatch(h -> h.hintName.equals("bounded_left")));
  }
}
