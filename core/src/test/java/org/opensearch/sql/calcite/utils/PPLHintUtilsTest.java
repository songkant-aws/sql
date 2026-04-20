/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

public class PPLHintUtilsTest {

  @Test
  public void boundedLeftHintSurvivesJoinWithHints() {
    SchemaPlus root = Frameworks.createRootSchema(true);
    root.add(
        "L",
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory f) {
            return f.builder().add("id", SqlTypeName.BIGINT).build();
          }
        });
    root.add(
        "R",
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory f) {
            return f.builder().add("rid", SqlTypeName.BIGINT).build();
          }
        });
    FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(root).build();
    RelBuilder builder = RelBuilder.create(config);
    builder.getCluster().setHintStrategies(PPLHintUtils.HINT_STRATEGY_TABLE);

    RelNode joinPlan =
        builder
            .scan("L")
            .scan("R")
            .join(
                JoinRelType.INNER,
                builder.equals(builder.field(2, 0, "id"), builder.field(2, 1, "rid")))
            .build();

    org.apache.calcite.rel.core.Join join = (org.apache.calcite.rel.core.Join) joinPlan;
    RelHint hint = RelHint.builder("bounded_left").hintOption("size", "100").build();
    org.apache.calcite.rel.core.Join withHint =
        (org.apache.calcite.rel.core.Join) join.withHints(List.of(hint));
    assertTrue(
        withHint.getHints().stream().anyMatch(h -> h.hintName.equals("bounded_left")),
        "bounded_left hint must survive on Join under HINT_STRATEGY_TABLE");
  }
}
