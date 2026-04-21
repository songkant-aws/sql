/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.FrameworkConfig;

/** Minimal RelBuilder factory for pushdown-rule unit tests. No real schema needed. */
final class TestRelBuilder {
  private TestRelBuilder() {}

  static RelBuilder newBuilder() {
    SchemaPlus root = Frameworks.createRootSchema(true);
    FrameworkConfig cfg = Frameworks.newConfigBuilder().defaultSchema(root).build();
    return RelBuilder.create(cfg);
  }
}
