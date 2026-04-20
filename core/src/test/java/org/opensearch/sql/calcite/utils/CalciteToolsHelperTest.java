/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.planner.logical.rules.BoundedJoinHintRule;

public class CalciteToolsHelperTest {
  @Test
  @SuppressWarnings("unchecked")
  public void boundedJoinHintRuleIsRegistered() throws Exception {
    Field f = CalciteToolsHelper.class.getDeclaredField("hepRuleList");
    f.setAccessible(true);
    List<RelOptRule> rules = (List<RelOptRule>) f.get(null);
    assertTrue(
        rules.stream().anyMatch(r -> r == BoundedJoinHintRule.INSTANCE),
        "BoundedJoinHintRule must be in HEP rule list");
  }
}
