/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.junit.jupiter.api.Test;

/** Pins the {@code registerHepRule} hook contract — mirrors {@code registerVolcanoRule}. */
public class CalciteToolsHelperHepHookTest {

  @Test
  public void register_hep_rule_is_idempotent_and_visible() {
    RelOptRule rule = FilterMergeRule.Config.DEFAULT.toRule();
    int before = CalciteToolsHelper.getExtraHepRules().size();
    CalciteToolsHelper.registerHepRule(rule);
    CalciteToolsHelper.registerHepRule(rule); // idempotent
    int after = CalciteToolsHelper.getExtraHepRules().size();

    assertEquals(before + 1, after, "registerHepRule must be idempotent");
    assertTrue(
        CalciteToolsHelper.getExtraHepRules().contains(rule),
        "registered rule must be visible via getExtraHepRules()");
  }
}
