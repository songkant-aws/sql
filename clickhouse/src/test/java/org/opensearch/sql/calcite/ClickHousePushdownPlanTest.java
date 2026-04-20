/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.calcite.ClickHouseConvention;

// Expression value is a dummy — these tests exercise rule registration only, not runtime codegen.

/**
 * Plan-level verification that Calcite's JdbcRules are discoverable for a ClickHouse-specific
 * {@link JdbcConvention} subclass, and that {@link JdbcConvention#register(org.apache.calcite.plan.RelOptPlanner)}
 * actually installs rules. These two contracts — surfaced by {@code javap} against
 * {@code calcite-core-1.41.0} — are the only pieces M4.4 needs to work; the rest is Calcite's
 * own auto-registration machinery kicking in during planning.
 *
 * <p>If these tests ever fail (e.g., after a Calcite upgrade), an explicit rule-registration
 * step must be added to {@code QueryService.buildFrameworkConfig()} via
 * {@code Programs.sequence(Programs.standard(), Programs.ofRules(JdbcRules.rules(convention)))}.
 *
 * <p>NOTE: This test lives in the {@code clickhouse} module (not {@code core}) because
 * {@link ClickHouseConvention} is defined there, and {@code clickhouse} already depends on
 * {@code core} — adding the reverse dependency would create a cycle. The package name
 * {@code org.opensearch.sql.calcite} is retained to match the original specification.
 */
public class ClickHousePushdownPlanTest {

  @Test
  public void jdbc_rules_factory_accepts_clickhouse_convention() {
    JdbcConvention convention = ClickHouseConvention.of("my_ch", Expressions.constant("dummy"));
    List<RelOptRule> rules = JdbcRules.rules(convention);
    assertFalse(rules.isEmpty(), "JdbcRules.rules(convention) must return at least one rule");
  }

  @Test
  public void clickhouse_convention_register_adds_rules_to_planner() {
    VolcanoPlanner planner = new VolcanoPlanner();
    int before = planner.getRules().size();
    ClickHouseConvention.of("my_ch", Expressions.constant("dummy")).register(planner);
    int after = planner.getRules().size();
    assertTrue(
        after > before,
        () -> "Convention.register must add rules; before=" + before + ", after=" + after);
  }
}
