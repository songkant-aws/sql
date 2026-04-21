/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

/**
 * Rule-level smoke test. Full plan-graph validation happens in ClickHouseFederationIT. Here we
 * only verify the rule class loads and its INSTANCE is non-null.
 */
class SideInputInListRuleTest {
  @Test
  void ruleInstanceExists() {
    assertNotNull(SideInputInListRule.INSTANCE);
  }
}
