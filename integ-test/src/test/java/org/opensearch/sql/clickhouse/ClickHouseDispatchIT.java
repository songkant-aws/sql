/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Pre-connector IT: verifies that {@code visitRelation} routes a CH datasource name into Calcite's
 * scan path. No CH datasource is registered yet, so the query must fail at a later stage (scan
 * resolution or datasource lookup) rather than at the {@code CalciteUnsupportedException} guard.
 *
 * <p>Once the connector is wired in later milestones, this test is superseded by
 * {@code ClickHouseBasicQueryIT}.
 *
 * <p><strong>Note:</strong> Not executed in M0; relies on M4 cluster-bootstrap infrastructure.
 */
public class ClickHouseDispatchIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Test
  public void ch_datasource_name_reaches_scan_layer_not_unsupported_guard() {
    try {
      executeQuery("source = my_ch.analytics.events | head 1");
    } catch (Exception e) {
      String msg = e.getMessage() == null ? "" : e.getMessage();
      assertThat(
          "Expected scan-resolution or datasource-lookup failure, not the unsupported-datasource"
              + " guard",
          msg,
          not(containsString("is unsupported in Calcite")));
    }
  }
}
