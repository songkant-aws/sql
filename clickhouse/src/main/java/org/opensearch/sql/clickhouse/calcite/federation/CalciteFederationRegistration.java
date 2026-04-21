/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import java.util.concurrent.atomic.AtomicBoolean;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;

/** Idempotent Volcano rule registration for federation-side optimisations. */
public final class CalciteFederationRegistration {
  private static final AtomicBoolean DONE = new AtomicBoolean(false);

  private CalciteFederationRegistration() {}

  public static void ensureRegistered() {
    if (DONE.compareAndSet(false, true)) {
      CalciteToolsHelper.registerVolcanoRule(SideInputInListRule.INSTANCE);
    }
  }
}
