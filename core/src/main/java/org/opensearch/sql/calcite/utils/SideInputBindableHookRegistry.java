/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import org.apache.calcite.prepare.Prepare.PreparedResult;
import org.apache.calcite.rel.RelRoot;

/**
 * Allows optional modules (e.g. clickhouse) to wrap the prepared result returned by the OpenSearch
 * Calcite prepare path, injecting runtime behaviour without a hard dependency from core &#x2192;
 * clickhouse.
 */
public final class SideInputBindableHookRegistry {
  private static final CopyOnWriteArrayList<BiFunction<PreparedResult, RelRoot, PreparedResult>>
      HOOKS = new CopyOnWriteArrayList<>();

  private SideInputBindableHookRegistry() {}

  public static void register(BiFunction<PreparedResult, RelRoot, PreparedResult> hook) {
    HOOKS.add(hook);
  }

  public static PreparedResult maybeWrap(PreparedResult prepared, RelRoot root) {
    PreparedResult cur = prepared;
    for (BiFunction<PreparedResult, RelRoot, PreparedResult> h : HOOKS) {
      cur = h.apply(cur, root);
    }
    return cur;
  }
}
