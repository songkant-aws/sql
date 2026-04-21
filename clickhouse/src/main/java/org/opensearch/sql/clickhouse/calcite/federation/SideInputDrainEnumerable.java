/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;

/**
 * Drain helper used by the runtime wrapper around an {@link Enumerable} representing the left
 * side of a federation join. Materialises the left rows into memory (up to {@code threshold+1}
 * rows) and extracts distinct keys from a specified column; throws {@link SideInputBailout} if
 * the left exceeds {@code threshold}.
 */
public final class SideInputDrainEnumerable {
  private SideInputDrainEnumerable() {}

  public record Result(List<Object[]> rows, Object[] distinctKeys) {}

  public static Result drain(Enumerable<Object[]> left, int keyCol, long threshold) {
    List<Object[]> rows = new ArrayList<>();
    LinkedHashSet<Object> keys = new LinkedHashSet<>();
    try (Enumerator<Object[]> en = left.enumerator()) {
      while (en.moveNext()) {
        Object[] row = en.current();
        rows.add(row);
        if (rows.size() > threshold) {
          throw new SideInputBailout(rows.size(), threshold);
        }
        keys.add(row[keyCol]);
      }
    }
    return new Result(rows, keys.toArray());
  }
}
