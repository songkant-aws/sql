/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.planner.logical.rules;

import java.math.BigDecimal;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexLiteral;

/**
 * Static, structurally-provable upper-bound extractor for {@link RelNode} row counts.
 *
 * <p>Returns {@code Optional.empty()} whenever no bound can be proven from the plan alone — never
 * an estimate. Used by the IN-list sideways pushdown rule to decide whether the left side of a join
 * is small enough to drain and materialise as a {@code WHERE key IN (...)} filter against a JDBC
 * source.
 */
public final class BoundedCardinalityExtractor {
  private BoundedCardinalityExtractor() {}

  public static Optional<Long> extract(RelNode node) {
    if (node instanceof Sort sort) {
      Optional<Long> fetchBound =
          sort.fetch instanceof RexLiteral
              ? Optional.of(((BigDecimal) ((RexLiteral) sort.fetch).getValue4()).longValueExact())
              : Optional.empty();
      Optional<Long> childBound = extract(sort.getInput());
      if (fetchBound.isPresent() && childBound.isPresent()) {
        return Optional.of(Math.min(fetchBound.get(), childBound.get()));
      }
      return fetchBound.isPresent() ? fetchBound : childBound;
    }
    if (node instanceof Project) {
      return extract(((Project) node).getInput());
    }
    if (node instanceof Calc) {
      return extract(((Calc) node).getInput());
    }
    if (node instanceof Filter) {
      // Filter is transparent for v1; PK-equality extraction is a v2 extension.
      return extract(((Filter) node).getInput());
    }
    if (node instanceof Values values) {
      return Optional.of((long) values.tuples.size());
    }
    if (node instanceof Aggregate
        || node instanceof Join
        || node instanceof Union
        || node instanceof TableScan) {
      return Optional.empty();
    }
    return Optional.empty();
  }
}
