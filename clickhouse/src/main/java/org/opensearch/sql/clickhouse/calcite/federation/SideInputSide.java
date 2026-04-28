/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

/**
 * Indicates which child of a {@link org.apache.calcite.rel.core.Join} is the JDBC input that
 * receives the runtime {@code WHERE key IN (?)} filter, and by implication which child is the
 * bounded side that gets drained to populate the IN-list.
 *
 * <p>Calcite's Volcano planner can commute join inputs freely (via {@code JoinCommuteRule} and
 * cost-based ordering), so the JDBC input is not stably on the right child. Detection happens at
 * rule time / bind time by probing both children.
 */
public enum SideInputSide {
  /** JDBC input is the join's left child; the right child is drained. */
  LEFT_IS_JDBC,
  /** JDBC input is the join's right child; the left child is drained. */
  RIGHT_IS_JDBC
}
