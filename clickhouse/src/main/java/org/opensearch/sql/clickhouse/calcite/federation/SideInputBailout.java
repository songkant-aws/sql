/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

/**
 * Thrown by the side-input runtime when the drained left side exceeds the configured IN-list
 * threshold. The query executor catches this before any row has been returned to the caller and
 * re-plans without the {@code bounded_left} hint.
 */
public class SideInputBailout extends RuntimeException {
  private final long observedSize;
  private final long threshold;

  public SideInputBailout(long observedSize, long threshold) {
    super(
        "Side-input IN-list pushdown bailout: observed "
            + observedSize
            + " rows exceeded threshold "
            + threshold);
    this.observedSize = observedSize;
    this.threshold = threshold;
  }

  public long getObservedSize() {
    return observedSize;
  }

  public long getThreshold() {
    return threshold;
  }
}
