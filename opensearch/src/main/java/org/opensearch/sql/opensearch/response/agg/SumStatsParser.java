/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.Stats;

/**
 * Parses SUM aggregation results using stats aggregation to correctly handle all-null input. When
 * all values are null, OpenSearch sum returns 0.0 which is indistinguishable from a real sum of 0.
 * By using stats aggregation, we can check count==0 to return null instead.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public class SumStatsParser implements MetricParser {

  @Getter private final String name;

  @Override
  public List<Map<String, Object>> parse(Aggregation agg) {
    Stats stats = (Stats) agg;
    Object value = stats.getCount() == 0 ? null : stats.getSum();
    return Collections.singletonList(new HashMap<>(Collections.singletonMap(agg.getName(), value)));
  }
}
