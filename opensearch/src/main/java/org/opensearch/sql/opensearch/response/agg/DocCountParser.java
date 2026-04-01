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
import org.opensearch.search.aggregations.bucket.filter.Filter;

/**
 * Parser for filter aggregation that extracts the doc_count as the metric value. Used for
 * COUNT(field) to correctly count documents (not array elements) by using a filter aggregation with
 * an exists query instead of value_count.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public class DocCountParser implements MetricParser {

  @Getter private final String name;

  @Override
  public List<Map<String, Object>> parse(Aggregation aggregation) {
    return Collections.singletonList(
        new HashMap<>(
            Collections.singletonMap(aggregation.getName(), ((Filter) aggregation).getDocCount())));
  }
}
