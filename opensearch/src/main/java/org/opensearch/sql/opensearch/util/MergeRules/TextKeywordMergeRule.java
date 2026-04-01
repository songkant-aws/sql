/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util.MergeRules;

import java.util.Map;
import java.util.Set;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;

/**
 * Deterministic merge rule for text/keyword type conflicts across indices. When one index maps a
 * field as TEXT (or MATCH_ONLY_TEXT) and another as KEYWORD, prefer TEXT because it is more general
 * (supports both full-text search and keyword-style queries via the .keyword sub-field).
 */
public class TextKeywordMergeRule implements MergeRule {

  private static final Set<MappingType> TEXT_TYPES =
      Set.of(MappingType.Text, MappingType.MatchOnlyText);

  @Override
  public boolean isMatch(OpenSearchDataType source, OpenSearchDataType target) {
    if (source == null || target == null) {
      return false;
    }
    MappingType srcType = source.getMappingType();
    MappingType tgtType = target.getMappingType();
    if (srcType == null || tgtType == null) {
      return false;
    }
    return (TEXT_TYPES.contains(srcType) && tgtType == MappingType.Keyword)
        || (srcType == MappingType.Keyword && TEXT_TYPES.contains(tgtType));
  }

  @Override
  public void mergeInto(
      String key, OpenSearchDataType source, Map<String, OpenSearchDataType> target) {
    MappingType srcType = source.getMappingType();
    if (TEXT_TYPES.contains(srcType)) {
      target.put(key, source);
    }
    // Otherwise target already has a text type — keep it.
  }
}
