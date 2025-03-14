/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain.hints;

/** Created by Eliran on 5/9/2015. */
public class Hint {
  private final HintType type;
  private final Object[] params;

  public Hint(HintType type, Object[] params) {
    this.type = type;
    this.params = params;
  }

  public HintType getType() {
    return type;
  }

  public Object[] getParams() {
    return params;
  }
}
