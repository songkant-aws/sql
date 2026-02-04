/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.graph;

import java.util.Locale;

/** Direction for graph traversal. */
public enum GraphDirection {
  OUT,
  IN,
  BOTH;

  public static GraphDirection fromString(String value) {
    if (value == null) {
      return OUT;
    }
    return switch (value.toLowerCase(Locale.ROOT)) {
      case "out", "outbound" -> OUT;
      case "in", "inbound" -> IN;
      case "both" -> BOTH;
      default -> throw new IllegalArgumentException("Unsupported graph direction: " + value);
    };
  }
}
