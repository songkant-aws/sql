/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;

import java.io.PrintWriter;
import java.io.StringWriter;

/** Test helper: explain a RelNode as plain string for substring assertions. */
final class RelNodeToString {
  private RelNodeToString() {}

  static String explain(RelNode rel) {
    StringWriter sw = new StringWriter();
    RelWriter rw = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, false);
    rel.explain(rw);
    return sw.toString();
  }
}
