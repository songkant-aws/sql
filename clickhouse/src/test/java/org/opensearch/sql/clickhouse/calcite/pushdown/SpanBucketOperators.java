/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.pushdown;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Test-only reference to the PPL SPAN_BUCKET operator name. We don't need the full UDF
 * registration — we just need a SqlOperator instance the rule can recognise by name.
 */
final class SpanBucketOperators {
  static final SqlOperator SPAN_BUCKET =
      new SqlFunction(
          "SPAN_BUCKET",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0,
          null,
          OperandTypes.VARIADIC,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  private SpanBucketOperators() {}
}
