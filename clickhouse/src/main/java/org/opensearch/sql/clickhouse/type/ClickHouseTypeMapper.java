/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.type;

import java.util.Map;
import java.util.Set;
import org.opensearch.sql.clickhouse.exception.ClickHouseSchemaException;
import org.opensearch.sql.data.type.ExprCoreType;

public final class ClickHouseTypeMapper {
  private ClickHouseTypeMapper() {}

  /** Allowed (ch_type_prefix → allowed ExprCoreType set). DateTime64(p) matched by prefix. */
  private static final Map<String, Set<ExprCoreType>> ALLOWED = Map.ofEntries(
      Map.entry("Int8", Set.of(ExprCoreType.INTEGER)),
      Map.entry("Int16", Set.of(ExprCoreType.INTEGER)),
      Map.entry("Int32", Set.of(ExprCoreType.INTEGER)),
      Map.entry("Int64", Set.of(ExprCoreType.LONG)),
      Map.entry("UInt8", Set.of(ExprCoreType.BOOLEAN, ExprCoreType.SHORT)),
      Map.entry("UInt16", Set.of(ExprCoreType.LONG)),
      Map.entry("UInt32", Set.of(ExprCoreType.LONG)),
      Map.entry("Float32", Set.of(ExprCoreType.FLOAT)),
      Map.entry("Float64", Set.of(ExprCoreType.DOUBLE)),
      Map.entry("String", Set.of(ExprCoreType.STRING)),
      Map.entry("FixedString", Set.of(ExprCoreType.STRING)),
      Map.entry("Bool", Set.of(ExprCoreType.BOOLEAN)),
      Map.entry("Date", Set.of(ExprCoreType.DATE)),
      Map.entry("Date32", Set.of(ExprCoreType.DATE)),
      Map.entry("DateTime", Set.of(ExprCoreType.TIMESTAMP)),
      Map.entry("DateTime64", Set.of(ExprCoreType.TIMESTAMP)));

  /** Explicitly rejected prefixes — present here for clearer error messages. */
  private static final Set<String> REJECTED = Set.of(
      "UInt64", "Int128", "UInt128", "Int256", "UInt256",
      "Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256",
      "Enum8", "Enum16", "LowCardinality", "Nullable",
      "Array", "Tuple", "Map", "UUID", "IPv4", "IPv6",
      "Nested", "AggregateFunction");

  public static ExprCoreType resolve(String chType, String declaredExprType) {
    String prefix = extractPrefix(chType);
    if (REJECTED.contains(prefix)) {
      throw new ClickHouseSchemaException(
          "ClickHouse type " + chType + " is not supported. Cast server-side or choose a different column.");
    }
    Set<ExprCoreType> allowed = ALLOWED.get(prefix);
    if (allowed == null) {
      throw new ClickHouseSchemaException("Unknown ClickHouse type: " + chType);
    }
    ExprCoreType declared;
    try {
      declared = ExprCoreType.valueOf(declaredExprType);
    } catch (IllegalArgumentException e) {
      throw new ClickHouseSchemaException("Unknown expr_type: " + declaredExprType);
    }
    if (!allowed.contains(declared)) {
      throw new ClickHouseSchemaException(
          "expr_type " + declaredExprType + " not allowed for ClickHouse type " + chType
              + "; allowed: " + allowed);
    }
    return declared;
  }

  /** "DateTime64(3)" → "DateTime64"; "FixedString(16)" → "FixedString"; "Int32" → "Int32". */
  private static String extractPrefix(String chType) {
    int paren = chType.indexOf('(');
    return paren < 0 ? chType : chType.substring(0, paren);
  }
}
