/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.calcite;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.linq4j.tree.Expression;

/**
 * Per-datasource {@link JdbcConvention}. The {@code expression} passed to the parent constructor
 * must be a Calcite linq4j tree that resolves to the per-datasource {@code JdbcSchema} at codegen
 * time; Calcite's {@code JdbcToEnumerableConverter} wraps it in {@code
 * Schemas.unwrap(expr, DataSource.class)} when emitting the executor. A bare {@code
 * ConstantExpression(String.class, name)} (as used originally in M3) generates {@code
 * "literal".unwrap(DataSource.class)} Java code, which fails to compile — see
 * {@code docs/superpowers/specs/2026-04-20-clickhouse-convention-expression-fix-design.md}.
 *
 * <p>Callers should construct the expression via {@link
 * org.apache.calcite.schema.Schemas#subSchemaExpression} against the {@link
 * org.apache.calcite.schema.SchemaPlus} under which the per-datasource sub-schema is mounted.
 */
public class ClickHouseConvention extends JdbcConvention {
  private ClickHouseConvention(String name, Expression expression) {
    super(ClickHouseSqlDialect.INSTANCE, expression, name);
  }

  public static ClickHouseConvention of(String datasourceName, Expression expression) {
    return new ClickHouseConvention(
        "CLICKHOUSE_" + datasourceName + "_" + System.nanoTime(), expression);
  }
}
