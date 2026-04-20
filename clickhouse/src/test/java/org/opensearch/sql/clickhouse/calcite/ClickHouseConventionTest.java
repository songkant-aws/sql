/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.calcite;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import java.util.List;
import javax.sql.DataSource;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.clickhouse.config.ClickHouseColumnSpec;
import org.opensearch.sql.clickhouse.config.ClickHouseTableSpec;
import org.opensearch.sql.clickhouse.storage.ClickHouseSchemaFactory;

/**
 * Unit-level proof that the convention's expression resolves through a live {@link SchemaPlus}
 * tree back to the {@link DataSource} we registered. Reproduces the chain that Calcite's {@code
 * JdbcToEnumerableConverter} uses at codegen time: {@code
 * parent.getSubSchema(name).unwrap(JdbcSchema.class).unwrap(DataSource.class)}.
 */
public class ClickHouseConventionTest {

  @Test
  public void sub_schema_unwraps_to_jdbc_schema_and_data_source() {
    DataSource originalDs = mock(DataSource.class);

    // Build a CH schema with TWO databases. Two is important: it lets us pin the invariant
    // "the FIRST database's inner JdbcSchema is the one exposed at the outer (ds) level".
    // With a single database the test would still pass if the factory picked the last or
    // any arbitrary database.
    ClickHouseTableSpec.Schema spec =
        new ClickHouseTableSpec.Schema(
            List.of(
                new ClickHouseTableSpec.Database(
                    "db1",
                    List.of(
                        new ClickHouseTableSpec(
                            "t1",
                            List.of(new ClickHouseColumnSpec("c1", "Int64", "LONG"))))),
                new ClickHouseTableSpec.Database(
                    "db2",
                    List.of(
                        new ClickHouseTableSpec(
                            "t2",
                            List.of(new ClickHouseColumnSpec("c2", "Int64", "LONG")))))));

    // Mount under a synthetic Calcite root, mirroring QueryService.buildFrameworkConfig +
    // ClickHouseSchema.install + provider.asCalciteSchema(name, parent).
    SchemaPlus root = CalciteSchema.createRootSchema(true, false).plus();
    SchemaPlus chParent =
        root.add("ClickHouse", new org.apache.calcite.schema.impl.AbstractSchema() {});
    Schema perDatasource = ClickHouseSchemaFactory.build(chParent, "ds1", originalDs, spec);
    chParent.add("ds1", perDatasource);

    // Walk the chain Calcite's codegen walks.
    SchemaPlus resolved = chParent.getSubSchema("ds1");
    assertNotNull(resolved, "parent.getSubSchema(name) returned null");

    JdbcSchema asJdbc = resolved.unwrap(JdbcSchema.class);
    assertNotNull(asJdbc, "wrapper did not expose inner JdbcSchema via unwrap(JdbcSchema.class)");

    DataSource got = asJdbc.unwrap(DataSource.class);
    assertNotNull(got, "JdbcSchema.unwrap(DataSource.class) returned null");
    assertSame(originalDs, got, "resolved DataSource differs from the one we registered");

    // Pin the invariant: the JdbcSchema exposed at the ds1 level is THE SAME instance as the
    // FIRST database's inner JdbcSchema (db1), and is NOT the second database's (db2). This
    // guards against the factory silently switching to LAST or an arbitrary database.
    SchemaPlus db1Resolved = resolved.getSubSchema("db1");
    assertNotNull(db1Resolved, "sub-schema 'db1' was not mounted");
    JdbcSchema db1Jdbc = db1Resolved.unwrap(JdbcSchema.class);
    assertNotNull(db1Jdbc, "db1 did not expose inner JdbcSchema");
    assertSame(
        db1Jdbc,
        asJdbc,
        "outer ds1 JdbcSchema must be the SAME instance as db1's (first database) JdbcSchema");

    SchemaPlus db2Resolved = resolved.getSubSchema("db2");
    assertNotNull(db2Resolved, "sub-schema 'db2' was not mounted");
    JdbcSchema db2Jdbc = db2Resolved.unwrap(JdbcSchema.class);
    assertNotNull(db2Jdbc, "db2 did not expose inner JdbcSchema");
    assertNotSame(
        db2Jdbc,
        asJdbc,
        "outer ds1 JdbcSchema must NOT be db2's (second database) JdbcSchema — the contract"
            + " is that the first database's delegate is exposed at the outer level");
  }
}
