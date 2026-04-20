/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.After;
import org.junit.Test;

/**
 * Spike: proves whether the ClickHouse JDBC driver accepts {@code PreparedStatement.setArray} with
 * {@code WHERE col IN (?)} (array-param path), or whether we must fall back to inline string-literal
 * substitution (e.g. {@code WHERE id IN (1, 3)}).
 *
 * <p>The test first attempts the array-param path. If the driver throws an exception indicating it
 * does not support {@code setArray} for {@code IN (?)}, the test falls back to the string-literal
 * path and asserts that path works, recording the capability flag in the output.
 *
 * <p>This test stays in the tree as a regression guard for the IN-list pushdown feature.
 *
 * <p>NOTE: This spike lives in {@code integ-test} (not {@code :clickhouse:test}) because the
 * Testcontainers harness ({@link ClickHouseITBase}) is wired up only in {@code integ-test};
 * moving it would require restructuring the testcontainer setup, which is out of scope here.
 */
public class ClickHouseArrayBindSpikeIT extends ClickHouseITBase {

  @After
  public void teardown() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection conn = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = conn.createStatement()) {
      st.execute("DROP TABLE IF EXISTS spike_in");
    } catch (Exception ignored) {
      // best-effort cleanup
    }
  }

  @Test
  public void testArrayParamInListBindsAgainstClickHouse() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());

    try (Connection conn = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p)) {
      // Set up spike table
      try (Statement st = conn.createStatement()) {
        st.execute("DROP TABLE IF EXISTS spike_in");
        st.execute(
            "CREATE TABLE spike_in (id Int64, v Float64) ENGINE = MergeTree ORDER BY id");
        st.execute("INSERT INTO spike_in VALUES (1,10.0),(2,20.0),(3,30.0),(4,40.0)");
      }

      boolean supportsArrayInListParam = false;
      double result = -1.0;
      String chosenPath;

      // --- Attempt 1: array-param path ---
      try {
        java.sql.Array keys = conn.createArrayOf("Int64", new Long[] {1L, 3L});
        try (PreparedStatement ps =
            conn.prepareStatement("SELECT sum(v) FROM spike_in WHERE id IN (?)")) {
          ps.setArray(1, keys);
          try (ResultSet rs = ps.executeQuery()) {
            assertTrue("ResultSet must have a row", rs.next());
            result = rs.getDouble(1);
          }
        }
        supportsArrayInListParam = true;
        chosenPath = "setArray";
      } catch (SQLException e) {
        // Driver does not support setArray for IN (?). Fall back to inline literal.
        System.out.println(
            "[spike] setArray path failed ("
                + e.getClass().getSimpleName()
                + ": "
                + e.getMessage()
                + "); falling back to string-literal IN-list.");
        chosenPath = "literal";

        // --- Fallback: string-literal substitution ---
        // JdbcSideInputFilter will generate: WHERE id IN (1, 3)
        String inList = "1, 3";
        String sql = "SELECT sum(v) FROM spike_in WHERE id IN (" + inList + ")";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          try (ResultSet rs = ps.executeQuery()) {
            assertTrue("ResultSet must have a row (literal path)", rs.next());
            result = rs.getDouble(1);
          }
        }
      }

      System.out.println(
          "[spike] chosenPath="
              + chosenPath
              + " supportsArrayInListParam="
              + supportsArrayInListParam
              + " sum(v) for ids {1,3}="
              + result);

      // ids 1 and 3: v=10.0 + v=30.0 = 40.0
      assertEquals(
          "sum(v) for ids {1,3} must equal 40.0 regardless of IN-list path",
          40.0,
          result,
          0.0001);

      // The ClickHouse JDBC driver must support setArray for IN-list binding.
      // If this assertion fails, the driver has regressed and downstream tasks
      // relying on the array-param approach must be reconsidered.
      assertTrue(
          "ClickHouse JDBC driver must support setArray for IN-list (supportsArrayInListParam"
              + " must be true); if false, the driver regressed and the array-param approach"
              + " must be reconsidered",
          supportsArrayInListParam);
    }
  }
}
