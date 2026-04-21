/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

/**
 * End-to-end IT for PPL federation IN-list pushdown: a bounded OpenSearch left side is drained at
 * runtime and its distinct join keys are bound into a {@code WHERE key IN (?)} on the ClickHouse
 * right side via {@link org.opensearch.sql.clickhouse.calcite.federation.SideInputInListRule} and
 * {@link org.opensearch.sql.clickhouse.calcite.federation.SideInputBindableWrapper}.
 *
 * <p>Extends {@link ClickHouseITBase} directly (rather than {@link ClickHousePushdownIT}) to avoid
 * re-running the parent's entire {@code @Test} battery every time this IT runs — JUnit 4 runs
 * inherited {@code @Test} methods. The environment setup needed here (testcontainer lifecycle /
 * {@code chJdbcUrl()} / etc.) already lives in {@link ClickHouseITBase}.
 */
public class ClickHouseFederationIT extends ClickHouseITBase {

  private static final String DS_NAME = "ch";
  private static final String OS_INDEX = "fed_docs";
  private static final String CH_DATABASE = "fed";
  private static final String CH_TABLE = "fed_events";

  @Before
  public void setup() throws Exception {
    super.init();
    enableCalcite();
    seedClickHouse();
    seedOpenSearch();
    registerDatasource();
  }

  @After
  public void teardown() throws Exception {
    try {
      client().performRequest(new Request("DELETE", "/_plugins/_query/_datasources/" + DS_NAME));
    } catch (Exception ignored) {
      // best-effort cleanup
    }
    try {
      client().performRequest(new Request("DELETE", "/" + OS_INDEX));
    } catch (Exception ignored) {
      // best-effort cleanup
    }
  }

  private void seedClickHouse() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("CREATE DATABASE IF NOT EXISTS " + CH_DATABASE);
      st.execute("DROP TABLE IF EXISTS " + CH_DATABASE + "." + CH_TABLE);
      st.execute(
          "CREATE TABLE "
              + CH_DATABASE
              + "."
              + CH_TABLE
              + " (user_id Int64, v Float64) ENGINE = MergeTree ORDER BY user_id");
      // 1000 rows across 10 users (user_id ∈ {1..10}, 100 rows/user, v = user_id * 1.5).
      StringBuilder sb =
          new StringBuilder("INSERT INTO " + CH_DATABASE + "." + CH_TABLE + " VALUES ");
      boolean first = true;
      for (int i = 0; i < 1000; i++) {
        long userId = (i % 10) + 1;
        double v = userId * 1.5;
        if (!first) {
          sb.append(',');
        }
        sb.append('(').append(userId).append(", ").append(v).append(')');
        first = false;
      }
      st.execute(sb.toString());
    }
  }

  private void seedOpenSearch() throws Exception {
    // Drop and recreate the OS index with a numeric user_id so the join key type lines up with CH.
    try {
      client().performRequest(new Request("DELETE", "/" + OS_INDEX));
    } catch (Exception ignored) {
      // not yet created
    }
    Request create = new Request("PUT", "/" + OS_INDEX);
    create.setJsonEntity(
        "{"
            + "\"mappings\":{"
            + "  \"properties\":{"
            + "    \"user_id\":{\"type\":\"long\"},"
            + "    \"name\":{\"type\":\"keyword\"}"
            + "  }"
            + "}}");
    client().performRequest(create);

    Request bulk = new Request("POST", "/" + OS_INDEX + "/_bulk");
    bulk.addParameter("refresh", "true");
    bulk.setJsonEntity(
        "{\"index\":{}}\n"
            + "{\"user_id\":1,\"name\":\"alice\"}\n"
            + "{\"index\":{}}\n"
            + "{\"user_id\":3,\"name\":\"carol\"}\n"
            + "{\"index\":{}}\n"
            + "{\"user_id\":5,\"name\":\"eve\"}\n");
    client().performRequest(bulk);
  }

  private void registerDatasource() throws Exception {
    String schemaJson =
        "{\\\"databases\\\":[{\\\"name\\\":\\\""
            + CH_DATABASE
            + "\\\",\\\"tables\\\":[{\\\"name\\\":\\\""
            + CH_TABLE
            + "\\\",\\\"columns\\\":["
            + "{\\\"name\\\":\\\"user_id\\\",\\\"ch_type\\\":\\\"Int64\\\",\\\"expr_type\\\":\\\"LONG\\\"},"
            + "{\\\"name\\\":\\\"v\\\",\\\"ch_type\\\":\\\"Float64\\\",\\\"expr_type\\\":\\\"DOUBLE\\\"}"
            + "]}]}]}";
    Request req = new Request("POST", "/_plugins/_query/_datasources");
    req.setJsonEntity(
        "{\"name\":\""
            + DS_NAME
            + "\",\"connector\":\"CLICKHOUSE\",\"properties\":{"
            + "\"clickhouse.uri\":\""
            + chJdbcUrl()
            + "\","
            + "\"clickhouse.auth.type\":\"basic\","
            + "\"clickhouse.auth.username\":\""
            + chUser()
            + "\","
            + "\"clickhouse.auth.password\":\""
            + chPassword()
            + "\","
            + "\"clickhouse.schema\":\""
            + schemaJson
            + "\"}}");
    Response r = client().performRequest(req);
    assertThat(
        String.valueOf(r.getStatusLine().getStatusCode()), anyOf(equalTo("200"), equalTo("201")));
  }

  // ---------------------------------------------------------------------------
  // Happy path: bounded left (OS, head 10) joined against an aggregated CH side.
  // The aggregation on the right blocks row fan-out so IN-list pushdown is safe.
  // Asserts:
  //   (1) the returned rows include the three seeded OS user_ids (1, 3, 5),
  //   (2) the CH system.query_log contains an IN (?) or IN (1, 3, 5)/IN (1,3,5)
  //       clause on user_id — i.e. the pushdown really landed at CH.
  // ---------------------------------------------------------------------------

  @Test
  public void testBoundedLeftJoinAggregatedCh() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE IF EXISTS system.query_log");
    }

    String ppl =
        "source="
            + OS_INDEX
            + " | head 10"
            + " | inner join left=d right=f on d.user_id = f.user_id"
            + " [ source="
            + DS_NAME
            + "."
            + CH_DATABASE
            + "."
            + CH_TABLE
            + " | stats sum(v) as s by user_id ]";
    JSONObject j = executeQuery(ppl);

    // (1) Result correctness: must include user_ids 1, 3, 5.
    JSONArray rows = j.getJSONArray("datarows");
    assertThat(
        "join must emit at least one row per seeded OS user (3)",
        rows.length(),
        greaterThanOrEqualTo(3));
    Set<Integer> observedUserIds = new HashSet<>();
    int userIdCol = findUserIdColumn(j);
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      observedUserIds.add(row.getInt(userIdCol));
    }
    assertThat("user_id=1 must be in the join output", observedUserIds.contains(1), equalTo(true));
    assertThat("user_id=3 must be in the join output", observedUserIds.contains(3), equalTo(true));
    assertThat("user_id=5 must be in the join output", observedUserIds.contains(5), equalTo(true));

    // (2) CH-side SQL verification — the query_log must show an IN-list pushdown on user_id.
    List<String> observed = new ArrayList<>();
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("SYSTEM FLUSH LOGS");
      try (ResultSet rs =
          st.executeQuery(
              "SELECT query FROM system.query_log "
                  + "WHERE type = 'QueryFinish' "
                  + "  AND query LIKE '%FROM %`"
                  + CH_DATABASE
                  + "`.`"
                  + CH_TABLE
                  + "`%' "
                  + "  AND query NOT LIKE '%system.query_log%' "
                  + "ORDER BY event_time ASC")) {
        while (rs.next()) {
          observed.add(rs.getString(1));
        }
      }
    }

    StringBuilder dump = new StringBuilder("=== CH query_log (federation IN-list) ===\n");
    for (int i = 0; i < observed.size(); i++) {
      dump.append("[CH SQL ").append(i).append("]\n").append(observed.get(i)).append("\n");
    }
    dump.append("=== END query_log (").append(observed.size()).append(" rows) ===");
    System.out.println(dump);

    assertThat("at least one CH SELECT must have landed", observed.size(), greaterThanOrEqualTo(1));

    // ClickHouse may inline small array params (IN (1, 3, 5)), preserve the parameter marker
    // (IN (?)) if the driver forwards the PreparedStatement's placeholder, or — the most
    // common shape with clickhouse-jdbc 0.6.5 — render the bound array as IN ([1,3,5]).
    //
    // Why IN ([...])? clickhouse-jdbc 0.6.5 client-side-interpolates bound arrays as
    // `[v1,v2,v3]` into the SQL text before shipping to the server; this is the post-bind
    // render seen in system.query_log and is a valid parametric-pushdown form. All three
    // shapes prove that the pushdown reached CH; accept any.
    String inListSql = findInListSql(observed);
    assertThat("IN-list pushdown must target user_id", inListSql, containsString("`user_id`"));
    boolean matchedArrayLiteral = hasInArrayLiteralWithDistinctKeys(inListSql);
    if (!matchedArrayLiteral) {
      assertThat(
          "IN-list pushdown must land at CH as IN (?), IN (1, 3, 5), or IN ([1,3,5])",
          inListSql,
          anyOf(
              containsString("IN (?)"),
              containsString("IN (1, 3, 5)"),
              containsString("IN (1,3,5)"),
              // Tolerate small reorderings from LinkedHashSet-driven distinct iteration.
              containsString("IN (1, 5, 3)"),
              containsString("IN (3, 1, 5)"),
              containsString("IN (3, 5, 1)"),
              containsString("IN (5, 1, 3)"),
              containsString("IN (5, 3, 1)")));
    }
  }

  /**
   * Scans {@code sql} for {@code IN ([...])} clauses and returns {@code true} iff at least one
   * contains a permutation of {@code {1, 3, 5}}. Whitespace around/between elements is tolerated.
   * The distinctness check rejects degenerate matches like {@code [1,1,1]}.
   *
   * <p>This is the post-bind render produced by clickhouse-jdbc 0.6.5 when the driver
   * client-side-interpolates a {@link java.sql.Array} (or {@code Object[]}) bound parameter into
   * the SQL text before shipping to the server — a valid parametric-pushdown shape.
   */
  static boolean hasInArrayLiteralWithDistinctKeys(String sql) {
    Pattern p =
        Pattern.compile(
            "IN\\s*\\(\\s*\\[\\s*(1|3|5)\\s*,\\s*(1|3|5)\\s*,\\s*(1|3|5)\\s*\\]\\s*\\)");
    Matcher m = p.matcher(sql);
    while (m.find()) {
      String a = m.group(1);
      String b = m.group(2);
      String c = m.group(3);
      if (!a.equals(b) && !a.equals(c) && !b.equals(c)) {
        return true;
      }
    }
    return false;
  }

  // --- helpers ---------------------------------------------------------------

  /** Finds the first CH SQL containing an {@code IN (...)} clause; never null. */
  private static String findInListSql(List<String> observed) {
    for (String s : observed) {
      if (s.contains("IN (")) {
        return s;
      }
    }
    throw new AssertionError(
        "no CH query_log entry contained an IN(...) clause — observed=" + observed);
  }

  /**
   * Finds the column index in the result schema corresponding to the join key {@code user_id}.
   * Falls back to searching by substring so it tolerates prefixed aliases (e.g. {@code d.user_id}
   * or {@code f.user_id}).
   */
  private static int findUserIdColumn(JSONObject response) {
    JSONArray schema = response.getJSONArray("schema");
    for (int i = 0; i < schema.length(); i++) {
      JSONObject field = schema.getJSONObject(i);
      String name = field.getString("name");
      if ("user_id".equals(name) || name.endsWith(".user_id") || name.endsWith("_user_id")) {
        return i;
      }
    }
    throw new AssertionError("no user_id-like column found in schema: " + schema);
  }
}
