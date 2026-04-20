/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

/**
 * Per-operator pushdown IT: filter / project / sort+limit / explain. Verifies end-to-end
 * correctness AND that the corresponding {@code Jdbc*} Calcite operator lands inside the JDBC
 * subtree (i.e., the operator really rode through the JDBC boundary rather than being evaluated by
 * Calcite's Enumerable runtime above a bare {@code JdbcTableScan}).
 *
 * <p>Runs against either a Testcontainers-managed server (default) or a locally started binary
 * (opt-in via {@code -DuseClickhouseBinary=true}); see {@link ClickHouseITBase}.
 */
public class ClickHousePushdownIT extends ClickHouseITBase {

  private static final String DS_NAME = "ch_push";

  @Before
  public void setup() throws Exception {
    super.init();
    enableCalcite();
    seedAndRegister();
  }

  @After
  public void teardown() throws Exception {
    try {
      client().performRequest(new Request("DELETE", "/_plugins/_query/_datasources/" + DS_NAME));
    } catch (Exception ignored) {
      // best-effort cleanup
    }
  }

  private void seedAndRegister() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    // Use the ClickHouse Driver directly; DriverManager can route the connect
    // through opensearch-jdbc (also on the test classpath), which rejects any
    // URL that doesn't start with jdbc:opensearch://.
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("CREATE DATABASE IF NOT EXISTS a");
      st.execute("DROP TABLE IF EXISTS a.t");
      st.execute("CREATE TABLE a.t (id Int64, v Float64) ENGINE = MergeTree ORDER BY id");
      StringBuilder sb = new StringBuilder("INSERT INTO a.t VALUES ");
      for (int i = 1; i <= 100; i++) {
        if (i > 1) {
          sb.append(',');
        }
        sb.append('(').append(i).append(", ").append(i * 1.5).append(')');
      }
      st.execute(sb.toString());
    }
    String schemaJson =
        "{\\\"databases\\\":[{\\\"name\\\":\\\"a\\\",\\\"tables\\\":[{\\\"name\\\":\\\"t\\\",\\\"columns\\\":["
            + "{\\\"name\\\":\\\"id\\\",\\\"ch_type\\\":\\\"Int64\\\",\\\"expr_type\\\":\\\"LONG\\\"},"
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
        String.valueOf(r.getStatusLine().getStatusCode()),
        org.hamcrest.Matchers.anyOf(equalTo("200"), equalTo("201")));
  }

  // ---------------------------------------------------------------------------
  // End-to-end correctness — proves the whole stack returns the right rows.
  // ---------------------------------------------------------------------------

  @Test
  public void filter_returns_only_matching_rows() throws Exception {
    JSONObject j = executeQuery("source = " + DS_NAME + ".a.t | where id = 42 | fields id");
    assertThat(j.getJSONArray("datarows").length(), equalTo(1));
    assertThat(j.getJSONArray("datarows").getJSONArray(0).getInt(0), equalTo(42));
  }

  @Test
  public void project_drops_unwanted_columns() throws Exception {
    JSONObject j = executeQuery("source = " + DS_NAME + ".a.t | head 1 | fields id");
    assertThat(j.getJSONArray("datarows").getJSONArray(0).length(), equalTo(1));
  }

  @Test
  public void sort_and_limit_return_top_n_descending() throws Exception {
    JSONObject j = executeQuery("source = " + DS_NAME + ".a.t | sort - id | head 3 | fields id");
    assertThat(j.getJSONArray("datarows").length(), equalTo(3));
    assertThat(j.getJSONArray("datarows").getJSONArray(0).getInt(0), equalTo(100));
  }

  // ---------------------------------------------------------------------------
  // Per-operator EXPLAIN assertions — proves each operator reaches the JDBC
  // subtree as a Jdbc* node rather than remaining above JdbcToEnumerableConverter.
  //
  // These asserts are pinned to the OBSERVED Calcite physical-plan output under
  // JdbcConvention for ClickHouse (captured via the captured explain body under
  // Wave 6 run of 2026-04-20). Calcite emits one class name per operator:
  //   JdbcToEnumerableConverter    (required — marks the enumerable boundary)
  //   JdbcTableScan                (required — the JDBC-side table read)
  //   JdbcFilter  / JdbcProject / JdbcSort
  //
  // Assertions use .contains(...) so formatting tweaks (indent, attr-order) do
  // not break them.
  // ---------------------------------------------------------------------------

  @Test
  public void explain_shows_jdbc_convention_nodes() throws Exception {
    String body = explainBody("source = " + DS_NAME + ".a.t | where id > 10");
    System.out.println("=== EXPLAIN (filter id>10) ===\n" + body + "\n=== END EXPLAIN ===");

    // Sanity: legacy contract (at least one JDBC-convention node is present).
    assertThat(body.toLowerCase().indexOf("jdbc"), greaterThanOrEqualTo(0));

    // Strong per-operator asserts on the physical plan.
    assertThat(
        "explain must cross the JDBC→Enumerable boundary", body, containsString("JdbcToEnumerableConverter"));
    assertThat(
        "explain must include a JdbcTableScan leaf", body, containsString("JdbcTableScan"));
    assertThat(
        "filter condition must ride into JDBC subtree as JdbcFilter",
        body,
        containsString("JdbcFilter"));
  }

  @Test
  public void explain_project_ridden_as_jdbc_project() throws Exception {
    String body = explainBody("source = " + DS_NAME + ".a.t | fields id");
    System.out.println("=== EXPLAIN (fields id) ===\n" + body + "\n=== END EXPLAIN ===");
    assertThat(body, containsString("JdbcToEnumerableConverter"));
    assertThat(body, containsString("JdbcTableScan"));
    assertThat(
        "project must ride into JDBC subtree as JdbcProject",
        body,
        containsString("JdbcProject"));
  }

  @Test
  public void explain_sort_and_limit_ridden_as_jdbc_sort() throws Exception {
    String body = explainBody("source = " + DS_NAME + ".a.t | sort - id | head 3");
    System.out.println("=== EXPLAIN (sort -id head 3) ===\n" + body + "\n=== END EXPLAIN ===");
    assertThat(body, containsString("JdbcToEnumerableConverter"));
    assertThat(body, containsString("JdbcTableScan"));
    assertThat(
        "sort+limit must ride into JDBC subtree as JdbcSort with fetch=3",
        body,
        containsString("JdbcSort"));
    assertThat(
        "sort direction must be preserved into the JDBC subtree",
        body,
        containsString("DESC"));
    assertThat(
        "limit (head N) must be preserved as fetch on the JDBC subtree",
        body,
        containsString("fetch=[3]"));
  }

  // ---------------------------------------------------------------------------
  // Empirical CH-side SQL assertion — proves the generated JDBC SQL that
  // crossed the wire actually contains the WHERE / column-list / ORDER BY
  // LIMIT clauses. This is the gold-standard "really pushed down" signal, since
  // only what CH receives can count as pushed; anything Calcite evaluated above
  // the JDBC boundary never appears in system.query_log.
  // ---------------------------------------------------------------------------

  @Test
  public void query_log_confirms_filter_project_sort_pushdown() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE IF EXISTS system.query_log");
    }

    executeQuery("source = " + DS_NAME + ".a.t | where id = 42 | fields id");
    executeQuery("source = " + DS_NAME + ".a.t | head 1 | fields id");
    executeQuery("source = " + DS_NAME + ".a.t | sort - id | head 3 | fields id");

    List<String> observed = new ArrayList<>();
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("SYSTEM FLUSH LOGS");
      try (ResultSet rs =
          st.executeQuery(
              "SELECT query FROM system.query_log "
                  + "WHERE type = 'QueryFinish' "
                  + "  AND query LIKE '%FROM %`a`.`t`%' "
                  + "  AND query NOT LIKE '%system.query_log%' "
                  + "ORDER BY event_time ASC")) {
        while (rs.next()) {
          observed.add(rs.getString(1));
        }
      }
    }

    // Emit verbatim SQL so runs leave an audit trail in build/test-results.
    StringBuilder dump =
        new StringBuilder("=== CH query_log (QueryFinish rows touching a.t) ===\n");
    for (int i = 0; i < observed.size(); i++) {
      dump.append("[CH SQL ").append(i).append("]\n").append(observed.get(i)).append("\n");
    }
    dump.append("=== END query_log (").append(observed.size()).append(" rows) ===");
    System.out.println(dump);

    assertThat(
        "expected three pushed-down SELECTs (filter, project+head, sort+head)",
        observed.size(),
        greaterThanOrEqualTo(3));

    // Filter query: WHERE predicate must be inside the SQL sent to CH.
    String filterSql = findMatching(observed, "WHERE");
    assertThat(
        "filter predicate must be pushed into the CH SQL",
        filterSql,
        containsString("WHERE"));
    assertThat(filterSql, containsString("`id`"));
    assertThat(filterSql, containsString("= 42"));
    // And only the projected column is selected (no `v`).
    assertThat(
        "projection must be pushed: only `id` reaches CH, not `v`",
        filterSql.toLowerCase(),
        org.hamcrest.Matchers.not(containsString("`v`")));

    // Sort query: ORDER BY DESC and LIMIT 3 must be inside the SQL sent to CH.
    String sortSql = findMatching(observed, "ORDER BY");
    assertThat(sortSql, containsString("ORDER BY"));
    assertThat(sortSql, containsString("`id`"));
    assertThat(sortSql, containsString("DESC"));
    assertThat("head 3 must push as LIMIT 3", sortSql, containsString("LIMIT 3"));

    // Project query: a LIMIT 1 (head 1) SELECT of just `id` must appear.
    String projectSql = findMatching(observed, "LIMIT 1)");
    assertThat(
        "head 1 must push as LIMIT 1 inside the JDBC sub-SELECT",
        projectSql,
        containsString("LIMIT 1"));
    assertThat(projectSql, containsString("`id`"));
    assertThat(
        "projection must strip `v` even when combined with head/LIMIT",
        projectSql.toLowerCase(),
        org.hamcrest.Matchers.not(containsString("`v`")));
  }

  // --- helpers ---------------------------------------------------------------

  private String explainBody(String ppl) throws Exception {
    Request req = new Request("POST", "/_plugins/_ppl/_explain");
    req.setJsonEntity("{\"query\":\"" + ppl + "\"}");
    return new String(client().performRequest(req).getEntity().getContent().readAllBytes());
  }

  /** Return the first SQL string from {@code observed} that contains {@code needle}; never null. */
  private static String findMatching(List<String> observed, String needle) {
    for (String s : observed) {
      if (s.contains(needle)) {
        return s;
      }
    }
    throw new AssertionError(
        "no CH query_log entry contained '" + needle + "' — observed=" + observed);
  }
}
