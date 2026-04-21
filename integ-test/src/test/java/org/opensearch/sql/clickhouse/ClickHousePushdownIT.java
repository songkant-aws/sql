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
        "explain must cross the JDBC→Enumerable boundary",
        body,
        containsString("JdbcToEnumerableConverter"));
    assertThat("explain must include a JdbcTableScan leaf", body, containsString("JdbcTableScan"));
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
        "project must ride into JDBC subtree as JdbcProject", body, containsString("JdbcProject"));
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
        "sort direction must be preserved into the JDBC subtree", body, containsString("DESC"));
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
        "filter predicate must be pushed into the CH SQL", filterSql, containsString("WHERE"));
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

  // ---------------------------------------------------------------------------
  // Aggregation pushdown — count / avg / sum / min / max, with and without
  // `by` grouping. Same two-prong verification as filter/project/sort above:
  //   (a) EXPLAIN shows JdbcAggregate inside the JDBC sub-plan (not
  //       EnumerableAggregate above JdbcToEnumerableConverter).
  //   (b) system.query_log shows the actual aggregate SQL that crossed the
  //       JDBC wire to ClickHouse.
  //
  // Empirically (Wave 7, 2026-04-20), Calcite does NOT decompose AVG into
  // SUM/COUNT over JDBC — a native `AVG(...)` call is emitted through the
  // ClickHouse dialect, so the observed JdbcAggregate row-type lists AVG
  // verbatim and ClickHouse computes it server-side.
  // ---------------------------------------------------------------------------

  @Test
  public void explain_global_aggregation_ridden_as_jdbc_aggregate() throws Exception {
    String body =
        explainBody(
            "source = "
                + DS_NAME
                + ".a.t | stats count() as c, avg(v) as av, sum(v) as sv,"
                + " min(v) as mn, max(v) as mx");
    System.out.println("=== EXPLAIN (global stats) ===\n" + body + "\n=== END EXPLAIN ===");
    assertThat(body, containsString("JdbcToEnumerableConverter"));
    assertThat(body, containsString("JdbcTableScan"));
    assertThat(
        "global aggregation must ride into JDBC subtree as JdbcAggregate with empty group",
        body,
        containsString("JdbcAggregate(group=[{}]"));
    // All five aggregate functions must land on the JdbcAggregate node —
    // none is evaluated above the JDBC boundary.
    assertThat("COUNT must push", body, containsString("COUNT()"));
    assertThat("AVG must push verbatim, not be split into SUM/COUNT", body, containsString("AVG("));
    assertThat("SUM must push", body, containsString("SUM("));
    assertThat("MIN must push", body, containsString("MIN("));
    assertThat("MAX must push", body, containsString("MAX("));
    assertThat(
        "no EnumerableAggregate should appear — aggregation must not stay above JDBC",
        body,
        org.hamcrest.Matchers.not(containsString("EnumerableAggregate")));
  }

  @Test
  public void explain_group_by_aggregation_ridden_as_jdbc_aggregate() throws Exception {
    String body = explainBody("source = " + DS_NAME + ".a.t | stats avg(v) as av by id");
    System.out.println("=== EXPLAIN (stats by id) ===\n" + body + "\n=== END EXPLAIN ===");
    assertThat(body, containsString("JdbcToEnumerableConverter"));
    assertThat(body, containsString("JdbcTableScan"));
    // group=[{0}] pins that column #0 (id) is the group key on the JDBC side.
    assertThat(
        "group-by aggregation must ride into JDBC subtree as JdbcAggregate(group=[{0}])",
        body, containsString("JdbcAggregate(group=[{0}]"));
    assertThat("AVG must appear on the JdbcAggregate row-type", body, containsString("AVG("));
    assertThat(
        "no EnumerableAggregate should appear when grouping by a raw column",
        body,
        org.hamcrest.Matchers.not(containsString("EnumerableAggregate")));
  }

  @Test
  public void explain_filter_plus_aggregation_ridden_as_jdbc_aggregate_over_jdbc_filter()
      throws Exception {
    String body = explainBody("source = " + DS_NAME + ".a.t | where id > 50 | stats count() as c");
    System.out.println(
        "=== EXPLAIN (where id>50 | stats count) ===\n" + body + "\n=== END EXPLAIN ===");
    assertThat(body, containsString("JdbcToEnumerableConverter"));
    assertThat(body, containsString("JdbcTableScan"));
    assertThat(
        "aggregate must ride into JDBC subtree as JdbcAggregate(group=[{}])",
        body,
        containsString("JdbcAggregate(group=[{}]"));
    assertThat(
        "filter must ride into JDBC subtree as JdbcFilter below JdbcAggregate",
        body,
        containsString("JdbcFilter(condition=[>($0, 50)])"));
  }

  @Test
  public void global_aggregation_returns_correct_row() throws Exception {
    JSONObject j =
        executeQuery(
            "source = "
                + DS_NAME
                + ".a.t | stats count() as c, avg(v) as av, sum(v) as sv,"
                + " min(v) as mn, max(v) as mx");
    assertThat(j.getJSONArray("datarows").length(), equalTo(1));
    org.json.JSONArray row = j.getJSONArray("datarows").getJSONArray(0);
    // Seeded: id in 1..100, v = id * 1.5
    //   count = 100
    //   avg(v)   = avg(1.5..150) = 75.75
    //   sum(v)   = 1.5 * (1+..+100) = 1.5 * 5050 = 7575.0
    //   min(v)   = 1.5
    //   max(v)   = 150.0
    assertThat("count", row.getLong(0), equalTo(100L));
    assertThat("avg(v)", row.getDouble(1), equalTo(75.75));
    assertThat("sum(v)", row.getDouble(2), equalTo(7575.0));
    assertThat("min(v)", row.getDouble(3), equalTo(1.5));
    assertThat("max(v)", row.getDouble(4), equalTo(150.0));
  }

  @Test
  public void group_by_aggregation_returns_one_row_per_group() throws Exception {
    JSONObject j = executeQuery("source = " + DS_NAME + ".a.t | stats avg(v) as av by id");
    // 100 distinct ids, each with a single value → 100 rows.
    assertThat(
        "| stats avg(v) by id must return one row per distinct id",
        j.getJSONArray("datarows").length(),
        equalTo(100));
  }

  @Test
  public void query_log_confirms_aggregation_pushdown() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE IF EXISTS system.query_log");
    }

    // 1) global aggregation (no GROUP BY)
    executeQuery(
        "source = "
            + DS_NAME
            + ".a.t | stats count() as c, avg(v) as av, sum(v) as sv,"
            + " min(v) as mn, max(v) as mx");
    // 2) aggregation with GROUP BY
    executeQuery("source = " + DS_NAME + ".a.t | stats avg(v) as av by id");
    // 3) filter + aggregation
    executeQuery("source = " + DS_NAME + ".a.t | where id > 50 | stats count() as c");

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

    StringBuilder dump = new StringBuilder("=== CH query_log (aggregation pushdown) ===\n");
    for (int i = 0; i < observed.size(); i++) {
      dump.append("[CH SQL ").append(i).append("]\n").append(observed.get(i)).append("\n");
    }
    dump.append("=== END query_log (").append(observed.size()).append(" rows) ===");
    System.out.println(dump);

    assertThat(
        "expected three pushed-down aggregations (global, group-by, filter+count)",
        observed.size(),
        greaterThanOrEqualTo(3));

    // (1) Global aggregation: CH-side SQL must contain all five aggregate calls
    //     in a single SELECT with no GROUP BY. Crucially, AVG must appear as
    //     AVG(`v`) — not as a SUM/COUNT pair — so ClickHouse computes it
    //     server-side (no above-JDBC reduction). We anchor on MIN(`v`) because
    //     it only appears in the global-stats SQL, not in the group-by or
    //     filter+count SQLs.
    String globalSql = findMatching(observed, "MIN(`v`)");
    assertThat(globalSql, containsString("COUNT("));
    assertThat(
        "AVG must push verbatim to CH, not be split into SUM/COUNT above JDBC",
        globalSql,
        containsString("AVG(`v`)"));
    assertThat(globalSql, containsString("SUM(`v`)"));
    assertThat(globalSql, containsString("MIN(`v`)"));
    assertThat(globalSql, containsString("MAX(`v`)"));
    assertThat(
        "global aggregation has no GROUP BY clause",
        globalSql,
        org.hamcrest.Matchers.not(containsString("GROUP BY")));

    // (2) Group-by aggregation: GROUP BY `id` + AVG(`v`) in the projection.
    String groupBySql = findMatching(observed, "GROUP BY");
    assertThat(groupBySql, containsString("GROUP BY `id`"));
    assertThat(
        "aggregate over grouped rows must push as AVG(`v`)",
        groupBySql,
        containsString("AVG(`v`)"));

    // (3) Filter + count: WHERE id > 50 and COUNT(*) in a single SELECT.
    String filterCountSql = findMatching(observed, "WHERE");
    assertThat(filterCountSql, containsString("WHERE"));
    assertThat(filterCountSql, containsString("`id` > 50"));
    assertThat(filterCountSql, containsString("COUNT("));
    assertThat(
        "filter + count must collapse into a single SELECT — no outer wrapping aggregate",
        filterCountSql,
        org.hamcrest.Matchers.not(containsString("GROUP BY")));
  }

  // ---------------------------------------------------------------------------
  // Tier-1 whitelist pushdown — math / window / statistical aggregate.
  //
  // These tests rely on system.query_log to confirm that the CH-native SQL
  // actually contains the pushed-down operator (ROUND/ABS, OVER, STDDEV_POP/
  // VAR_SAMP) rather than a Calcite-side fallback.
  // ---------------------------------------------------------------------------

  @Test
  public void query_log_confirms_tier1_math_pushdown() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE IF EXISTS system.query_log");
    }

    executeQuery("source = " + DS_NAME + ".a.t | eval r = round(v, 1) | fields id, r | head 1");
    executeQuery("source = " + DS_NAME + ".a.t | where abs(id) > 50 | stats count() as c");

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
        while (rs.next()) observed.add(rs.getString(1));
      }
    }

    assertThat("expected both math queries in CH log", observed.size(), greaterThanOrEqualTo(2));
    String roundSql = findMatching(observed, "ROUND");
    assertThat("ROUND must be pushed into CH SQL", roundSql, containsString("ROUND"));
    String absSql = findMatching(observed, "ABS");
    assertThat("ABS must be pushed into CH SQL", absSql, containsString("ABS"));
  }

  @Test
  public void query_log_confirms_tier1_window_pushdown() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE IF EXISTS system.query_log");
    }

    // `eventstats` naturally lowers to Project(RexOver(ROW_NUMBER/RANK/...)) over the CH table.
    executeQuery("source = " + DS_NAME + ".a.t | eventstats count() as total | head 3");

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
        while (rs.next()) observed.add(rs.getString(1));
      }
    }

    assertThat(
        "expected at least one CH log entry for eventstats",
        observed.size(),
        greaterThanOrEqualTo(1));
    String windowSql = findMatching(observed, "OVER");
    assertThat(
        "window function must push as OVER clause to CH", windowSql, containsString(" OVER "));
  }

  @Test
  public void query_log_confirms_tier1_stddev_pushdown() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE IF EXISTS system.query_log");
    }

    executeQuery("source = " + DS_NAME + ".a.t | stats stddev_pop(v) as sd, var_samp(v) as vs");

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
        while (rs.next()) observed.add(rs.getString(1));
      }
    }

    assertThat(observed.size(), greaterThanOrEqualTo(1));
    String stddevSql = findMatching(observed, "STDDEV_POP");
    assertThat(stddevSql, containsString("STDDEV_POP"));
    String varSampSql = findMatching(observed, "VAR_SAMP");
    assertThat(varSampSql, containsString("VAR_SAMP"));
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
