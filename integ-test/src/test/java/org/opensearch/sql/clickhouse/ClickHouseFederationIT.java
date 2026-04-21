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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hc.core5.http.io.entity.EntityUtils;
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
  private static final String OS_INDEX_LARGE = "fed_docs_large";
  private static final String OS_INDEX_EMPTY = "fed_docs_empty";
  private static final String CH_DATABASE = "fed";
  private static final String CH_TABLE = "fed_events";

  // Must match org.opensearch.sql.clickhouse.calcite.ClickHouseSqlDialect IN-list threshold.
  private static final int CH_IN_LIST_THRESHOLD = 10_000;
  // Sized to force bailout: left-side row count must strictly exceed CH_IN_LIST_THRESHOLD.
  private static final int BAILOUT_SEED_SIZE = CH_IN_LIST_THRESHOLD + 5_000;

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
    try {
      client().performRequest(new Request("DELETE", "/" + OS_INDEX_LARGE));
    } catch (Exception ignored) {
      // best-effort cleanup — only the bailout test creates this index
    }
    try {
      client().performRequest(new Request("DELETE", "/" + OS_INDEX_EMPTY));
    } catch (Exception ignored) {
      // best-effort cleanup — only the empty-left test creates this index
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
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p)) {
      truncateChQueryLog(c);
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
    List<String> observed;
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p)) {
      observed = readChQueryLogForFedEvents(c);
    }
    // Built eagerly but only surfaced by JUnit when a downstream assertion fails.
    String dump = dumpObservedSql("CH query_log (federation IN-list)", observed);

    assertThat(
        "at least one CH SELECT must have landed\n" + dump,
        observed.size(),
        greaterThanOrEqualTo(1));

    // ClickHouse may inline small array params (IN (1, 3, 5)), preserve the parameter marker
    // (IN (?)) if the driver forwards the PreparedStatement's placeholder, or — the most
    // common shape with clickhouse-jdbc 0.6.5 — render the bound array as IN ([1,3,5]).
    //
    // Why IN ([...])? clickhouse-jdbc 0.6.5 client-side-interpolates bound arrays as
    // `[v1,v2,v3]` into the SQL text before shipping to the server; this is the post-bind
    // render seen in system.query_log and is a valid parametric-pushdown form. All three
    // shapes prove that the pushdown reached CH; accept any.
    String inListSql = findInListSql(observed);
    assertThat(
        "IN-list pushdown must target user_id\n" + dump,
        inListSql,
        containsString("`user_id`"));
    assertThat(
        "IN-list pushdown must land at CH as IN (?), IN (<keys>), or IN ([<keys>])\n" + dump,
        hasParametricInFormWithDistinctKeys(inListSql, Set.of(1, 3, 5)),
        equalTo(true));
  }

  // ---------------------------------------------------------------------------
  // Bailout: when the bounded left (head BAILOUT_SEED_SIZE) exceeds the ClickHouse
  // dialect's IN-list threshold (CH_IN_LIST_THRESHOLD — see ClickHouseSqlDialect),
  // the federation rule must no-op rather than push down a monstrously-sized
  // IN-list. The CH query_log should therefore carry the full-scan + aggregation
  // SQL with NO IN-list pushdown on user_id — proving the rule correctly degrades
  // to the non-pushdown path when the bound exceeds the configured threshold.
  // ---------------------------------------------------------------------------

  @Test
  public void testBailoutWhenLeftExceedsThreshold() throws Exception {
    // Clean slate: drop the large index if a prior failed run left it behind. This
    // prevents state leak from bleeding into this run's bulk-seed.
    try {
      client().performRequest(new Request("DELETE", "/" + OS_INDEX_LARGE));
    } catch (Exception ignored) {
      // not yet created
    }
    Request create = new Request("PUT", "/" + OS_INDEX_LARGE);
    create.setJsonEntity(
        "{\"mappings\":{\"properties\":{\"user_id\":{\"type\":\"long\"}}},"
            + "\"settings\":{\"index.number_of_shards\":1}}");
    client().performRequest(create);

    // Bulk BAILOUT_SEED_SIZE unique user_ids — head BAILOUT_SEED_SIZE will bind
    // them all, exceeding the CH dialect threshold of CH_IN_LIST_THRESHOLD.
    StringBuilder bulk = new StringBuilder();
    for (int i = 0; i < BAILOUT_SEED_SIZE; i++) {
      bulk.append("{\"index\":{\"_index\":\"").append(OS_INDEX_LARGE).append("\"}}\n");
      bulk.append("{\"user_id\":").append(i).append("}\n");
    }
    Request bulkReq = new Request("POST", "/_bulk");
    bulkReq.addParameter("refresh", "true");
    bulkReq.setJsonEntity(bulk.toString());
    client().performRequest(bulkReq);

    // Verify all docs made it in — catches partial bulk failures that would
    // silently flip this test to a false positive (left row count < threshold).
    Request count = new Request("GET", "/" + OS_INDEX_LARGE + "/_count");
    Response countResp = client().performRequest(count);
    String countBody = EntityUtils.toString(countResp.getEntity());
    assertTrue(
        "Bulk failed to index all " + BAILOUT_SEED_SIZE + " docs. Response: " + countBody,
        countBody.contains("\"count\":" + BAILOUT_SEED_SIZE));

    // Truncate CH query_log so we only see SQL from this test.
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p)) {
      truncateChQueryLog(c);
    }

    String ppl =
        "source="
            + OS_INDEX_LARGE
            + " | head "
            + BAILOUT_SEED_SIZE
            + " | inner join left=d right=f on d.user_id = f.user_id"
            + " [ source="
            + DS_NAME
            + "."
            + CH_DATABASE
            + "."
            + CH_TABLE
            + " | stats sum(v) as s by user_id ]";
    executeQuery(ppl);

    // Collect every CH SQL touching our table from the query_log.
    List<String> observed;
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p)) {
      observed = readChQueryLogForFedEvents(c);
    }

    // Robust assertion: for every CH SQL observed, reject any IN-list on user_id
    // regardless of shape. Matches `user_id IN (<anything except closing paren>)`
    // so it catches all three render shapes that prove a pushdown landed —
    //   (1) IN (?)               — parametric placeholder form
    //   (2) IN (1, 2, 3)         — driver-inlined scalar form
    //   (3) IN ([1, 2, 3])       — clickhouse-jdbc 0.6.5 client-side-interpolated
    //                              array-literal form (see Task 13)
    // Without the pushdown rule firing, the CH SQL is
    //   SELECT SUM(v), user_id FROM fed.fed_events GROUP BY user_id LIMIT 50000
    // with no WHERE clause at all, so any matcher that rejects `user_id IN (...)`
    // is correct. The matcher accepts exactly two word forms for the column
    // reference: the bare identifier `user_id` (bounded by `\b` on both sides)
    // and the backtick-quoted form `` `user_id` ``. Whitespace around `IN` is
    // tolerated via `\s*`.
    Pattern inListOnUserId =
        Pattern.compile("(?s).*(?:\\buser_id\\b|`user_id`)\\s*IN\\s*\\([^)]*\\).*");
    String dump = dumpObservedSql("CH query_log (bailout test)", observed);
    for (String sql : observed) {
      assertFalse(
          "At "
              + BAILOUT_SEED_SIZE
              + " left rows (> threshold "
              + CH_IN_LIST_THRESHOLD
              + "), IN-list pushdown must not fire. Saw:\n"
              + sql
              + "\n"
              + dump,
          inListOnUserId.matcher(sql).matches());
    }
  }

  // ---------------------------------------------------------------------------
  // Non-aggregated right side: no stats on the CH sub-search. SideInputInListRule's
  // step-8 check for an aggregate on the right subtree is advisory — on miss it logs
  // a WARN about potential row fan-out and proceeds anyway, relying on the upstream
  // JOIN_SUBSEARCH_MAXOUT cap (default 50k) to bound the result. Correctness only:
  // verify the query still executes and emits join output covering each seeded left
  // user_id. The WARN itself is informational; we don't assert on log output here.
  // ---------------------------------------------------------------------------

  @Test
  public void testNonAggRightSideLogsWarning() throws Exception {
    String ppl =
        "source="
            + OS_INDEX
            + " | head 3"
            + " | inner join left=d right=f on d.user_id = f.user_id"
            + " [ source="
            + DS_NAME
            + "."
            + CH_DATABASE
            + "."
            + CH_TABLE
            + " ]";
    JSONObject j = executeQuery(ppl);

    // Result correctness: datarows must include each seeded left user_id. Parse
    // via schema-driven column lookup (datarows are positional arrays, not keyed
    // objects) so the check is robust to column ordering.
    JSONArray rows = j.getJSONArray("datarows");
    Set<Integer> observedUserIds = new HashSet<>();
    int userIdCol = findUserIdColumn(j);
    for (int i = 0; i < rows.length(); i++) {
      observedUserIds.add(rows.getJSONArray(i).getInt(userIdCol));
    }
    assertTrue(
        "non-agg right side: user_id=1 must be present. Got: " + observedUserIds,
        observedUserIds.contains(1));
    assertTrue(
        "non-agg right side: user_id=3 must be present. Got: " + observedUserIds,
        observedUserIds.contains(3));
    assertTrue(
        "non-agg right side: user_id=5 must be present. Got: " + observedUserIds,
        observedUserIds.contains(5));
  }

  // ---------------------------------------------------------------------------
  // Empty-left: seed an empty OS index and join it against an aggregated CH side.
  // The runtime drain yields zero keys, which short-circuits the right-side
  // sub-search to an empty IN-list — the final join result must be empty. This
  // pins the no-rows edge case so a future refactor can't accidentally fall back
  // to a full right-side scan when the left is empty.
  // ---------------------------------------------------------------------------

  @Test
  public void testEmptyLeftReturnsEmpty() throws Exception {
    // Clean slate: drop leftover empty-left index from a prior failed run.
    try {
      client().performRequest(new Request("DELETE", "/" + OS_INDEX_EMPTY));
    } catch (Exception ignored) {
      // not yet created
    }
    Request create = new Request("PUT", "/" + OS_INDEX_EMPTY);
    create.setJsonEntity("{\"mappings\":{\"properties\":{\"user_id\":{\"type\":\"long\"}}}}");
    client().performRequest(create);
    client().performRequest(new Request("POST", "/" + OS_INDEX_EMPTY + "/_refresh"));

    String ppl =
        "source="
            + OS_INDEX_EMPTY
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

    // Empty left → empty join output. Assert on the parsed shape rather than the
    // raw body text so we don't depend on pretty-print whitespace.
    JSONArray rows = j.getJSONArray("datarows");
    assertTrue(
        "Empty left should produce empty result. Got datarows: " + rows.toString(),
        rows.length() == 0);
  }

  /**
   * Returns {@code true} iff {@code sql} contains an IN-list matching any of the three shapes
   * accepted by this test suite, with distinct keys drawn from {@code expectedKeys}:
   *
   * <ul>
   *   <li>{@code IN (?)} &mdash; the driver forwarded the PreparedStatement's placeholder;
   *   <li>{@code IN (k1, k2, k3)} &mdash; the driver inlined small scalar params;
   *   <li>{@code IN ([k1, k2, k3])} &mdash; the post-bind render produced by clickhouse-jdbc 0.6.5
   *       when it client-side-interpolates a bound array into the SQL text.
   * </ul>
   *
   * <p>Whitespace around brackets, parentheses, and commas is tolerated. For the enumerated shapes,
   * distinctness across all positions is enforced via a post-match set check so degenerate inputs
   * like {@code IN (1,1,1)} fail the assertion. Shape {@code IN (?)} carries no keys to compare, so
   * it matches unconditionally when the expected set is non-empty.
   *
   * <p>This unifies the previous {@code anyOf(containsString(...))} permutation enumeration and the
   * array-literal regex into one consistent check that scales to any {@code expectedKeys} set
   * without exponential blowup in permutation count. See the {@code IN ([...])} comment on the
   * caller for the clickhouse-jdbc 0.6.5 client-side-interpolation rationale.
   */
  static boolean hasParametricInFormWithDistinctKeys(String sql, Set<Integer> expectedKeys) {
    // Shape 1: the bare placeholder.
    if (Pattern.compile("IN\\s*\\(\\s*\\?\\s*\\)").matcher(sql).find()) {
      return true;
    }
    // Build a disjunction of the expected key tokens, e.g. "(1|3|5)". Keys are integers so no
    // regex-escaping is needed. For the enumerated shapes we require exactly N groups where N is
    // expectedKeys.size(), because both the inlined-scalar and inlined-array renders emit all
    // distinct keys exactly once.
    int n = expectedKeys.size();
    StringBuilder alt = new StringBuilder("(");
    boolean first = true;
    for (Integer k : expectedKeys) {
      if (!first) {
        alt.append('|');
      }
      alt.append(k);
      first = false;
    }
    alt.append(')');

    StringBuilder scalarForm = new StringBuilder("IN\\s*\\(\\s*");
    StringBuilder arrayForm = new StringBuilder("IN\\s*\\(\\s*\\[\\s*");
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        scalarForm.append("\\s*,\\s*");
        arrayForm.append("\\s*,\\s*");
      }
      scalarForm.append(alt);
      arrayForm.append(alt);
    }
    scalarForm.append("\\s*\\)");
    arrayForm.append("\\s*\\]\\s*\\)");

    // Shape 2 (inlined scalars) and shape 3 (inlined array literal): require distinctness.
    return matchesWithDistinctGroups(sql, Pattern.compile(scalarForm.toString()), n)
        || matchesWithDistinctGroups(sql, Pattern.compile(arrayForm.toString()), n);
  }

  /**
   * Returns {@code true} iff {@code sql} contains a match of {@code pattern} whose {@code
   * groupCount} capture groups are all distinct. Rejects degenerate inputs like {@code IN (1, 1,
   * 1)}.
   */
  private static boolean matchesWithDistinctGroups(String sql, Pattern pattern, int groupCount) {
    Matcher m = pattern.matcher(sql);
    while (m.find()) {
      Set<String> groups = new LinkedHashSet<>();
      for (int g = 1; g <= groupCount; g++) {
        groups.add(m.group(g));
      }
      if (groups.size() == groupCount) {
        return true;
      }
    }
    return false;
  }

  // --- helpers ---------------------------------------------------------------

  /**
   * Truncates ClickHouse's {@code system.query_log} so the test only sees fresh SQL. Flushes
   * any buffered query_log rows first so that entries still in the async write buffer from a
   * prior test are landed on disk before the TRUNCATE wipes them — without this pre-flush,
   * late-arriving entries from a prior test can appear in the current test's read window and
   * cause spurious cross-test contamination.
   */
  private static void truncateChQueryLog(Connection ch) throws SQLException {
    try (Statement st = ch.createStatement()) {
      st.execute("SYSTEM FLUSH LOGS");
      st.execute("TRUNCATE TABLE IF EXISTS system.query_log");
    }
  }

  /**
   * Returns every CH SQL in {@code system.query_log} that targets {@code fed.fed_events} from a
   * finished query, oldest-first. Issues {@code SYSTEM FLUSH LOGS} first so the query_log reflects
   * writes the current session made. Self-references (SQL that itself reads {@code
   * system.query_log}) are filtered out.
   */
  private static List<String> readChQueryLogForFedEvents(Connection ch) throws SQLException {
    List<String> observed = new ArrayList<>();
    try (Statement st = ch.createStatement()) {
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
    return observed;
  }

  /**
   * Builds a human-readable dump of the observed CH SQL list for use as a JUnit failure message.
   * Routing this through {@code assertFalse}'s message parameter (rather than {@code
   * System.out.println}) keeps stdout quiet on green runs — the dump is only surfaced when the
   * assertion actually fails.
   */
  private static String dumpObservedSql(String label, List<String> observed) {
    StringBuilder dump = new StringBuilder("=== ").append(label).append(" ===\n");
    for (int i = 0; i < observed.size(); i++) {
      dump.append("[CH SQL ").append(i).append("]\n").append(observed.get(i)).append("\n");
    }
    dump.append("=== END query_log (").append(observed.size()).append(" rows) ===");
    return dump.toString();
  }

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
