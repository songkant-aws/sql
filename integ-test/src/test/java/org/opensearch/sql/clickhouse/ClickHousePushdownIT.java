/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.testcontainers.clickhouse.ClickHouseContainer;

/**
 * Per-operator pushdown IT: filter / project / sort+limit / explain. Verifies end-to-end
 * correctness and that Calcite's JDBC convention is reached by the planner.
 *
 * <p>Requires Docker. Executed via full integ-test cluster bootstrap.
 */
public class ClickHousePushdownIT extends PPLIntegTestCase {

  @ClassRule
  public static final ClickHouseContainer CH =
      new ClickHouseContainer("clickhouse/clickhouse-server:24.3");

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
    try (Connection c =
            DriverManager.getConnection(CH.getJdbcUrl(), CH.getUsername(), CH.getPassword());
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
            + CH.getJdbcUrl()
            + "\","
            + "\"clickhouse.auth.type\":\"basic\","
            + "\"clickhouse.auth.username\":\""
            + CH.getUsername()
            + "\","
            + "\"clickhouse.auth.password\":\""
            + CH.getPassword()
            + "\","
            + "\"clickhouse.schema\":\""
            + schemaJson
            + "\"}}");
    Response r = client().performRequest(req);
    assertThat(
        String.valueOf(r.getStatusLine().getStatusCode()),
        org.hamcrest.Matchers.anyOf(equalTo("200"), equalTo("201")));
  }

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

  @Test
  public void explain_shows_jdbc_convention_nodes() throws Exception {
    Request req = new Request("POST", "/_plugins/_ppl/_explain");
    req.setJsonEntity("{\"query\":\"source = " + DS_NAME + ".a.t | where id > 10\"}");
    String body = new String(client().performRequest(req).getEntity().getContent().readAllBytes());
    // Calcite's explain output includes "Jdbc" for JDBC convention nodes.
    assertThat(body.toLowerCase().indexOf("jdbc"), greaterThanOrEqualTo(0));
  }
}
