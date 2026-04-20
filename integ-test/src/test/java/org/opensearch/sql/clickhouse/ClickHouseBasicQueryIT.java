/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

/**
 * End-to-end IT: seed a ClickHouse instance, register it as a datasource, run PPL queries that
 * resolve to CH tables, verify rows come back.
 *
 * <p>Runs against either a Testcontainers-managed server (default) or a locally started binary
 * (opt-in via {@code -DuseClickhouseBinary=true}); see {@link ClickHouseITBase}.
 */
public class ClickHouseBasicQueryIT extends ClickHouseITBase {

  private static final String DS_NAME = "ch_basic";

  @Before
  public void setup() throws Exception {
    super.init();
    enableCalcite();
    seedClickHouse();
    registerDatasource();
  }

  @After
  public void teardown() throws Exception {
    try {
      client().performRequest(new Request("DELETE", "/_plugins/_query/_datasources/" + DS_NAME));
    } catch (Exception ignored) {
      // best-effort cleanup
    }
  }

  private void seedClickHouse() throws Exception {
    Properties p = new Properties();
    p.setProperty("user", chUser());
    p.setProperty("password", chPassword());
    // Use the ClickHouse Driver directly; going through DriverManager can pick up
    // opensearch-jdbc (also on the test classpath), which throws on any URL that
    // doesn't start with jdbc:opensearch://.
    try (Connection c = new com.clickhouse.jdbc.ClickHouseDriver().connect(chJdbcUrl(), p);
        Statement st = c.createStatement()) {
      st.execute("CREATE DATABASE IF NOT EXISTS analytics");
      st.execute("DROP TABLE IF EXISTS analytics.events");
      st.execute(
          "CREATE TABLE analytics.events ("
              + " event_id Int64,"
              + " user_email String,"
              + " amount Float64,"
              + " ts DateTime"
              + ") ENGINE = MergeTree ORDER BY event_id");
      st.execute(
          "INSERT INTO analytics.events VALUES"
              + " (1, 'a@x.com', 10.5, now()),"
              + " (2, 'b@x.com', 20.0, now()),"
              + " (3, 'a@x.com', 30.0, now())");
    }
  }

  private void registerDatasource() throws Exception {
    String schemaJson =
        "{\\\"databases\\\":[{\\\"name\\\":\\\"analytics\\\",\\\"tables\\\":[{\\\"name\\\":\\\"events\\\",\\\"columns\\\":["
            + "{\\\"name\\\":\\\"event_id\\\",\\\"ch_type\\\":\\\"Int64\\\",\\\"expr_type\\\":\\\"LONG\\\"},"
            + "{\\\"name\\\":\\\"user_email\\\",\\\"ch_type\\\":\\\"String\\\",\\\"expr_type\\\":\\\"STRING\\\"},"
            + "{\\\"name\\\":\\\"amount\\\",\\\"ch_type\\\":\\\"Float64\\\",\\\"expr_type\\\":\\\"DOUBLE\\\"},"
            + "{\\\"name\\\":\\\"ts\\\",\\\"ch_type\\\":\\\"DateTime\\\",\\\"expr_type\\\":\\\"TIMESTAMP\\\"}"
            + "]}]}]}";
    Request req = new Request("POST", "/_plugins/_query/_datasources");
    req.setJsonEntity(
        "{"
            + "\"name\":\""
            + DS_NAME
            + "\","
            + "\"connector\":\"CLICKHOUSE\","
            + "\"properties\":{"
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
            + "\""
            + "}"
            + "}");
    Response resp = client().performRequest(req);
    assertThat(
        String.valueOf(resp.getStatusLine().getStatusCode()),
        org.hamcrest.Matchers.anyOf(equalTo("200"), equalTo("201")));
  }

  @Test
  public void head_returns_rows() throws Exception {
    JSONObject result =
        executeQuery("source = " + DS_NAME + ".analytics.events | head 3 | fields event_id");
    assertThat(result.getJSONArray("datarows").length(), equalTo(3));
  }

  @Test
  public void filter_and_project_end_to_end() throws Exception {
    JSONObject result =
        executeQuery(
            "source = "
                + DS_NAME
                + ".analytics.events"
                + " | where user_email = 'a@x.com'"
                + " | fields event_id, amount");
    assertThat(result.getJSONArray("datarows").length(), equalTo(2));
  }
}
