/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.testcontainers.clickhouse.ClickHouseContainer;

public class ClickHouseRegistrationIT extends PPLIntegTestCase {
  @ClassRule
  public static final ClickHouseContainer CH =
      new ClickHouseContainer("clickhouse/clickhouse-server:24.3");

  private static final String DS_NAME = "ch_reg_it";

  @Before
  public void setup() throws Exception {
    super.init();
    enableCalcite();
  }

  @After
  public void teardown() throws Exception {
    try {
      Request del = new Request("DELETE", "/_plugins/_query/_datasources/" + DS_NAME);
      client().performRequest(del);
    } catch (Exception ignored) {
    }
  }

  @Test
  public void register_succeeds_with_valid_config() throws Exception {
    Request req = new Request("POST", "/_plugins/_query/_datasources");
    req.setJsonEntity(
        "{"
            + "\"name\":\""
            + DS_NAME
            + "\","
            + "\"connector\":\"CLICKHOUSE\","
            + "\"properties\":{"
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
            + "\"clickhouse.schema\":\"{\\\"databases\\\":[]}\""
            + "}"
            + "}");
    Response resp = client().performRequest(req);
    assertThat(String.valueOf(resp.getStatusLine().getStatusCode()), containsString("20"));
  }

  @Test
  public void register_fails_fast_on_bad_credentials() throws Exception {
    Request req = new Request("POST", "/_plugins/_query/_datasources");
    req.setJsonEntity(
        "{"
            + "\"name\":\""
            + DS_NAME
            + "\","
            + "\"connector\":\"CLICKHOUSE\","
            + "\"properties\":{"
            + "\"clickhouse.uri\":\""
            + CH.getJdbcUrl()
            + "\","
            + "\"clickhouse.auth.type\":\"basic\","
            + "\"clickhouse.auth.username\":\"wrong\","
            + "\"clickhouse.auth.password\":\"wrong\","
            + "\"clickhouse.schema\":\"{\\\"databases\\\":[]}\""
            + "}"
            + "}");
    try {
      client().performRequest(req);
      org.junit.Assert.fail("Expected failure");
    } catch (org.opensearch.client.ResponseException e) {
      String body =
          new String(e.getResponse().getEntity().getContent().readAllBytes());
      assertThat(body.toLowerCase(), containsString("clickhouse"));
    }
  }

  @Test
  public void register_fails_on_unsupported_type_in_schema() throws Exception {
    Request req = new Request("POST", "/_plugins/_query/_datasources");
    req.setJsonEntity(
        "{"
            + "\"name\":\""
            + DS_NAME
            + "\","
            + "\"connector\":\"CLICKHOUSE\","
            + "\"properties\":{"
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
            + "\"clickhouse.schema\":\"{\\\"databases\\\":[{\\\"name\\\":\\\"db\\\",\\\"tables\\\":[{\\\"name\\\":\\\"t\\\",\\\"columns\\\":[{\\\"name\\\":\\\"c\\\",\\\"ch_type\\\":\\\"UInt64\\\",\\\"expr_type\\\":\\\"LONG\\\"}]}]}]}\""
            + "}"
            + "}");
    try {
      client().performRequest(req);
      org.junit.Assert.fail("Expected schema validation failure");
    } catch (org.opensearch.client.ResponseException e) {
      String body =
          new String(e.getResponse().getEntity().getContent().readAllBytes());
      assertThat(body, containsString("UInt64"));
    }
  }
}
