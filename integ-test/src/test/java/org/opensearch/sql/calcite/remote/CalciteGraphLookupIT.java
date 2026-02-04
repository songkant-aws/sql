/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.util.TestUtils;

public class CalciteGraphLookupIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    createGraphIndices();

    indexEdge("1", "A", "B", "e1");
    indexEdge("2", "A", "C", "e2");
    indexEdge("3", "B", "D", "e3");
    indexEdge("4", "C", "D", "e4");

    indexNode("A", "chunk_A");
    indexNode("B", "chunk_B");
    indexNode("C", "chunk_C");
    indexNode("D", "chunk_D");
  }

  @Test
  public void testGraphLookupOutTwoHop() throws IOException {
    JSONObject result =
        executeQuery(
            "graphlookup from=graph_edges nodes=graph_nodes start_with='A' max_depth=2"
                + " direction=out");

    verifySchema(
        result,
        schema("node_id", "string"),
        schema("depth", "int"),
        schema("edge_src", "string"),
        schema("edge_dst", "string"),
        schema("node_chunk_id", "string"),
        schema("edge_chunk_id", "string"));

    verifyNumOfRows(result, 5);
    verifyDataRows(
        result,
        rows("A", 0, null, null, "chunk_A", null),
        rows("B", 1, "A", "B", "chunk_B", "e1"),
        rows("C", 1, "A", "C", "chunk_C", "e2"),
        rows("D", 2, "B", "D", "chunk_D", "e3"),
        rows("D", 2, "C", "D", "chunk_D", "e4"));
  }

  private void indexEdge(String id, String src, String dst, String chunkId) throws IOException {
    Request request = new Request("PUT", "/graph_edges/_doc/" + id + "?refresh=true");
    request.setJsonEntity(
        String.format("{\"src\":\"%s\",\"dst\":\"%s\",\"chunk_id\":\"%s\"}", src, dst, chunkId));
    client().performRequest(request);
  }

  private void indexNode(String id, String chunkId) throws IOException {
    Request request = new Request("PUT", "/graph_nodes/_doc/" + id + "?refresh=true");
    request.setJsonEntity(String.format("{\"id\":\"%s\",\"chunk_id\":\"%s\"}", id, chunkId));
    client().performRequest(request);
  }

  private void createGraphIndices() {
    String edgeMapping =
        "{\"mappings\":{\"properties\":{"
            + "\"src\":{\"type\":\"keyword\"},"
            + "\"dst\":{\"type\":\"keyword\"},"
            + "\"chunk_id\":{\"type\":\"keyword\"}"
            + "}}}";
    String nodeMapping =
        "{\"mappings\":{\"properties\":{"
            + "\"id\":{\"type\":\"keyword\"},"
            + "\"chunk_id\":{\"type\":\"keyword\"}"
            + "}}}";
    TestUtils.createIndexByRestClient(client(), "graph_edges", edgeMapping);
    TestUtils.createIndexByRestClient(client(), "graph_nodes", nodeMapping);
  }
}
