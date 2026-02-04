/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.graph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.graph.GraphDirection;
import org.opensearch.sql.graph.GraphSearchRequest;
import org.opensearch.sql.graph.GraphSearchResponse;
import org.opensearch.sql.graph.GraphSearchSchema;
import org.opensearch.sql.graph.GraphStorage;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.transport.client.node.NodeClient;

/** OpenSearch-backed graph storage using BFS traversal. */
@RequiredArgsConstructor
public class OpenSearchGraphStorage implements GraphStorage {

  private static final int TERMS_BATCH_SIZE = 1024;

  private final OpenSearchClient client;
  private final Settings settings;

  @Override
  public GraphSearchResponse search(GraphSearchRequest request) {
    NodeClient nodeClient =
        client
            .getNodeClient()
            .orElseThrow(
                () ->
                    new UnsupportedOperationException(
                        "Graph lookup requires a node client in OpenSearch"));

    int querySizeLimit = settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT);

    Set<String> visited = new HashSet<>();
    Queue<String> frontier = new ArrayDeque<>();
    List<TraversalRow> rows = new ArrayList<>();

    List<String> startWiths = request.getStartWiths();
    if (startWiths.isEmpty()) {
      throw new IllegalArgumentException("start_with must be provided for graph lookup");
    }

    for (String start : startWiths) {
      if (start == null || start.isBlank()) {
        continue;
      }
      if (!visited.add(start)) {
        continue;
      }
      frontier.add(start);
      if (querySizeLimit > 0) {
        rows.add(new TraversalRow(start, 0, null, null, null));
        if (rows.size() >= querySizeLimit) {
          break;
        }
      }
    }

    for (int depth = 1; depth <= request.getMaxDepth(); depth++) {
      if (frontier.isEmpty() || rows.size() >= querySizeLimit) {
        break;
      }

      Set<String> currentFrontier = new HashSet<>();
      while (!frontier.isEmpty()) {
        currentFrontier.add(frontier.poll());
      }

      List<EdgeHit> edges = searchEdges(nodeClient, request, currentFrontier, querySizeLimit);

      List<EdgeTraversal> traversals = new ArrayList<>();
      for (EdgeHit edge : edges) {
        if (request.getDirection() == GraphDirection.OUT) {
          if (currentFrontier.contains(edge.src()) && edge.dst() != null) {
            traversals.add(new EdgeTraversal(edge.dst(), edge.src(), edge.dst(), edge.chunkId()));
          }
        } else if (request.getDirection() == GraphDirection.IN) {
          if (currentFrontier.contains(edge.dst()) && edge.src() != null) {
            traversals.add(new EdgeTraversal(edge.src(), edge.src(), edge.dst(), edge.chunkId()));
          }
        } else {
          if (currentFrontier.contains(edge.src()) && edge.dst() != null) {
            traversals.add(new EdgeTraversal(edge.dst(), edge.src(), edge.dst(), edge.chunkId()));
          }
          if (currentFrontier.contains(edge.dst()) && edge.src() != null) {
            traversals.add(new EdgeTraversal(edge.src(), edge.src(), edge.dst(), edge.chunkId()));
          }
        }
      }

      Set<String> candidateNeighbors =
          traversals.stream().map(EdgeTraversal::neighbor).collect(Collectors.toSet());
      Set<String> allowedNeighbors = candidateNeighbors;

      if (request.getNodeFilter() != null && request.getNodeIndex() != null) {
        allowedNeighbors =
            filterNodes(
                nodeClient, request, candidateNeighbors, querySizeLimit, request.getNodeFilter());
      }

      for (EdgeTraversal traversal : traversals) {
        if (!allowedNeighbors.contains(traversal.neighbor())) {
          continue;
        }
        rows.add(
            new TraversalRow(
                traversal.neighbor(),
                depth,
                traversal.edgeSrc(),
                traversal.edgeDst(),
                traversal.edgeChunkId()));
        if (rows.size() >= querySizeLimit) {
          break;
        }
      }

      if (rows.size() >= querySizeLimit) {
        break;
      }

      for (String neighbor : allowedNeighbors) {
        if (!visited.add(neighbor)) {
          continue;
        }
        frontier.add(neighbor);
      }
    }

    final Map<String, String> nodeChunks;
    if (request.getNodeIndex() != null) {
      Set<String> nodesInResult =
          rows.stream()
              .map(TraversalRow::nodeId)
              .filter(Objects::nonNull)
              .collect(Collectors.toSet());
      nodeChunks = fetchNodeChunks(nodeClient, request, nodesInResult, querySizeLimit);
    } else {
      nodeChunks = Map.of();
    }

    List<ExprValue> results =
        rows.stream().map(row -> toExprValue(row, nodeChunks)).collect(Collectors.toList());
    return new GraphSearchResponse(GraphSearchSchema.schema(), results);
  }

  private ExprValue toExprValue(TraversalRow row, Map<String, String> nodeChunks) {
    Map<String, Object> values = new LinkedHashMap<>();
    values.put(GraphSearchSchema.FIELD_NODE_ID, row.nodeId());
    values.put(GraphSearchSchema.FIELD_DEPTH, row.depth());
    values.put(GraphSearchSchema.FIELD_EDGE_SRC, row.edgeSrc());
    values.put(GraphSearchSchema.FIELD_EDGE_DST, row.edgeDst());
    values.put(GraphSearchSchema.FIELD_NODE_CHUNK_ID, nodeChunks.get(row.nodeId()));
    values.put(GraphSearchSchema.FIELD_EDGE_CHUNK_ID, row.edgeChunkId());
    return ExprValueUtils.tupleValue(values);
  }

  private List<EdgeHit> searchEdges(
      NodeClient nodeClient, GraphSearchRequest request, Set<String> frontier, int querySizeLimit) {
    if (frontier.isEmpty()) {
      return List.of();
    }

    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    if (request.getDirection() == GraphDirection.OUT) {
      boolQuery.filter(QueryBuilders.termsQuery(request.getConnectFromField(), frontier));
    } else if (request.getDirection() == GraphDirection.IN) {
      boolQuery.filter(QueryBuilders.termsQuery(request.getConnectToField(), frontier));
    } else {
      boolQuery.should(QueryBuilders.termsQuery(request.getConnectFromField(), frontier));
      boolQuery.should(QueryBuilders.termsQuery(request.getConnectToField(), frontier));
      boolQuery.minimumShouldMatch(1);
    }

    if (request.getEdgeFilter() != null) {
      boolQuery.filter(QueryBuilders.queryStringQuery(request.getEdgeFilter()));
    }

    SearchSourceBuilder source =
        new SearchSourceBuilder()
            .query(boolQuery)
            .size(querySizeLimit)
            .trackTotalHits(false)
            .fetchSource(
                new String[] {
                  request.getConnectFromField(),
                  request.getConnectToField(),
                  request.getEdgeChunkField()
                },
                null);
    SearchResponse response =
        nodeClient.search(new SearchRequest(request.getEdgeIndex()).source(source)).actionGet();

    List<EdgeHit> edges = new ArrayList<>();
    for (SearchHit hit : response.getHits().getHits()) {
      Map<String, Object> sourceMap = hit.getSourceAsMap();
      if (sourceMap == null) {
        continue;
      }
      String src = extractString(sourceMap.get(request.getConnectFromField()));
      String dst = extractString(sourceMap.get(request.getConnectToField()));
      String chunkId = extractString(sourceMap.get(request.getEdgeChunkField()));
      if (src == null && dst == null) {
        continue;
      }
      edges.add(new EdgeHit(src, dst, chunkId));
    }
    return edges;
  }

  private Set<String> filterNodes(
      NodeClient nodeClient,
      GraphSearchRequest request,
      Set<String> candidates,
      int querySizeLimit,
      String nodeFilter) {
    if (candidates.isEmpty()) {
      return Set.of();
    }
    Set<String> allowed = new HashSet<>();
    for (List<String> batch : partition(candidates, TERMS_BATCH_SIZE)) {
      BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
      boolQuery.filter(QueryBuilders.termsQuery(request.getNodeIdField(), batch));
      boolQuery.filter(QueryBuilders.queryStringQuery(nodeFilter));

      SearchSourceBuilder source =
          new SearchSourceBuilder()
              .query(boolQuery)
              .size(Math.min(querySizeLimit, batch.size()))
              .trackTotalHits(false)
              .fetchSource(new String[] {request.getNodeIdField()}, null);
      SearchResponse response =
          nodeClient.search(new SearchRequest(request.getNodeIndex()).source(source)).actionGet();
      for (SearchHit hit : response.getHits().getHits()) {
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        if (sourceMap == null) {
          continue;
        }
        String nodeId = extractString(sourceMap.get(request.getNodeIdField()));
        if (nodeId != null) {
          allowed.add(nodeId);
        }
      }
    }
    return allowed;
  }

  private Map<String, String> fetchNodeChunks(
      NodeClient nodeClient, GraphSearchRequest request, Set<String> nodeIds, int querySizeLimit) {
    if (nodeIds.isEmpty()) {
      return Map.of();
    }
    Map<String, String> chunks = new HashMap<>();
    for (List<String> batch : partition(nodeIds, TERMS_BATCH_SIZE)) {
      QueryBuilder query = QueryBuilders.termsQuery(request.getNodeIdField(), batch);
      if (request.getNodeFilter() != null) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.filter(query);
        boolQuery.filter(QueryBuilders.queryStringQuery(request.getNodeFilter()));
        query = boolQuery;
      }

      SearchSourceBuilder source =
          new SearchSourceBuilder()
              .query(query)
              .size(Math.min(querySizeLimit, batch.size()))
              .trackTotalHits(false)
              .fetchSource(
                  new String[] {request.getNodeIdField(), request.getNodeChunkField()}, null);
      SearchResponse response =
          nodeClient.search(new SearchRequest(request.getNodeIndex()).source(source)).actionGet();
      for (SearchHit hit : response.getHits().getHits()) {
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        if (sourceMap == null) {
          continue;
        }
        String nodeId = extractString(sourceMap.get(request.getNodeIdField()));
        String chunkId = extractString(sourceMap.get(request.getNodeChunkField()));
        if (nodeId != null) {
          chunks.put(nodeId, chunkId);
        }
      }
    }
    return chunks;
  }

  private List<List<String>> partition(Set<String> values, int size) {
    if (values.isEmpty()) {
      return List.of();
    }
    List<String> list = new ArrayList<>(values);
    List<List<String>> partitions = new ArrayList<>();
    for (int i = 0; i < list.size(); i += size) {
      partitions.add(list.subList(i, Math.min(i + size, list.size())));
    }
    return partitions;
  }

  private String extractString(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof List<?> list && !list.isEmpty()) {
      return Objects.toString(list.get(0), null);
    }
    return Objects.toString(value, null);
  }

  private record EdgeHit(String src, String dst, String chunkId) {}

  private record EdgeTraversal(
      String neighbor, String edgeSrc, String edgeDst, String edgeChunkId) {}

  private record TraversalRow(
      String nodeId, int depth, String edgeSrc, String edgeDst, String edgeChunkId) {}
}
