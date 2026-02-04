/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.exception.SemanticCheckException;

/** Graph search request parsed from PPL graphlookup arguments. */
@Getter
@ToString
public class GraphSearchRequest {
  public static final String ARG_FROM = "from";
  public static final String ARG_NODES = "nodes";
  public static final String ARG_START_WITH = "start_with";
  public static final String ARG_CONNECT_FROM = "connect_from";
  public static final String ARG_CONNECT_TO = "connect_to";
  public static final String ARG_MAX_DEPTH = "max_depth";
  public static final String ARG_DIRECTION = "direction";
  public static final String ARG_NODE_ID_FIELD = "node_id_field";
  public static final String ARG_EDGE_FILTER = "edge_filter";
  public static final String ARG_NODE_FILTER = "node_filter";
  public static final String ARG_NODE_CHUNK_FIELD = "node_chunk_field";
  public static final String ARG_EDGE_CHUNK_FIELD = "edge_chunk_field";

  public static final String DEFAULT_CONNECT_FROM = "src";
  public static final String DEFAULT_CONNECT_TO = "dst";
  public static final String DEFAULT_NODE_ID_FIELD = "id";
  public static final String DEFAULT_CHUNK_FIELD = "chunk_id";
  public static final int DEFAULT_MAX_DEPTH = 2;

  private final String edgeIndex;
  private final String nodeIndex;
  private final List<String> startWiths;
  private final String startWithField;
  private final String connectFromField;
  private final String connectToField;
  private final int maxDepth;
  private final GraphDirection direction;
  private final String nodeIdField;
  private final String edgeFilter;
  private final String nodeFilter;
  private final String nodeChunkField;
  private final String edgeChunkField;

  private GraphSearchRequest(
      String edgeIndex,
      String nodeIndex,
      List<String> startWiths,
      String startWithField,
      String connectFromField,
      String connectToField,
      int maxDepth,
      GraphDirection direction,
      String nodeIdField,
      String edgeFilter,
      String nodeFilter,
      String nodeChunkField,
      String edgeChunkField) {
    this.edgeIndex = edgeIndex;
    this.nodeIndex = nodeIndex;
    this.startWiths = startWiths;
    this.startWithField = startWithField;
    this.connectFromField = connectFromField;
    this.connectToField = connectToField;
    this.maxDepth = maxDepth;
    this.direction = direction;
    this.nodeIdField = nodeIdField;
    this.edgeFilter = edgeFilter;
    this.nodeFilter = nodeFilter;
    this.nodeChunkField = nodeChunkField;
    this.edgeChunkField = edgeChunkField;
  }

  public static GraphSearchRequest fromArguments(Map<String, UnresolvedExpression> arguments) {
    Map<String, UnresolvedExpression> lowerCaseArgs =
        arguments.entrySet().stream()
            .collect(
                java.util.stream.Collectors.toMap(
                    entry -> entry.getKey().toLowerCase(Locale.ROOT), Map.Entry::getValue));

    String edgeIndex = requireString(lowerCaseArgs, ARG_FROM);
    StartWithSpec startWithSpec = parseStartWith(lowerCaseArgs, ARG_START_WITH);
    String nodeIndex = optionalString(lowerCaseArgs, ARG_NODES, null);
    String connectFrom = optionalString(lowerCaseArgs, ARG_CONNECT_FROM, DEFAULT_CONNECT_FROM);
    String connectTo = optionalString(lowerCaseArgs, ARG_CONNECT_TO, DEFAULT_CONNECT_TO);
    int maxDepth = optionalInt(lowerCaseArgs, ARG_MAX_DEPTH, DEFAULT_MAX_DEPTH);
    if (maxDepth < 0) {
      throw new SemanticCheckException("max_depth must be a non-negative integer");
    }
    String directionArg = optionalString(lowerCaseArgs, ARG_DIRECTION, null);
    GraphDirection direction =
        directionArg == null ? GraphDirection.OUT : GraphDirection.fromString(directionArg);
    String nodeIdField = optionalString(lowerCaseArgs, ARG_NODE_ID_FIELD, DEFAULT_NODE_ID_FIELD);
    String edgeFilter = optionalString(lowerCaseArgs, ARG_EDGE_FILTER, null);
    String nodeFilter = optionalString(lowerCaseArgs, ARG_NODE_FILTER, null);
    String nodeChunkField =
        optionalString(lowerCaseArgs, ARG_NODE_CHUNK_FIELD, DEFAULT_CHUNK_FIELD);
    String edgeChunkField =
        optionalString(lowerCaseArgs, ARG_EDGE_CHUNK_FIELD, DEFAULT_CHUNK_FIELD);

    if (nodeFilter != null && nodeIndex == null) {
      throw new SemanticCheckException("node_filter requires nodes index (use nodes=<index>)");
    }

    List<String> startWiths =
        startWithSpec.literal == null ? List.of() : List.of(startWithSpec.literal);

    return new GraphSearchRequest(
        edgeIndex,
        nodeIndex,
        startWiths,
        startWithSpec.field,
        connectFrom,
        connectTo,
        maxDepth,
        direction,
        nodeIdField,
        edgeFilter,
        nodeFilter,
        nodeChunkField,
        edgeChunkField);
  }

  public boolean hasStartWithField() {
    return startWithField != null;
  }

  public String getStartWith() {
    if (startWiths == null || startWiths.isEmpty()) {
      throw new SemanticCheckException("start_with must be a literal when executing graph lookup");
    }
    return startWiths.getFirst();
  }

  public List<String> getStartWiths() {
    return startWiths == null ? List.of() : startWiths;
  }

  public GraphSearchRequest withStartWiths(Collection<String> startWiths) {
    List<String> values =
        startWiths == null ? List.of() : new ArrayList<>(new LinkedHashSet<>(startWiths));
    return new GraphSearchRequest(
        edgeIndex,
        nodeIndex,
        values,
        null,
        connectFromField,
        connectToField,
        maxDepth,
        direction,
        nodeIdField,
        edgeFilter,
        nodeFilter,
        nodeChunkField,
        edgeChunkField);
  }

  public GraphSearchRequest withEdgeFilter(String edgeFilter) {
    return new GraphSearchRequest(
        edgeIndex,
        nodeIndex,
        startWiths,
        startWithField,
        connectFromField,
        connectToField,
        maxDepth,
        direction,
        nodeIdField,
        edgeFilter,
        nodeFilter,
        nodeChunkField,
        edgeChunkField);
  }

  public GraphSearchRequest withNodeFilter(String nodeFilter) {
    return new GraphSearchRequest(
        edgeIndex,
        nodeIndex,
        startWiths,
        startWithField,
        connectFromField,
        connectToField,
        maxDepth,
        direction,
        nodeIdField,
        edgeFilter,
        nodeFilter,
        nodeChunkField,
        edgeChunkField);
  }

  public GraphSearchRequest withMaxDepth(int maxDepth) {
    return new GraphSearchRequest(
        edgeIndex,
        nodeIndex,
        startWiths,
        startWithField,
        connectFromField,
        connectToField,
        maxDepth,
        direction,
        nodeIdField,
        edgeFilter,
        nodeFilter,
        nodeChunkField,
        edgeChunkField);
  }

  private static String requireString(Map<String, UnresolvedExpression> args, String name) {
    UnresolvedExpression expr = args.get(name);
    if (expr == null) {
      throw new SemanticCheckException(String.format("Missing required argument: %s", name));
    }
    return expressionToString(expr, name);
  }

  private static String optionalString(
      Map<String, UnresolvedExpression> args, String name, String defaultValue) {
    UnresolvedExpression expr = args.get(name);
    return expr == null ? defaultValue : expressionToString(expr, name);
  }

  private static int optionalInt(
      Map<String, UnresolvedExpression> args, String name, int defaultValue) {
    UnresolvedExpression expr = args.get(name);
    if (expr == null) {
      return defaultValue;
    }
    if (expr instanceof Literal literal) {
      Object value = literal.getValue();
      if (value instanceof Number number) {
        return number.intValue();
      }
    }
    throw new SemanticCheckException(
        String.format("Argument %s must be an integer literal, but was %s", name, expr.toString()));
  }

  private static String expressionToString(UnresolvedExpression expr, String name) {
    if (expr instanceof Literal literal) {
      return String.valueOf(literal.getValue());
    }
    if (expr instanceof QualifiedName qualifiedName) {
      return qualifiedName.toString();
    }
    if (expr instanceof Field field) {
      UnresolvedExpression inner = field.getField();
      if (inner instanceof QualifiedName qualifiedName) {
        return qualifiedName.toString();
      }
    }
    throw new SemanticCheckException(
        String.format(
            "Argument %s must be a literal or identifier, but was %s", name, expr.toString()));
  }

  private static StartWithSpec parseStartWith(Map<String, UnresolvedExpression> args, String name) {
    UnresolvedExpression expr = args.get(name);
    if (expr == null) {
      throw new SemanticCheckException(String.format("Missing required argument: %s", name));
    }
    if (expr instanceof Literal literal) {
      return new StartWithSpec(String.valueOf(literal.getValue()), null);
    }
    if (expr instanceof QualifiedName qualifiedName) {
      return new StartWithSpec(null, qualifiedName.toString());
    }
    if (expr instanceof Field field) {
      UnresolvedExpression inner = field.getField();
      if (inner instanceof QualifiedName qualifiedName) {
        return new StartWithSpec(null, qualifiedName.toString());
      }
    }
    throw new SemanticCheckException(
        String.format("Argument %s must be a literal or field, but was %s", name, expr.toString()));
  }

  private record StartWithSpec(String literal, String field) {}
}
