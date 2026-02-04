/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.graph;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;

/** Shared schema definition for graph lookup results. */
public final class GraphSearchSchema {

  public static final String FIELD_NODE_ID = "node_id";
  public static final String FIELD_DEPTH = "depth";
  public static final String FIELD_EDGE_SRC = "edge_src";
  public static final String FIELD_EDGE_DST = "edge_dst";
  public static final String FIELD_NODE_CHUNK_ID = "node_chunk_id";
  public static final String FIELD_EDGE_CHUNK_ID = "edge_chunk_id";

  @Getter
  private static final List<FieldSpec> fieldSpecs =
      List.of(
          new FieldSpec(FIELD_NODE_ID, ExprCoreType.STRING, SqlTypeName.VARCHAR),
          new FieldSpec(FIELD_DEPTH, ExprCoreType.INTEGER, SqlTypeName.INTEGER),
          new FieldSpec(FIELD_EDGE_SRC, ExprCoreType.STRING, SqlTypeName.VARCHAR),
          new FieldSpec(FIELD_EDGE_DST, ExprCoreType.STRING, SqlTypeName.VARCHAR),
          new FieldSpec(FIELD_NODE_CHUNK_ID, ExprCoreType.STRING, SqlTypeName.VARCHAR),
          new FieldSpec(FIELD_EDGE_CHUNK_ID, ExprCoreType.STRING, SqlTypeName.VARCHAR));

  private GraphSearchSchema() {}

  public static List<String> fieldNames() {
    return fieldSpecs.stream().map(FieldSpec::name).toList();
  }

  public static ExecutionEngine.Schema schema() {
    return new ExecutionEngine.Schema(
        fieldSpecs.stream()
            .map(spec -> new ExecutionEngine.Schema.Column(spec.name(), spec.name(), spec.type()))
            .toList());
  }

  public static RelDataType rowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (FieldSpec spec : fieldSpecs) {
      RelDataType type = typeFactory.createSqlType(spec.sqlType());
      builder.add(spec.name(), typeFactory.createTypeWithNullability(type, true));
    }
    return builder.build();
  }

  public record FieldSpec(String name, ExprCoreType type, SqlTypeName sqlType) {}
}
