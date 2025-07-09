/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serialization;

import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.JsonBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.type.ExprType;

@Getter
public class RelJsonSerializer {

  private final RelOptCluster cluster;
  private final JsonMapper jsonMapper = JsonMapper.builder().build();

  public RelJsonSerializer(RelOptCluster cluster) {
    this.cluster = cluster;
  }

  //  public String serialize(RexNode rexNode) {
  //    try {
  //      final JsonBuilder jsonBuilder = new JsonBuilder();
  //      final RelJson relJson = RelJson.create().withJsonBuilder(jsonBuilder);
  //      Object rexNodeJsonRepre = relJson.toJson(rexNode);
  //      return
  // Base64.getEncoder().encodeToString(jsonBuilder.toJsonString(rexNodeJsonRepre).getBytes());
  //    } catch (Exception e) {
  //      throw new IllegalStateException("Failed to serialize expression: " + rexNode, e);
  //    }
  //  }

  public String serialize(
      RexNode rexNode, RelDataType relDataType, Map<String, ExprType> fieldTypes) {
    try {
      final JsonBuilder jsonBuilder = new JsonBuilder();
      final RelJson relJson = RelJson.create().withJsonBuilder(jsonBuilder);
      Object rexNodeJsonRepre = relJson.toJson(rexNode);
      Object relDataTypeJsonRepre = relJson.toJson(relDataType);
      String rexNodeStr = jsonBuilder.toJsonString(rexNodeJsonRepre);
      String rowTypeStr = jsonBuilder.toJsonString(relDataTypeJsonRepre);

      Map<String, String> fieldTypeStrMap =
          fieldTypes.entrySet().stream()
              .map(entry -> Pair.of(entry.getKey(), serializeOpenSearchDataType(entry.getValue())))
              .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

      //      String fieldTypesStr = jsonMapper.writeValueAsString(fieldTypes);
      Map<String, Object> envelope =
          Map.of(
              "rowType", rowTypeStr,
              "expr", rexNodeStr,
              "fieldTypes", fieldTypeStrMap);
      //      return
      // Base64.getEncoder().encodeToString(jsonBuilder.toJsonString(envelope).getBytes());
      String finalJsonString = jsonBuilder.toJsonString(envelope);
      return finalJsonString;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to serialize expression: " + rexNode, e);
    }
  }

  //  public RexNode deserialize(String code) {
  //    try {
  //      String jsonString = new String(Base64.getDecoder().decode(code), StandardCharsets.UTF_8);
  //      return RelJsonReader.readRex(relBuilder.getCluster(), jsonString);
  //    } catch (Exception e) {
  //      throw new IllegalStateException("Failed to deserialize kryo expression code: " + code, e);
  //    }
  //  }

  public Map<String, Object> deserialize(String code) {
    try {
      //      String jsonString = new String(Base64.getDecoder().decode(code),
      // StandardCharsets.UTF_8);
      Map<String, Object> objectMap = (Map<String, Object>) jsonMapper.readValue(code, Map.class);
      Map<String, String> fieldTypeStrMap =
          (Map<String, String>) objectMap.get("fieldTypes");
      Map<String, ExprType> fieldTypes =
          fieldTypeStrMap.entrySet().stream()
              .map(
                  entry -> Pair.of(entry.getKey(), deserializeOpenSearchDataType(entry.getValue())))
              .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

      RelDataType rowType =
          RelJsonReader.readType(cluster.getTypeFactory(), (String) objectMap.get("rowType"));

      RelJson relJson = RelJson.create();

      RexNode rexNode = RelJsonReader.readRex(cluster, (String) objectMap.get("expr"));

      return Map.of(
          "expr", rexNode,
          "rowType", rowType,
          "fieldTypes", fieldTypes);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to deserialize kryo expression code: " + code, e);
    }
  }

  private String serializeOpenSearchDataType(ExprType exprType) {
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      ObjectOutputStream objectOutput = new ObjectOutputStream(output);
      objectOutput.writeObject(exprType);
      objectOutput.flush();
      return Base64.getEncoder().encodeToString(output.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to serialize OpenSearchDataType object: " + exprType, e);
    }
  }

  private ExprType deserializeOpenSearchDataType(String exprTypeStr) {
    try {
      ByteArrayInputStream input =
          new ByteArrayInputStream(Base64.getDecoder().decode(exprTypeStr));
      ObjectInputStream objectInput = new ObjectInputStream(input);
      return (ExprType) objectInput.readObject();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to deserialize OpenSearchDataType: " + exprTypeStr, e);
    }
  }
}
