/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serialization;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.JsonBuilder;

public class RelJsonSerializer {

  private final FrameworkConfig frameworkConfig;
  private final RelBuilder relBuilder;

  public RelJsonSerializer(FrameworkConfig config) {
    this.frameworkConfig = config;
    this.relBuilder = RelBuilder.create(config);
  }

  public FrameworkConfig getFrameworkConfig() {
    return this.frameworkConfig;
  }

  public String serialize(RexNode rexNode) {
    try {
      final JsonBuilder jsonBuilder = new JsonBuilder();
      final RelJson relJson = RelJson.create().withJsonBuilder(jsonBuilder);
      Object rexNodeJsonRepre = relJson.toJson(rexNode);
      return Base64.getEncoder().encodeToString(jsonBuilder.toJsonString(rexNodeJsonRepre).getBytes());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to serialize expression: " + rexNode, e);
    }
  }

  public RexNode deserialize(String code) {
    try {
      String jsonString = new String(Base64.getDecoder().decode(code), StandardCharsets.UTF_8);
      return RelJsonReader.readRex(relBuilder.getCluster(), jsonString);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to deserialize kryo expression code: " + code, e);
    }
  }


}
