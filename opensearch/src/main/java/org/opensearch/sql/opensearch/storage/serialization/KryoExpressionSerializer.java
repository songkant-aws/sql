/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serialization;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Base64;
import com.esotericsoftware.kryo.Kryo;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rex.RexNode;

public class KryoExpressionSerializer {

  private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(() -> {
    Kryo kryo = new Kryo();
//    kryo.register(Expression.class);
//    kryo.register(RexNode.class);
//    kryo.addDefaultSerializer(RexNode.class, FieldSerializer.class);
    kryo.setRegistrationRequired(false);
    return kryo;
  });

  public String serialize(Expression rexNode) {
    try {
      Kryo kryo = kryoThreadLocal.get();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Output output = new Output(baos);
      kryo.writeObject(output, rexNode);
      output.flush();
      return Base64.getEncoder().encodeToString(baos.toByteArray());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to serialize expression: " + rexNode, e);
    }
  }

  public Expression deserialize(String code) {
    try {
      Kryo kryo = kryoThreadLocal.get();
      ByteArrayInputStream bais = new ByteArrayInputStream(Base64.getDecoder().decode(code));
      Input input = new Input(bais);
      return kryo.readObject(input, Expression.class);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to deserialize kryo expression code: " + code, e);
    }
  }

//  public String serialize(RexNode rexNode) {
//    try {
//      Kryo kryo = kryoThreadLocal.get();
//      ByteArrayOutputStream baos = new ByteArrayOutputStream();
//      Output output = new Output(baos);
//      kryo.writeObject(output, rexNode);
//      output.flush();
//      return Base64.getEncoder().encodeToString(baos.toByteArray());
//    } catch (Exception e) {
//      throw new IllegalStateException("Failed to serialize expression: " + rexNode, e);
//    }
//  }
//
//  public RexNode deserialize(String code) {
//    try {
//      Kryo kryo = kryoThreadLocal.get();
//      ByteArrayInputStream bais = new ByteArrayInputStream(Base64.getDecoder().decode(code));
//      Input input = new Input(bais);
//      return kryo.readObject(input, RexNode.class);
//    } catch (Exception e) {
//      throw new IllegalStateException("Failed to deserialize kryo expression code: " + code, e);
//    }
//  }
}
