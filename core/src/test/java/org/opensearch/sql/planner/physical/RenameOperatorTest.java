/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;

@ExtendWith(MockitoExtension.class)
public class RenameOperatorTest extends PhysicalPlanTestBase {
  @Mock private PhysicalPlan inputPlan;

  @Test
  public void avg_aggregation_rename() {
    PhysicalPlan plan =
        new RenameOperator(
            new AggregationOperator(
                new TestScan(),
                Collections.singletonList(
                    DSL.named("avg(response)", DSL.avg(DSL.ref("response", INTEGER)))),
                Collections.singletonList(DSL.named("action", DSL.ref("action", STRING)))),
            ImmutableMap.of(DSL.ref("avg(response)", DOUBLE), DSL.ref("avg", DOUBLE)));
    List<ExprValue> result = execute(plan);
    assertEquals(2, result.size());
    assertThat(
        result,
        containsInAnyOrder(
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "GET", "avg", 268d)),
            ExprValueUtils.tupleValue(ImmutableMap.of("action", "POST", "avg", 350d))));
  }

  @Test
  public void rename_int_value() {
    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(ExprValueUtils.integerValue(1));
    PhysicalPlan plan =
        new RenameOperator(
            inputPlan, ImmutableMap.of(DSL.ref("avg(response)", DOUBLE), DSL.ref("avg", DOUBLE)));
    List<ExprValue> result = execute(plan);
    assertEquals(1, result.size());
    assertThat(result, containsInAnyOrder(ExprValueUtils.integerValue(1)));
  }

  /** Test rename on nested struct field (e.g. rename message.info to msg.info). */
  @Test
  public void rename_nested_struct_field() {
    LinkedHashMap<String, ExprValue> innerMap = new LinkedHashMap<>();
    innerMap.put("info", ExprValueUtils.stringValue("hello"));
    ExprValue innerStruct = ExprTupleValue.fromExprValueMap(innerMap);

    LinkedHashMap<String, ExprValue> outerMap = new LinkedHashMap<>();
    outerMap.put("message", innerStruct);
    outerMap.put("action", ExprValueUtils.stringValue("GET"));
    ExprValue tupleValue = ExprTupleValue.fromExprValueMap(outerMap);

    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(tupleValue);

    PhysicalPlan plan =
        new RenameOperator(
            inputPlan,
            ImmutableMap.of(DSL.ref("message.info", STRING), DSL.ref("msg.info", STRING)));
    List<ExprValue> result = execute(plan);
    assertEquals(1, result.size());

    ExprValue resultRow = result.get(0);
    assertEquals("hello", resultRow.tupleValue().get("msg.info").stringValue());
    assertEquals("GET", resultRow.tupleValue().get("action").stringValue());
  }

  /** Test rename on deeply nested struct field. */
  @Test
  public void rename_deeply_nested_struct_field() {
    LinkedHashMap<String, ExprValue> deepMap = new LinkedHashMap<>();
    deepMap.put("id", ExprValueUtils.stringValue("123"));
    ExprValue deepStruct = ExprTupleValue.fromExprValueMap(deepMap);

    LinkedHashMap<String, ExprValue> midMap = new LinkedHashMap<>();
    midMap.put("info", deepStruct);
    ExprValue midStruct = ExprTupleValue.fromExprValueMap(midMap);

    LinkedHashMap<String, ExprValue> outerMap = new LinkedHashMap<>();
    outerMap.put("message", midStruct);
    ExprValue tupleValue = ExprTupleValue.fromExprValueMap(outerMap);

    when(inputPlan.hasNext()).thenReturn(true, false);
    when(inputPlan.next()).thenReturn(tupleValue);

    PhysicalPlan plan =
        new RenameOperator(
            inputPlan,
            ImmutableMap.of(DSL.ref("message.info.id", STRING), DSL.ref("msg_id", STRING)));
    List<ExprValue> result = execute(plan);
    assertEquals(1, result.size());

    ExprValue resultRow = result.get(0);
    assertEquals("123", resultRow.tupleValue().get("msg_id").stringValue());
  }
}
