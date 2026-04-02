/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.functions.response;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

class DefaultSparkSqlFunctionResponseHandleTest {

  @Test
  public void testConstruct() throws Exception {
    DefaultSparkSqlFunctionResponseHandle handle =
        new DefaultSparkSqlFunctionResponseHandle(readJson());

    assertTrue(handle.hasNext());
    ExprValue value = handle.next();
    Map<String, ExprValue> row = value.tupleValue();
    assertEquals(ExprBooleanValue.of(true), row.get("col1"));
    assertEquals(new ExprLongValue(2), row.get("col2"));
    assertEquals(new ExprIntegerValue(3), row.get("col3"));
    assertEquals(new ExprShortValue(4), row.get("col4"));
    assertEquals(new ExprByteValue(5), row.get("col5"));
    assertEquals(new ExprDoubleValue(6.1), row.get("col6"));
    assertEquals(new ExprFloatValue(7.1), row.get("col7"));
    assertEquals(new ExprTimestampValue("2024-01-02 03:04:05.1234"), row.get("col8"));
    assertEquals(new ExprDateValue("2024-01-03 04:05:06.1234"), row.get("col9"));
    assertEquals(new ExprStringValue("some string"), row.get("col10"));

    ExecutionEngine.Schema schema = handle.schema();
    List<Column> columns = schema.getColumns();
    assertEquals("col1", columns.get(0).getName());
  }

  /** Regression: DATE column must produce ExprDateValue, not ExprStringValue. */
  @Test
  public void testDateColumnProducesExprDateValue() {
    JSONObject response =
        buildResponse(
            new String[] {"date_col"}, new String[] {"date"}, new Object[] {"2024-06-15"});

    DefaultSparkSqlFunctionResponseHandle handle =
        new DefaultSparkSqlFunctionResponseHandle(response);

    assertTrue(handle.hasNext());
    ExprValue row = handle.next();
    ExprValue dateVal = row.tupleValue().get("date_col");
    assertEquals(new ExprDateValue("2024-06-15"), dateVal);
    assertInstanceOf(
        ExprDateValue.class,
        dateVal,
        "DATE column should produce ExprDateValue, not ExprStringValue");

    ExecutionEngine.Schema schema = handle.schema();
    assertEquals(ExprCoreType.DATE, schema.getColumns().get(0).getExprType());
  }

  /** Regression: TIMESTAMP column must produce ExprTimestampValue, not ExprStringValue. */
  @Test
  public void testTimestampColumnProducesExprTimestampValue() {
    JSONObject response =
        buildResponse(
            new String[] {"ts_col"},
            new String[] {"timestamp"},
            new Object[] {"2024-06-15 10:30:45"});

    DefaultSparkSqlFunctionResponseHandle handle =
        new DefaultSparkSqlFunctionResponseHandle(response);

    assertTrue(handle.hasNext());
    ExprValue row = handle.next();
    ExprValue tsVal = row.tupleValue().get("ts_col");
    assertEquals(new ExprTimestampValue("2024-06-15 10:30:45"), tsVal);
    assertInstanceOf(
        ExprTimestampValue.class,
        tsVal,
        "TIMESTAMP column should produce ExprTimestampValue, not ExprStringValue");

    ExecutionEngine.Schema schema = handle.schema();
    assertEquals(ExprCoreType.TIMESTAMP, schema.getColumns().get(0).getExprType());
  }

  /** Mixed types in same response: DATE + TIMESTAMP + STRING + INTEGER. */
  @Test
  public void testMixedTypesInSameResponse() {
    JSONObject response =
        buildResponse(
            new String[] {"d", "ts", "s", "i"},
            new String[] {"date", "timestamp", "string", "integer"},
            new Object[] {"2023-12-25", "2023-12-25 08:00:00", "hello", 42});

    DefaultSparkSqlFunctionResponseHandle handle =
        new DefaultSparkSqlFunctionResponseHandle(response);

    assertTrue(handle.hasNext());
    Map<String, ExprValue> row = handle.next().tupleValue();
    assertInstanceOf(ExprDateValue.class, row.get("d"));
    assertInstanceOf(ExprTimestampValue.class, row.get("ts"));
    assertInstanceOf(ExprStringValue.class, row.get("s"));
    assertInstanceOf(ExprIntegerValue.class, row.get("i"));
    assertEquals(new ExprDateValue("2023-12-25"), row.get("d"));
    assertEquals(new ExprTimestampValue("2023-12-25 08:00:00"), row.get("ts"));
    assertEquals(new ExprStringValue("hello"), row.get("s"));
    assertEquals(new ExprIntegerValue(42), row.get("i"));

    List<Column> cols = handle.schema().getColumns();
    assertEquals(ExprCoreType.DATE, cols.get(0).getExprType());
    assertEquals(ExprCoreType.TIMESTAMP, cols.get(1).getExprType());
    assertEquals(ExprCoreType.STRING, cols.get(2).getExprType());
    assertEquals(ExprCoreType.INTEGER, cols.get(3).getExprType());
  }

  /** Null values for date and timestamp columns. */
  @Test
  public void testNullDateAndTimestampValues() {
    // Build a response where the row is missing date_col and ts_col (simulates null)
    JSONObject data = new JSONObject();
    JSONObject schemaCol1 = new JSONObject();
    schemaCol1.put("column_name", "date_col");
    schemaCol1.put("data_type", "date");
    JSONObject schemaCol2 = new JSONObject();
    schemaCol2.put("column_name", "ts_col");
    schemaCol2.put("data_type", "timestamp");
    JSONObject schemaCol3 = new JSONObject();
    schemaCol3.put("column_name", "str_col");
    schemaCol3.put("data_type", "string");

    data.put("schema", new org.json.JSONArray().put(schemaCol1).put(schemaCol2).put(schemaCol3));
    // Row only has str_col; date_col and ts_col are missing → null
    JSONObject rowObj = new JSONObject();
    rowObj.put("str_col", "present");
    data.put("result", new org.json.JSONArray().put(rowObj));

    JSONObject response = new JSONObject();
    response.put("data", data);

    DefaultSparkSqlFunctionResponseHandle handle =
        new DefaultSparkSqlFunctionResponseHandle(response);

    assertTrue(handle.hasNext());
    Map<String, ExprValue> row = handle.next().tupleValue();
    assertEquals(ExprNullValue.of(), row.get("date_col"));
    assertEquals(ExprNullValue.of(), row.get("ts_col"));
    assertEquals(new ExprStringValue("present"), row.get("str_col"));
  }

  /**
   * Build a simple Spark-style response JSON with one row.
   *
   * @param names column names
   * @param types Spark data type strings
   * @param values column values for the single row
   */
  private static JSONObject buildResponse(String[] names, String[] types, Object[] values) {
    JSONObject data = new JSONObject();
    org.json.JSONArray schemaArray = new org.json.JSONArray();
    for (int i = 0; i < names.length; i++) {
      JSONObject col = new JSONObject();
      col.put("column_name", names[i]);
      col.put("data_type", types[i]);
      schemaArray.put(col);
    }
    data.put("schema", schemaArray);

    JSONObject rowObj = new JSONObject();
    for (int i = 0; i < names.length; i++) {
      rowObj.put(names[i], values[i]);
    }
    data.put("result", new org.json.JSONArray().put(rowObj));

    JSONObject response = new JSONObject();
    response.put("data", data);
    return response;
  }

  private JSONObject readJson() throws Exception {
    final URL url =
        DefaultSparkSqlFunctionResponseHandle.class.getResource(
            "/spark_execution_result_test.json");
    return new JSONObject(Files.readString(Paths.get(url.toURI())));
  }
}
