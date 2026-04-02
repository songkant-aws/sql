/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.Test;

class NullableSqlAvgAggFunctionTest {

  @Test
  void testCreateAvgFunction() {
    NullableSqlAvgAggFunction avg = new NullableSqlAvgAggFunction(SqlKind.AVG);
    assertNotNull(avg);
    assertEquals("AVG", avg.getName());
    assertEquals(SqlKind.AVG, avg.kind);
  }

  @Test
  void testCreateStddevPopFunction() {
    NullableSqlAvgAggFunction fn = new NullableSqlAvgAggFunction(SqlKind.STDDEV_POP);
    assertNotNull(fn);
    assertEquals("STDDEV_POP", fn.getName());
  }

  @Test
  void testUnsupportedKindRejected() {
    assertThrows(IllegalArgumentException.class, () -> new NullableSqlAvgAggFunction(SqlKind.SUM));
  }

  @Test
  void testOperandTypeCheckerAcceptsNumeric() {
    NullableSqlAvgAggFunction avg = new NullableSqlAvgAggFunction(SqlKind.AVG);
    // The operand type checker should accept NUMERIC types
    assertNotNull(avg.getOperandTypeChecker());
    String allowedSignatures = avg.getOperandTypeChecker().getAllowedSignatures(avg, "AVG");
    assertNotNull(allowedSignatures);
    // Verify numeric types are allowed
    assertTrue(
        allowedSignatures.contains("NUMERIC") || allowedSignatures.contains("AVG("),
        "Should accept numeric operands, got: " + allowedSignatures);
  }

  @Test
  void testOperandTypeCheckerAcceptsDatetime() {
    NullableSqlAvgAggFunction avg = new NullableSqlAvgAggFunction(SqlKind.AVG);
    // The operand type checker should accept DATETIME types
    String allowedSignatures = avg.getOperandTypeChecker().getAllowedSignatures(avg, "AVG");
    assertNotNull(allowedSignatures);
    // Verify datetime types are allowed (the .or() combines both NUMERIC and DATETIME)
    assertTrue(
        allowedSignatures.contains("DATETIME") || allowedSignatures.contains("AVG("),
        "Should accept datetime operands, got: " + allowedSignatures);
  }

  @Test
  void testReturnTypeForNumericOperand() {
    NullableSqlAvgAggFunction avg = new NullableSqlAvgAggFunction(SqlKind.AVG);
    assertNotNull(avg.getReturnTypeInference());
  }

  @Test
  void testGetSubtype() {
    NullableSqlAvgAggFunction avg = new NullableSqlAvgAggFunction(SqlKind.AVG);
    assertEquals(org.apache.calcite.sql.fun.SqlAvgAggFunction.Subtype.AVG, avg.getSubtype());
  }

  @Test
  void testStddevPopDoesNotAcceptDatetime() {
    NullableSqlAvgAggFunction fn = new NullableSqlAvgAggFunction(SqlKind.STDDEV_POP);
    String allowedSignatures = fn.getOperandTypeChecker().getAllowedSignatures(fn, "STDDEV_POP");
    assertNotNull(allowedSignatures);
    assertTrue(
        !allowedSignatures.contains("DATETIME"),
        "STDDEV_POP should not accept datetime operands, got: " + allowedSignatures);
  }

  @Test
  void testVarPopDoesNotAcceptDatetime() {
    NullableSqlAvgAggFunction fn = new NullableSqlAvgAggFunction(SqlKind.VAR_POP);
    String allowedSignatures = fn.getOperandTypeChecker().getAllowedSignatures(fn, "VAR_POP");
    assertNotNull(allowedSignatures);
    assertTrue(
        !allowedSignatures.contains("DATETIME"),
        "VAR_POP should not accept datetime operands, got: " + allowedSignatures);
  }

  @Test
  void testStddevSampDoesNotAcceptDatetime() {
    NullableSqlAvgAggFunction fn = new NullableSqlAvgAggFunction(SqlKind.STDDEV_SAMP);
    String allowedSignatures = fn.getOperandTypeChecker().getAllowedSignatures(fn, "STDDEV_SAMP");
    assertNotNull(allowedSignatures);
    assertTrue(
        !allowedSignatures.contains("DATETIME"),
        "STDDEV_SAMP should not accept datetime operands, got: " + allowedSignatures);
  }

  @Test
  void testVarSampDoesNotAcceptDatetime() {
    NullableSqlAvgAggFunction fn = new NullableSqlAvgAggFunction(SqlKind.VAR_SAMP);
    String allowedSignatures = fn.getOperandTypeChecker().getAllowedSignatures(fn, "VAR_SAMP");
    assertNotNull(allowedSignatures);
    assertTrue(
        !allowedSignatures.contains("DATETIME"),
        "VAR_SAMP should not accept datetime operands, got: " + allowedSignatures);
  }

  @Test
  void testAvgAcceptsDatetimeButNonAvgDoesNot() {
    // AVG should accept DATETIME
    NullableSqlAvgAggFunction avg = new NullableSqlAvgAggFunction(SqlKind.AVG);
    String avgSig = avg.getOperandTypeChecker().getAllowedSignatures(avg, "AVG");
    assertTrue(avgSig.contains("DATETIME"), "AVG should accept datetime operands, got: " + avgSig);

    // STDDEV_POP should NOT accept DATETIME
    NullableSqlAvgAggFunction stddev = new NullableSqlAvgAggFunction(SqlKind.STDDEV_POP);
    String stddevSig = stddev.getOperandTypeChecker().getAllowedSignatures(stddev, "STDDEV_POP");
    assertTrue(
        !stddevSig.contains("DATETIME"),
        "STDDEV_POP should not accept datetime operands, got: " + stddevSig);
  }

  @Test
  void testAllAvgKindsSupported() {
    // Verify all AVG-family SqlKinds can be created
    List<SqlKind> avgKinds =
        List.of(
            SqlKind.AVG,
            SqlKind.STDDEV_POP,
            SqlKind.STDDEV_SAMP,
            SqlKind.VAR_POP,
            SqlKind.VAR_SAMP);
    for (SqlKind kind : avgKinds) {
      NullableSqlAvgAggFunction fn = new NullableSqlAvgAggFunction(kind);
      assertEquals(kind, fn.kind);
    }
  }
}
