/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.MathUtils;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Implementation for subtraction function with overflow checking for integral types. Uses
 * Math.subtractExact to detect overflow instead of silently wrapping.
 */
public class SubtractFunction extends ImplementorUDF {

  public SubtractFunction() {
    super(new SubtractImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.LEAST_RESTRICTIVE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.NUMERIC_NUMERIC;
  }

  public static class SubtractImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          SubtractImplementor.class,
          "subtract",
          Expressions.convert_(Expressions.box(translatedOperands.get(0)), Number.class),
          Expressions.convert_(Expressions.box(translatedOperands.get(1)), Number.class));
    }

    public static Number subtract(Number a, Number b) {
      if (MathUtils.isIntegral(a) && MathUtils.isIntegral(b)) {
        long result = Math.subtractExact(a.longValue(), b.longValue());
        return MathUtils.coerceToWidestIntegralType(a, b, result);
      } else if (MathUtils.isDecimal(a) || MathUtils.isDecimal(b)) {
        BigDecimal da =
            MathUtils.isDecimal(a) ? (BigDecimal) a : BigDecimal.valueOf(a.doubleValue());
        BigDecimal db =
            MathUtils.isDecimal(b) ? (BigDecimal) b : BigDecimal.valueOf(b.doubleValue());
        return da.subtract(db);
      }
      double result = a.doubleValue() - b.doubleValue();
      return MathUtils.coerceToWidestFloatingType(a, b, result);
    }
  }
}
