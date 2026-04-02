/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.core;

import static java.util.stream.Collectors.toMap;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import java.time.chrono.ChronoZonedDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import lombok.EqualsAndHashCode;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.parse.ParseExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

/**
 * Expression script executor that executes the expression on each document and determine if the
 * document is supposed to be filtered out or not.
 */
@EqualsAndHashCode(callSuper = false)
public class ExpressionScript {

  /** Expression to execute. */
  private final Expression expression;

  /** ElasticsearchExprValueFactory. */
  @EqualsAndHashCode.Exclude private final OpenSearchExprValueFactory valueFactory;

  /** Reference Fields. */
  @EqualsAndHashCode.Exclude private final Set<ReferenceExpression> fields;

  /** Expression constructor. */
  public ExpressionScript(Expression expression) {
    this.expression = expression;
    this.fields = extractFields(expression);
    this.valueFactory = buildValueFactory(fields);
  }

  /**
   * Evaluate on the doc generate by the doc provider.
   *
   * @param docProvider doc provider.
   * @param evaluator evaluator
   * @return expr value
   */
  public ExprValue execute(
      Supplier<Map<String, ScriptDocValues<?>>> docProvider,
      BiFunction<Expression, Environment<Expression, ExprValue>, ExprValue> evaluator) {
    Environment<Expression, ExprValue> valueEnv = buildValueEnv(fields, valueFactory, docProvider);
    ExprValue result = evaluator.apply(expression, valueEnv);
    return result;
  }

  public static Set<ReferenceExpression> extractFields(Expression expr) {
    Set<ReferenceExpression> fields = new HashSet<>();
    expr.accept(
        new ExpressionNodeVisitor<Object, Set<ReferenceExpression>>() {
          @Override
          public Object visitReference(ReferenceExpression node, Set<ReferenceExpression> context) {
            context.add(node);
            return null;
          }

          @Override
          public Object visitParse(ParseExpression node, Set<ReferenceExpression> context) {
            node.getSourceField().accept(this, context);
            return null;
          }
        },
        fields);
    return fields;
  }

  private OpenSearchExprValueFactory buildValueFactory(Set<ReferenceExpression> fields) {
    Map<String, OpenSearchDataType> typeEnv =
        fields.stream()
            .collect(toMap(ReferenceExpression::getAttr, e -> OpenSearchDataType.of(e.type())));
    return new OpenSearchExprValueFactory(typeEnv, false);
  }

  private Environment<Expression, ExprValue> buildValueEnv(
      Set<ReferenceExpression> fields,
      OpenSearchExprValueFactory valueFactory,
      Supplier<Map<String, ScriptDocValues<?>>> docProvider) {

    Map<Expression, ExprValue> valueEnv = new HashMap<>();
    for (ReferenceExpression field : fields) {
      String fieldName = field.getAttr();
      ExprValue exprValue =
          valueFactory.construct(fieldName, getDocValue(field, docProvider), false);
      valueEnv.put(field, exprValue);
    }
    // Encapsulate map data structure into anonymous Environment class
    return valueEnv::get;
  }

  private Object getDocValue(
      ReferenceExpression field, Supplier<Map<String, ScriptDocValues<?>>> docProvider) {
    String fieldName = OpenSearchTextType.convertTextToKeyword(field.getAttr(), field.type());
    ScriptDocValues<?> docValue = docProvider.get().get(fieldName);
    if (docValue == null || docValue.isEmpty()) {
      return null; // No way to differentiate null and missing from doc value
    }

    Object value = docValue.get(0);
    if (value instanceof ChronoZonedDateTime) {
      return ((ChronoZonedDateTime<?>) value).toInstant();
    }
    return castNumberToFieldType(value, field.type());
  }

  /**
   * DocValue only support long and double so cast to integer and float if needed. The doc value
   * must be Long and Double for expr type Long/Integer and Double/Float respectively.
   *
   * <p>Multivalue fields may return a List (ArrayList) instead of a scalar Number from doc_values.
   * We unwrap single-element lists only for scalar numeric types (INTEGER, FLOAT) to avoid a
   * ClassCastException while preserving true array semantics for multi-element lists and
   * non-numeric types.
   */
  private Object castNumberToFieldType(Object value, ExprType type) {
    if (value == null) {
      return value;
    }

    if (type == INTEGER) {
      return toNumber(value).intValue();
    } else if (type == FLOAT) {
      return toNumber(value).floatValue();
    } else {
      return value;
    }
  }

  /**
   * Convert a value to {@link Number}, unwrapping a single-element List if necessary. OpenSearch
   * doc_values may return an ArrayList containing a single Long/Double for scalar-mapped fields.
   */
  private static Number toNumber(Object value) {
    if (value instanceof Number num) {
      return num;
    }
    if (value instanceof List<?> list && list.size() == 1 && list.get(0) instanceof Number num) {
      return num;
    }
    throw new IllegalStateException(
        "Unexpected value type for numeric field: " + value.getClass().getName());
  }
}
