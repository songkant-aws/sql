/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.parse;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;

/** ParseExpression. */
@EqualsAndHashCode(callSuper = false)
@ToString
public abstract class ParseExpression extends FunctionExpression {
  @Getter protected final Expression sourceField;
  protected final Expression pattern;
  @Getter protected final Expression identifier;
  protected final String identifierStr;

  /**
   * ParseExpression.
   *
   * @param functionName name of function expression
   * @param sourceField source text field
   * @param pattern pattern used for parsing
   * @param identifier derived field
   */
  public ParseExpression(
      String functionName, Expression sourceField, Expression pattern, Expression identifier) {
    super(FunctionName.of(functionName), ImmutableList.of(sourceField, pattern, identifier));
    this.sourceField = sourceField;
    this.pattern = pattern;
    this.identifier = identifier;
    this.identifierStr = identifier.valueOf().stringValue();
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    ExprValue value = valueEnv.resolve(sourceField);
    if (value.isNull() || value.isMissing()) {
      return ExprValueUtils.nullValue();
    }

    // When the source field is an array, iterate over all elements and return
    // the first successful match. This fixes regex/grok/patterns evaluation on
    // array-backed fields which previously only processed the first element.
    if (value.type() == ExprCoreType.ARRAY) {
      return parseArrayValue(value);
    }

    try {
      return parseValue(value);
    } catch (ExpressionEvaluationException e) {
      throw new SemanticCheckException(
          String.format("failed to parse field \"%s\" with type [%s]", sourceField, value.type()));
    }
  }

  /**
   * Iterate over array elements, apply parseValue to each, and return the first non-empty match.
   * Falls back to null if no element produces a non-empty result.
   */
  private ExprValue parseArrayValue(ExprValue arrayValue) {
    List<ExprValue> elements = arrayValue.collectionValue();
    for (ExprValue element : elements) {
      if (element.isNull() || element.isMissing()) {
        continue;
      }
      try {
        ExprValue result = parseValue(element);
        // A non-empty string result means a successful match
        if (!result.stringValue().isEmpty()) {
          return result;
        }
      } catch (ExpressionEvaluationException e) {
        // Skip elements that cannot be parsed (e.g. non-string types in a mixed array)
        continue;
      }
    }
    return ExprValueUtils.nullValue();
  }

  @Override
  public ExprType type() {
    return ExprCoreType.STRING;
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitParse(this, context);
  }

  public abstract ExprValue parseValue(ExprValue value) throws ExpressionEvaluationException;
}
