/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.predicate;

import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.expression.function.SerializableFunction;
import org.opensearch.sql.utils.OperatorUtils;

/**
 * The definition of binary predicate function<br>
 * and, Accepts two Boolean values and produces a Boolean.<br>
 * or, Accepts two Boolean values and produces a Boolean.<br>
 * xor, Accepts two Boolean values and produces a Boolean.<br>
 * equalTo, Compare the left expression and right expression and produces a Boolean.
 */
@UtilityClass
public class BinaryPredicateOperators {
  /**
   * Register Binary Predicate Function.
   *
   * @param repository {@link BuiltinFunctionRepository}.
   */
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(and());
    repository.register(or());
    repository.register(xor());
    repository.register(equal());
    repository.register(notEqual());
    repository.register(less());
    repository.register(lte());
    repository.register(greater());
    repository.register(gte());
    repository.register(like());
    repository.register(ilike());
    repository.register(notLike());
    repository.register(regexp());
  }

  /**
   * The and logic.
   *
   * <table>
   *   <tr>
   *     <th>A</th>
   *     <th>B</th>
   *     <th>A AND B</th>
   *   </tr>
   *   <tr>
   *     <td>TRUE</td>
   *     <td>TRUE</td>
   *     <td>TRUE</td>
   *   </tr>
   *   <tr>
   *     <td>TRUE</td>
   *     <td>FALSE</td>
   *     <td>FALSE</td>
   *   </tr>
   *   <tr>
   *     <td>TRUE</td>
   *     <td>NULL</td>
   *     <td>NULL</td>
   *   </tr>
   *   <tr>
   *     <td>TRUE</td>
   *     <td>MISSING</td>
   *     <td>MISSING</td>
   *   </tr>
   *   <tr>
   *     <td>FALSE</td>
   *     <td>FALSE</td>
   *     <td>FALSE</td>
   *   </tr>
   *   <tr>
   *     <td>FALSE</td>
   *     <td>NULL</td>
   *     <td>FALSE</td>
   *   </tr>
   *   <tr>
   *     <td>FALSE</td>
   *     <td>MISSING</td>
   *     <td>FALSE</td>
   *   </tr>
   *   <tr>
   *     <td>NULL</td>
   *     <td>NULL</td>
   *     <td>NULL</td>
   *   </tr>
   *   <tr>
   *       <td>NULL</td>
   *       <td>MISSING</td>
   *       <td>MISSING</td>
   *   </tr>
   *   <tr>
   *       <td>MISSING</td>
   *       <td>MISSING</td>
   *       <td>MISSING</td>
   *   </tr>
   * </table>
   */
  private static final Table<ExprValue, ExprValue, ExprValue> andTable =
      new ImmutableTable.Builder<ExprValue, ExprValue, ExprValue>()
          .put(LITERAL_TRUE, LITERAL_TRUE, LITERAL_TRUE)
          .put(LITERAL_TRUE, LITERAL_FALSE, LITERAL_FALSE)
          .put(LITERAL_TRUE, LITERAL_NULL, LITERAL_NULL)
          .put(LITERAL_TRUE, LITERAL_MISSING, LITERAL_MISSING)
          .put(LITERAL_FALSE, LITERAL_FALSE, LITERAL_FALSE)
          .put(LITERAL_FALSE, LITERAL_NULL, LITERAL_FALSE)
          .put(LITERAL_FALSE, LITERAL_MISSING, LITERAL_FALSE)
          .put(LITERAL_NULL, LITERAL_NULL, LITERAL_NULL)
          .put(LITERAL_NULL, LITERAL_MISSING, LITERAL_MISSING)
          .put(LITERAL_MISSING, LITERAL_MISSING, LITERAL_MISSING)
          .build();

  /**
   * The or logic.
   *
   * <table>
   *   <tr>
   *     <th>A</th>
   *     <th>B</th>
   *     <th>A OR B</th>
   *   </tr>
   *   <tr>
   *     <td>TRUE</td>
   *     <td>TRUE</td>
   *     <td>TRUE</td>
   *   </tr>
   *   <tr>
   *     <td>TRUE</td>
   *     <td>FALSE</td>
   *     <td>TRUE</td>
   *   </tr>
   *   <tr>
   *     <td>TRUE</td>
   *     <td>NULL</td>
   *     <td>TRUE</td>
   *   </tr>
   *   <tr>
   *     <td>TRUE</td>
   *     <td>MISSING</td>
   *     <td>TRUE</td>
   *   </tr>
   *   <tr>
   *     <td>FALSE</td>
   *     <td>FALSE</td>
   *     <td>FALSE</td>
   *   </tr>
   *   <tr>
   *     <td>FALSE</td>
   *     <td>NULL</td>
   *     <td>NULL</td>
   *   </tr>
   *   <tr>
   *     <td>FALSE</td>
   *     <td>MISSING</td>
   *     <td>MISSING</td>
   *   </tr>
   *   <tr>
   *     <td>NULL</td>
   *     <td>NULL</td>
   *     <td>NULL</td>
   *   </tr>
   *   <tr>
   *       <td>NULL</td>
   *       <td>MISSING</td>
   *       <td>NULL</td>
   *   </tr>
   *   <tr>
   *       <td>MISSING</td>
   *       <td>MISSING</td>
   *       <td>MISSING</td>
   *   </tr>
   * </table>
   */
  private static final Table<ExprValue, ExprValue, ExprValue> orTable =
      new ImmutableTable.Builder<ExprValue, ExprValue, ExprValue>()
          .put(LITERAL_TRUE, LITERAL_TRUE, LITERAL_TRUE)
          .put(LITERAL_TRUE, LITERAL_FALSE, LITERAL_TRUE)
          .put(LITERAL_TRUE, LITERAL_NULL, LITERAL_TRUE)
          .put(LITERAL_TRUE, LITERAL_MISSING, LITERAL_TRUE)
          .put(LITERAL_FALSE, LITERAL_FALSE, LITERAL_FALSE)
          .put(LITERAL_FALSE, LITERAL_NULL, LITERAL_NULL)
          .put(LITERAL_FALSE, LITERAL_MISSING, LITERAL_MISSING)
          .put(LITERAL_NULL, LITERAL_NULL, LITERAL_NULL)
          .put(LITERAL_NULL, LITERAL_MISSING, LITERAL_NULL)
          .put(LITERAL_MISSING, LITERAL_MISSING, LITERAL_MISSING)
          .build();

  /**
   * The xor logic.
   *
   * <table>
   *   <tr>
   *     <th>A</th>
   *     <th>B</th>
   *     <th>A XOR B</th>
   *   </tr>
   *   <tr>
   *     <td>TRUE</td>
   *     <td>TRUE</td>
   *     <td>FALSE</td>
   *   </tr>
   *   <tr>
   *     <td>TRUE</td>
   *     <td>FALSE</td>
   *     <td>TRUE</td>
   *   </tr>
   *   <tr>
   *     <td>TRUE</td>
   *     <td>NULL</td>
   *     <td>TRUE</td>
   *   </tr>
   *   <tr>
   *     <td>TRUE</td>
   *     <td>MISSING</td>
   *     <td>TRUE</td>
   *   </tr>
   *   <tr>
   *     <td>FALSE</td>
   *     <td>FALSE</td>
   *     <td>FALSE</td>
   *   </tr>
   *   <tr>
   *     <td>FALSE</td>
   *     <td>NULL</td>
   *     <td>NULL</td>
   *   </tr>
   *   <tr>
   *     <td>FALSE</td>
   *     <td>MISSING</td>
   *     <td>MISSING</td>
   *   </tr>
   *   <tr>
   *     <td>NULL</td>
   *     <td>NULL</td>
   *     <td>NULL</td>
   *   </tr>
   *   <tr>
   *       <td>NULL</td>
   *       <td>MISSING</td>
   *       <td>NULL</td>
   *   </tr>
   *   <tr>
   *       <td>MISSING</td>
   *       <td>MISSING</td>
   *       <td>MISSING</td>
   *   </tr>
   * </table>
   */
  private static final Table<ExprValue, ExprValue, ExprValue> xorTable =
      new ImmutableTable.Builder<ExprValue, ExprValue, ExprValue>()
          .put(LITERAL_TRUE, LITERAL_TRUE, LITERAL_FALSE)
          .put(LITERAL_TRUE, LITERAL_FALSE, LITERAL_TRUE)
          .put(LITERAL_TRUE, LITERAL_NULL, LITERAL_TRUE)
          .put(LITERAL_TRUE, LITERAL_MISSING, LITERAL_TRUE)
          .put(LITERAL_FALSE, LITERAL_FALSE, LITERAL_FALSE)
          .put(LITERAL_FALSE, LITERAL_NULL, LITERAL_NULL)
          .put(LITERAL_FALSE, LITERAL_MISSING, LITERAL_MISSING)
          .put(LITERAL_NULL, LITERAL_NULL, LITERAL_NULL)
          .put(LITERAL_NULL, LITERAL_MISSING, LITERAL_NULL)
          .put(LITERAL_MISSING, LITERAL_MISSING, LITERAL_MISSING)
          .build();

  private static DefaultFunctionResolver and() {
    return define(
        BuiltinFunctionName.AND.getName(),
        impl((v1, v2) -> lookupTableFunction(v1, v2, andTable), BOOLEAN, BOOLEAN, BOOLEAN));
  }

  private static DefaultFunctionResolver or() {
    return define(
        BuiltinFunctionName.OR.getName(),
        impl((v1, v2) -> lookupTableFunction(v1, v2, orTable), BOOLEAN, BOOLEAN, BOOLEAN));
  }

  private static DefaultFunctionResolver xor() {
    return define(
        BuiltinFunctionName.XOR.getName(),
        impl((v1, v2) -> lookupTableFunction(v1, v2, xorTable), BOOLEAN, BOOLEAN, BOOLEAN));
  }

  /**
   * Cast a string ExprValue to the given temporal type for implicit coercion in comparisons.
   * Supports DATE, TIME, and TIMESTAMP target types.
   */
  private static ExprValue castStringToTemporal(ExprValue stringVal, ExprCoreType temporalType) {
    String s = stringVal.stringValue();
    return switch (temporalType) {
      case DATE -> new ExprDateValue(s);
      case TIME -> new ExprTimeValue(s);
      case TIMESTAMP -> new ExprTimestampValue(s);
      default -> throw new IllegalArgumentException("Unsupported temporal type: " + temporalType);
    };
  }

  /**
   * Build implicit string-to-temporal coercion overloads for a comparison operator. For each
   * temporal type (DATE, TIME, TIMESTAMP), adds (STRING, temporal) and (temporal, STRING) overloads
   * that cast the string operand before comparing.
   */
  private static List<SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>>
      stringTemporalCompareImpls(
          java.util.function.BiFunction<ExprValue, ExprValue, ExprBooleanValue> comparator) {
    List<SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>> impls =
        new ArrayList<>();
    for (ExprCoreType temporalType : List.of(DATE, TIME, TIMESTAMP)) {
      impls.add(
          impl(
              nullMissingHandling(
                  (v1, v2) -> comparator.apply(castStringToTemporal(v1, temporalType), v2)),
              BOOLEAN,
              STRING,
              temporalType));
      impls.add(
          impl(
              nullMissingHandling(
                  (v1, v2) -> comparator.apply(v1, castStringToTemporal(v2, temporalType))),
              BOOLEAN,
              temporalType,
              STRING));
    }
    return impls;
  }

  private static DefaultFunctionResolver equal() {
    java.util.function.BiFunction<ExprValue, ExprValue, ExprBooleanValue> cmp =
        (v1, v2) -> ExprBooleanValue.of(v1.equals(v2));
    List<SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>> impls =
        new ArrayList<>(
            ExprCoreType.coreTypes().stream()
                .map(
                    type ->
                        impl(
                            nullMissingHandling((v1, v2) -> ExprBooleanValue.of(v1.equals(v2))),
                            BOOLEAN,
                            type,
                            type))
                .collect(Collectors.toList()));
    impls.addAll(stringTemporalCompareImpls(cmp));
    return define(BuiltinFunctionName.EQUAL.getName(), impls);
  }

  private static DefaultFunctionResolver notEqual() {
    java.util.function.BiFunction<ExprValue, ExprValue, ExprBooleanValue> cmp =
        (v1, v2) -> ExprBooleanValue.of(!v1.equals(v2));
    List<SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>> impls =
        new ArrayList<>(
            ExprCoreType.coreTypes().stream()
                .map(
                    type ->
                        impl(
                            nullMissingHandling((v1, v2) -> ExprBooleanValue.of(!v1.equals(v2))),
                            BOOLEAN,
                            type,
                            type))
                .collect(Collectors.toList()));
    impls.addAll(stringTemporalCompareImpls(cmp));
    return define(BuiltinFunctionName.NOTEQUAL.getName(), impls);
  }

  private static DefaultFunctionResolver less() {
    java.util.function.BiFunction<ExprValue, ExprValue, ExprBooleanValue> cmp =
        (v1, v2) -> ExprBooleanValue.of(v1.compareTo(v2) < 0);
    List<SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>> impls =
        new ArrayList<>(
            ExprCoreType.coreTypes().stream()
                .map(
                    type ->
                        impl(
                            nullMissingHandling(
                                (v1, v2) -> ExprBooleanValue.of(v1.compareTo(v2) < 0)),
                            BOOLEAN,
                            type,
                            type))
                .collect(Collectors.toList()));
    impls.addAll(stringTemporalCompareImpls(cmp));
    return define(BuiltinFunctionName.LESS.getName(), impls);
  }

  private static DefaultFunctionResolver lte() {
    java.util.function.BiFunction<ExprValue, ExprValue, ExprBooleanValue> cmp =
        (v1, v2) -> ExprBooleanValue.of(v1.compareTo(v2) <= 0);
    List<SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>> impls =
        new ArrayList<>(
            ExprCoreType.coreTypes().stream()
                .map(
                    type ->
                        impl(
                            nullMissingHandling(
                                (v1, v2) -> ExprBooleanValue.of(v1.compareTo(v2) <= 0)),
                            BOOLEAN,
                            type,
                            type))
                .collect(Collectors.toList()));
    impls.addAll(stringTemporalCompareImpls(cmp));
    return define(BuiltinFunctionName.LTE.getName(), impls);
  }

  private static DefaultFunctionResolver greater() {
    java.util.function.BiFunction<ExprValue, ExprValue, ExprBooleanValue> cmp =
        (v1, v2) -> ExprBooleanValue.of(v1.compareTo(v2) > 0);
    List<SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>> impls =
        new ArrayList<>(
            ExprCoreType.coreTypes().stream()
                .map(
                    type ->
                        impl(
                            nullMissingHandling(
                                (v1, v2) -> ExprBooleanValue.of(v1.compareTo(v2) > 0)),
                            BOOLEAN,
                            type,
                            type))
                .collect(Collectors.toList()));
    impls.addAll(stringTemporalCompareImpls(cmp));
    return define(BuiltinFunctionName.GREATER.getName(), impls);
  }

  private static DefaultFunctionResolver gte() {
    java.util.function.BiFunction<ExprValue, ExprValue, ExprBooleanValue> cmp =
        (v1, v2) -> ExprBooleanValue.of(v1.compareTo(v2) >= 0);
    List<SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>> impls =
        new ArrayList<>(
            ExprCoreType.coreTypes().stream()
                .map(
                    type ->
                        impl(
                            nullMissingHandling(
                                (v1, v2) -> ExprBooleanValue.of(v1.compareTo(v2) >= 0)),
                            BOOLEAN,
                            type,
                            type))
                .collect(Collectors.toList()));
    impls.addAll(stringTemporalCompareImpls(cmp));
    return define(BuiltinFunctionName.GTE.getName(), impls);
  }

  private static DefaultFunctionResolver like() {
    return define(
        BuiltinFunctionName.LIKE.getName(),
        impl(nullMissingHandling(OperatorUtils::matches2), BOOLEAN, STRING, STRING),
        impl(nullMissingHandling(OperatorUtils::matches3), BOOLEAN, STRING, STRING, BOOLEAN));
  }

  private static DefaultFunctionResolver ilike() {
    return define(
        BuiltinFunctionName.ILIKE.getName(),
        impl(nullMissingHandling(OperatorUtils::matches2), BOOLEAN, STRING, STRING));
  }

  private static DefaultFunctionResolver regexp() {
    return define(
        BuiltinFunctionName.REGEXP.getName(),
        impl(nullMissingHandling(OperatorUtils::matchesRegexp), INTEGER, STRING, STRING));
  }

  private static DefaultFunctionResolver notLike() {
    return define(
        BuiltinFunctionName.NOT_LIKE.getName(),
        impl(
            nullMissingHandling(
                (v1, v2) -> UnaryPredicateOperators.not(OperatorUtils.matches2(v1, v2))),
            BOOLEAN,
            STRING,
            STRING),
        impl(
            nullMissingHandling(
                (v1, v2, v3) -> UnaryPredicateOperators.not(OperatorUtils.matches3(v1, v2, v3))),
            BOOLEAN,
            STRING,
            STRING,
            BOOLEAN));
  }

  private static ExprValue lookupTableFunction(
      ExprValue arg1, ExprValue arg2, Table<ExprValue, ExprValue, ExprValue> table) {
    if (table.contains(arg1, arg2)) {
      return table.get(arg1, arg2);
    } else {
      return table.get(arg2, arg1);
    }
  }
}
