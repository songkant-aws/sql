/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serialization;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.externalize.RelEnumTypes;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Sarg;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

public class OpenSearchRelJson extends RelJson {

  private final SqlOperatorTable operatorTable;
  private final InputTranslator inputTranslator;

  /**
   * Creates a RelJson.
   *
   * @param jsonBuilder
   * @deprecated Use {@link RelJson#create}, followed by {@link #withJsonBuilder} if
   * {@code jsonBuilder} is not null.
   */
  public OpenSearchRelJson(
      @Nullable JsonBuilder jsonBuilder) {
    super(jsonBuilder);
    PPLBuiltinOperators operatorTable = new PPLBuiltinOperators();
    this.operatorTable = operatorTable.init();
    this.inputTranslator = null; // TODO: implement our own input translator
  }

  RexNode toRex(RelInput relInput, @PolyNull Object o) {
    final RelOptCluster cluster = relInput.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    if (o == null) {
      return null;
      // Support JSON deserializing of non-default Map classes such as gson LinkedHashMap
    } else if (Map.class.isAssignableFrom(o.getClass())) {
      final Map<String, @Nullable Object> map = (Map) o;
      final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
      if (map.containsKey("op")) {
        final Map<String, @Nullable Object> opMap = get(map, "op");
        if (map.containsKey("class")) {
          opMap.put("class", get(map, "class"));
        }
        final List operands = get(map, "operands");
        final List<RexNode> rexOperands = toRexList(relInput, operands);
        final Object jsonType = map.get("type");
        final Map window = (Map) map.get("window");
        if (window != null) {
          final SqlAggFunction operator = requireNonNull(toAggregation(opMap), "operator");
          final RelDataType type = toType(typeFactory, requireNonNull(jsonType, "jsonType"));
          List<RexNode> partitionKeys = new ArrayList<>();
          Object partition = window.get("partition");
          if (partition != null) {
            partitionKeys = toRexList(relInput, (List) partition);
          }
          List<RexFieldCollation> orderKeys = new ArrayList<>();
          if (window.containsKey("order")) {
            addRexFieldCollationList(orderKeys, relInput, (List) window.get("order"));
          }
          final RexWindowBound lowerBound;
          final RexWindowBound upperBound;
          final boolean physical;
          if (window.get("rows-lower") != null) {
            lowerBound = toRexWindowBound(relInput, (Map) window.get("rows-lower"));
            upperBound = toRexWindowBound(relInput, (Map) window.get("rows-upper"));
            physical = true;
          } else if (window.get("range-lower") != null) {
            lowerBound = toRexWindowBound(relInput, (Map) window.get("range-lower"));
            upperBound = toRexWindowBound(relInput, (Map) window.get("range-upper"));
            physical = false;
          } else {
            // No ROWS or RANGE clause
            // Note: lower and upper bounds are non-nullable, so this branch is not reachable
            lowerBound = null;
            upperBound = null;
            physical = false;
          }
          final RexWindowExclusion exclude;
          if (window.get("exclude") != null) {
            exclude = toRexWindowExclusion((Map) window.get("exclude"));
          } else {
            exclude = RexWindowExclusion.EXCLUDE_NO_OTHER;
          }
          final boolean distinct = get((Map<String, Object>) map, "distinct");
          return rexBuilder.makeOver(type, operator, rexOperands, partitionKeys,
              ImmutableList.copyOf(orderKeys),
              requireNonNull(lowerBound, "lowerBound"),
              requireNonNull(upperBound, "upperBound"),
              requireNonNull(exclude, "exclude"),
              physical,
              true, false, distinct, false);
        } else {
          final SqlOperator operator = requireNonNull(toOp(opMap), "operator");
          final RelDataType type;
          if (jsonType != null) {
            type = toType(typeFactory, jsonType);
          } else {
            type = rexBuilder.deriveReturnType(operator, rexOperands);
          }
          return rexBuilder.makeCall(type, operator, rexOperands);
        }
      }
      final Integer input = (Integer) map.get("input");
      if (input != null) {
        return inputTranslator.translateInput(this, input, map, relInput);
      }
      final String field = (String) map.get("field");
      if (field != null) {
        final Object jsonExpr = get(map, "expr");
        final RexNode expr = toRex(relInput, jsonExpr);
        return rexBuilder.makeFieldAccess(expr, field, true);
      }
      final String correl = (String) map.get("correl");
      if (correl != null) {
        final Object jsonType = get(map, "type");
        RelDataType type = toType(typeFactory, jsonType);
        return rexBuilder.makeCorrel(type, new CorrelationId(correl));
      }
      if (map.containsKey("literal")) {
        Object literal = map.get("literal");
        if (literal == null) {
          final RelDataType type = toType(typeFactory, get(map, "type"));
          return rexBuilder.makeNullLiteral(type);
        }
        if (!map.containsKey("type")) {
          // In previous versions, type was not specified for all literals.
          // To keep backwards compatibility, if type is not specified
          // we just interpret the literal
          return toRex(relInput, literal);
        }
        final RelDataType type = toType(typeFactory, get(map, "type"));
        if (literal instanceof Map
            && ((Map<?, ?>) literal).containsKey("rangeSet")) {
          Sarg sarg = sargFromJson((Map) literal);
          return rexBuilder.makeSearchArgumentLiteral(sarg, type);
        }
        if (type.getSqlTypeName() == SqlTypeName.SYMBOL) {
          literal = OpenSearchRelEnumTypes.toEnum((String) literal);
        }
        return rexBuilder.makeLiteral(literal, type);
      }
      if (map.containsKey("sargLiteral")) {
        Object sargObject = map.get("sargLiteral");
        if (sargObject == null) {
          final RelDataType type = toType(typeFactory, get(map, "type"));
          return rexBuilder.makeNullLiteral(type);
        }
        final RelDataType type = toType(typeFactory, get(map, "type"));
        Sarg sarg = sargFromJson((Map) sargObject);
        return rexBuilder.makeSearchArgumentLiteral(sarg, type);
      }
      if (map.containsKey("dynamicParam")) {
        final Object dynamicParamObject = requireNonNull(map.get("dynamicParam"));
        final Integer index = (Integer) dynamicParamObject;
        final RelDataType type = toType(typeFactory, get(map, "type"));
        return rexBuilder.makeDynamicParam(type, index);
      }
      throw new UnsupportedOperationException("cannot convert to rex " + o);
    } else if (o instanceof Boolean) {
      return rexBuilder.makeLiteral((Boolean) o);
    } else if (o instanceof String) {
      return rexBuilder.makeLiteral((String) o);
    } else if (o instanceof Number) {
      final Number number = (Number) o;
      if (number instanceof Double || number instanceof Float) {
        return rexBuilder.makeApproxLiteral(
            BigDecimal.valueOf(number.doubleValue()));
      } else {
        return rexBuilder.makeExactLiteral(
            BigDecimal.valueOf(number.longValue()));
      }
    } else {
      throw new UnsupportedOperationException("cannot convert to rex " + o);
    }
  }

  private List<RexNode> toRexList(RelInput relInput, List operands) {
    final List<RexNode> list = new ArrayList<>();
    for (Object operand : operands) {
      list.add(toRex(relInput, operand));
    }
    return list;
  }

  private @Nullable RexWindowBound toRexWindowBound(RelInput input,
      @Nullable Map<String, Object> map) {
    if (map == null) {
      return null;
    }

    final String type = get(map, "type");
    switch (type) {
      case "CURRENT_ROW":
        return RexWindowBounds.CURRENT_ROW;
      case "UNBOUNDED_PRECEDING":
        return RexWindowBounds.UNBOUNDED_PRECEDING;
      case "UNBOUNDED_FOLLOWING":
        return RexWindowBounds.UNBOUNDED_FOLLOWING;
      case "PRECEDING":
        return RexWindowBounds.preceding(toRex(input, get(map, "offset")));
      case "FOLLOWING":
        return RexWindowBounds.following(toRex(input, get(map, "offset")));
      default:
        throw new UnsupportedOperationException("cannot convert " + type + " to rex window bound");
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends Object> T get(Map<String, ? extends @Nullable Object> map,
      String key) {
    return (T) requireNonNull(map.get(key), () -> "entry for key " + key);
  }

  @Nullable SqlOperator toOp(Map<String, ? extends @Nullable Object> map) {
    // in case different operator has the same kind, check with both name and kind.
    String name = get(map, "name");
    String kind = get(map, "kind");
    String syntax = get(map, "syntax");
    SqlKind sqlKind = SqlKind.valueOf(kind);
    SqlSyntax sqlSyntax = SqlSyntax.valueOf(syntax);
    List<SqlOperator> operators = new ArrayList<>();
    operatorTable.lookupOperatorOverloads(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        null,
        sqlSyntax,
        operators,
        SqlNameMatchers.liberal());
    for (SqlOperator operator : operators) {
      if (operator.kind == sqlKind) {
        return operator;
      }
    }
    String class_ = (String) map.get("class");
    if (class_ != null) {
      return AvaticaUtils.instantiatePlugin(SqlOperator.class, class_);
    }
    throw RESOURCE.noOperator(name, kind, syntax).ex();
  }

  @Nullable SqlAggFunction toAggregation(Map<String, ? extends @Nullable Object> map) {
    return (SqlAggFunction) toOp(map);
  }

  void addRexFieldCollationList(List<RexFieldCollation> list,
      RelInput relInput, @Nullable List<Map<String, Object>> order) {
    if (order == null) {
      return;
    }

    for (Map<String, Object> o : order) {
      RexNode expr = requireNonNull(toRex(relInput, o.get("expr")), "expr");
      Set<SqlKind> directions = new HashSet<>();
      if (Direction.valueOf(get(o, "direction")) == Direction.DESCENDING) {
        directions.add(SqlKind.DESCENDING);
      }
      if (NullDirection.valueOf(get(o, "null-direction")) == NullDirection.FIRST) {
        directions.add(SqlKind.NULLS_FIRST);
      } else {
        directions.add(SqlKind.NULLS_LAST);
      }
      list.add(new RexFieldCollation(expr, directions));
    }
  }

  private static @Nullable RexWindowExclusion toRexWindowExclusion(
      @Nullable Map<String, Object> map) {
    if (map == null) {
      return null;
    }
    final String type = get(map, "type");
    switch (type) {
      case "CURRENT_ROW":
        return RexWindowExclusion.EXCLUDE_CURRENT_ROW;
      case "GROUP":
        return RexWindowExclusion.EXCLUDE_GROUP;
      case "TIES":
        return RexWindowExclusion.EXCLUDE_TIES;
      case "NO OTHERS":
        return RexWindowExclusion.EXCLUDE_NO_OTHER;
      default:
        throw new UnsupportedOperationException(
            "cannot convert " + type + " to rex window exclusion");
    }
  }

}
