/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.storage.serialization;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.linq4j.Nullness.castNonNull;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlInsertKeyword;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlJsonQueryWrapperBehavior;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class OpenSearchRelEnumTypes {

  private OpenSearchRelEnumTypes() {}

  private static final ImmutableMap<String, Enum<?>> ENUM_BY_NAME;

  static {
    // Build a mapping from enum constants (e.g. LEADING) to the enum
    // that contains them (e.g. SqlTrimFunction.Flag). If there two
    // enum constants have the same name, the builder will throw.
    final ImmutableMap.Builder<String, Enum<?>> enumByName =
        ImmutableMap.builder();
    register(enumByName, JoinConditionType.class);
    register(enumByName, JoinType.class);
    register(enumByName, RexUnknownAs.class);
    register(enumByName, SqlExplain.Depth.class);
    register(enumByName, SqlExplainFormat.class);
    register(enumByName, SqlExplainLevel.class);
    register(enumByName, SqlInsertKeyword.class);
    register(enumByName, SqlJsonConstructorNullClause.class);
    register(enumByName, SqlJsonQueryWrapperBehavior.class);
    register(enumByName, SqlJsonValueEmptyOrErrorBehavior.class);
    register(enumByName, SqlMatchRecognize.AfterOption.class);
    register(enumByName, SqlSelectKeyword.class);
    register(enumByName, SqlTrimFunction.Flag.class);
    register(enumByName, TimeUnitRange.class);
    register(enumByName, TableModify.Operation.class);
    ENUM_BY_NAME = enumByName.build();
  }

  private static void register(ImmutableMap.Builder<String, Enum<?>> builder,
      Class<? extends Enum> aClass) {
    for (Enum enumConstant : castNonNull(aClass.getEnumConstants())) {
      builder.put(enumConstant.name(), enumConstant);
    }
  }

  /** Converts a literal into a value that can be serialized to JSON.
   * In particular, if is an enum, converts it to its name. */
  public static @Nullable Object fromEnum(@Nullable Object value) {
    return value instanceof Enum ? fromEnum((Enum) value) : value;
  }

  /** Converts an enum into its name.
   * Throws if the enum's class is not registered. */
  public static String fromEnum(Enum enumValue) {
    if (ENUM_BY_NAME.get(enumValue.name()) != enumValue) {
      throw new AssertionError("cannot serialize enum value to JSON: "
          + enumValue.getDeclaringClass().getCanonicalName() + "."
          + enumValue);
    }
    return enumValue.name();
  }

  /** Converts a string to an enum value.
   * The converse of {@link #fromEnum(Enum)}.
   *
   * @throws NullPointerException if there is no corresponding registered {@link Enum}
   * */
  static <E extends Enum<E>> @NonNull E toEnum(String name) {
    return (E) requireNonNull(ENUM_BY_NAME.get(name));
  }
}
