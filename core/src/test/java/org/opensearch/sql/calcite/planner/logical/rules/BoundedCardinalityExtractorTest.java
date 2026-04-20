/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.planner.logical.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BoundedCardinalityExtractorTest {

  private RelBuilder builder;

  @BeforeEach
  public void setUp() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add(
        "T",
        new AbstractTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory f) {
            return f.builder().add("id", SqlTypeName.BIGINT).add("v", SqlTypeName.VARCHAR).build();
          }
        });
    FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(rootSchema).build();
    builder = RelBuilder.create(config);
  }

  @Test
  public void sortFetchGivesFetchValue() {
    RelNode plan = builder.scan("T").sort(0).limit(0, 100).build();
    Optional<Long> bound = BoundedCardinalityExtractor.extract(plan);
    assertTrue(bound.isPresent());
    assertEquals(Long.valueOf(100L), bound.get());
  }

  @Test
  public void nestedLimitTakesOuterFetch() {
    RelNode plan = builder.scan("T").limit(0, 200).limit(0, 50).build();
    Optional<Long> bound = BoundedCardinalityExtractor.extract(plan);
    assertTrue(bound.isPresent());
    assertEquals(Long.valueOf(50L), bound.get());
  }

  @Test
  public void projectIsTransparent() {
    RelNode plan = builder.scan("T").limit(0, 10).project(builder.field("id")).build();
    Optional<Long> bound = BoundedCardinalityExtractor.extract(plan);
    assertTrue(bound.isPresent());
    assertEquals(Long.valueOf(10L), bound.get());
  }

  @Test
  public void filterWithoutPkIsTransparent() {
    RelNode plan =
        builder
            .scan("T")
            .limit(0, 25)
            .filter(builder.equals(builder.field("v"), builder.literal("x")))
            .build();
    Optional<Long> bound = BoundedCardinalityExtractor.extract(plan);
    assertTrue(bound.isPresent());
    assertEquals(Long.valueOf(25L), bound.get());
  }

  @Test
  public void bareScanIsNotBounded() {
    RelNode plan = builder.scan("T").build();
    Optional<Long> bound = BoundedCardinalityExtractor.extract(plan);
    assertFalse(bound.isPresent());
  }

  @Test
  public void aggregateIsNotBounded() {
    RelNode plan =
        builder.scan("T").aggregate(builder.groupKey("id"), builder.count(false, "c")).build();
    Optional<Long> bound = BoundedCardinalityExtractor.extract(plan);
    assertFalse(bound.isPresent());
  }
}
