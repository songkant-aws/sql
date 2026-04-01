/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.utils.ComparisonUtil.compare;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class ExprCollectionValueTest {
  @Test
  public void equal_to_itself() {
    ExprValue value = ExprValueUtils.collectionValue(ImmutableList.of(1));
    assertTrue(value.equals(value));
  }

  @Test
  public void collection_compare_int() {
    ExprValue intValue = ExprValueUtils.integerValue(10);
    ExprValue value = ExprValueUtils.collectionValue(ImmutableList.of(1));
    assertFalse(value.equals(intValue));
  }

  @Test
  public void compare_collection_with_different_size() {
    ExprValue value1 = ExprValueUtils.collectionValue(ImmutableList.of(1));
    ExprValue value2 = ExprValueUtils.collectionValue(ImmutableList.of(1, 2));
    assertFalse(value1.equals(value2));
    assertFalse(value2.equals(value1));
  }

  @Test
  public void compare_collection_with_int_object() {
    ExprValue value = ExprValueUtils.collectionValue(ImmutableList.of(1));
    assertFalse(value.equals(1));
  }

  @Test
  public void lexicographic_compare_equal_arrays() {
    ExprValue v1 = ExprValueUtils.collectionValue(Arrays.asList(1, 2, 3));
    ExprValue v2 = ExprValueUtils.collectionValue(Arrays.asList(1, 2, 3));
    assertEquals(0, compare(v1, v2));
  }

  @Test
  public void lexicographic_compare_first_element_less() {
    ExprValue v1 = ExprValueUtils.collectionValue(Arrays.asList(1, 5, 9));
    ExprValue v2 = ExprValueUtils.collectionValue(Arrays.asList(2, 1, 0));
    assertTrue(compare(v1, v2) < 0);
    assertTrue(compare(v2, v1) > 0);
  }

  @Test
  public void lexicographic_compare_second_element_differs() {
    ExprValue v1 = ExprValueUtils.collectionValue(Arrays.asList(1, 2, 3));
    ExprValue v2 = ExprValueUtils.collectionValue(Arrays.asList(1, 3, 0));
    assertTrue(compare(v1, v2) < 0);
    assertTrue(compare(v2, v1) > 0);
  }

  @Test
  public void lexicographic_compare_prefix_shorter_is_less() {
    ExprValue v1 = ExprValueUtils.collectionValue(Arrays.asList(1, 2));
    ExprValue v2 = ExprValueUtils.collectionValue(Arrays.asList(1, 2, 3));
    assertTrue(compare(v1, v2) < 0);
    assertTrue(compare(v2, v1) > 0);
  }

  @Test
  public void lexicographic_compare_empty_arrays() {
    ExprValue v1 = ExprValueUtils.collectionValue(Collections.emptyList());
    ExprValue v2 = ExprValueUtils.collectionValue(Collections.emptyList());
    assertEquals(0, compare(v1, v2));
  }

  @Test
  public void lexicographic_compare_empty_vs_nonempty() {
    ExprValue empty = ExprValueUtils.collectionValue(Collections.emptyList());
    ExprValue nonempty = ExprValueUtils.collectionValue(Arrays.asList(1));
    assertTrue(compare(empty, nonempty) < 0);
    assertTrue(compare(nonempty, empty) > 0);
  }

  @Test
  public void lexicographic_compare_string_arrays() {
    ExprValue v1 =
        new ExprCollectionValue(
            Arrays.asList(new ExprStringValue("apple"), new ExprStringValue("banana")));
    ExprValue v2 =
        new ExprCollectionValue(
            Arrays.asList(new ExprStringValue("apple"), new ExprStringValue("cherry")));
    assertTrue(compare(v1, v2) < 0);
    assertTrue(compare(v2, v1) > 0);
  }

  @Test
  public void compareTo_delegates_to_compare() {
    ExprValue v1 = ExprValueUtils.collectionValue(Arrays.asList(1, 2));
    ExprValue v2 = ExprValueUtils.collectionValue(Arrays.asList(1, 3));
    assertTrue(v1.compareTo(v2) < 0);
  }

  @Test
  public void equal_arrays_same_group() {
    ExprValue v1 = ExprValueUtils.collectionValue(Arrays.asList(1, 2, 3));
    ExprValue v2 = ExprValueUtils.collectionValue(Arrays.asList(1, 2, 3));
    assertTrue(v1.equals(v2));
    assertEquals(v1.hashCode(), v2.hashCode());
  }

  @Test
  public void different_arrays_different_group() {
    ExprValue v1 = ExprValueUtils.collectionValue(Arrays.asList(1, 2, 3));
    ExprValue v2 = ExprValueUtils.collectionValue(Arrays.asList(1, 2, 4));
    assertFalse(v1.equals(v2));
  }
}
