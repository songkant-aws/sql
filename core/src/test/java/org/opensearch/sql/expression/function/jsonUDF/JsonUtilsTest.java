/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.junit.jupiter.api.Test;

public class JsonUtilsTest {

  @Test
  public void testConvertToJsonPath_simplePath() {
    assertEquals("$.a.b.c", JsonUtils.convertToJsonPath("a.b.c"));
  }

  @Test
  public void testConvertToJsonPath_nullInput() {
    assertEquals("$", JsonUtils.convertToJsonPath(null));
  }

  @Test
  public void testConvertToJsonPath_emptyInput() {
    assertEquals("$", JsonUtils.convertToJsonPath(""));
  }

  @Test
  public void testConvertToJsonPath_alreadyJsonPath() {
    assertEquals("$.a.b", JsonUtils.convertToJsonPath("$.a.b"));
  }

  @Test
  public void testConvertToJsonPath_alreadyJsonPathRoot() {
    assertEquals("$", JsonUtils.convertToJsonPath("$"));
  }

  @Test
  public void testConvertToJsonPath_alreadyJsonPathWithBrackets() {
    assertEquals("$.a[0].b", JsonUtils.convertToJsonPath("$.a[0].b"));
  }

  @Test
  public void testConvertToJsonPath_alreadyJsonPathWithWildcard() {
    assertEquals("$.a[*].b", JsonUtils.convertToJsonPath("$.a[*].b"));
  }

  @Test
  public void testConvertToJsonPath_curlyBraceWildcard() {
    assertEquals("$.a[*].b", JsonUtils.convertToJsonPath("a{}.b"));
  }

  @Test
  public void testConvertToJsonPath_curlyBraceIndex() {
    assertEquals("$.a[2].b", JsonUtils.convertToJsonPath("a{2}.b"));
  }

  @Test
  public void testExpandJsonPath_alreadyJsonPath() {
    JsonNode root = JsonUtils.convertInputToJsonNode("{\"a\":{\"b\":1}}");
    List<String> result = JsonUtils.expandJsonPath(root, "$.a.b");
    assertEquals(List.of("$.a.b"), result);
  }

  @Test
  public void testExpandJsonPath_withWildcard() {
    JsonNode root = JsonUtils.convertInputToJsonNode("{\"a\":[1,2,3]}");
    List<String> result = JsonUtils.expandJsonPath(root, "$.a[*]");
    assertEquals(List.of("$.a[0]", "$.a[1]", "$.a[2]"), result);
  }
}
