/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.calcite.linq4j.Linq4j;
import org.junit.jupiter.api.Test;

class SideInputDrainEnumerableTest {

  @Test
  void drainsAndDedupesKeys() {
    List<Object[]> input =
        List.of(new Object[] {1L, "a"}, new Object[] {2L, "b"}, new Object[] {1L, "c"});

    SideInputDrainEnumerable.Result result =
        SideInputDrainEnumerable.drain(Linq4j.asEnumerable(input), /*keyCol=*/ 0, /*threshold=*/ 10);

    assertEquals(3, result.rows().size());
    // LinkedHashSet preserves insertion order — 1L first, then 2L.
    assertArrayEquals(new Object[] {1L, 2L}, result.distinctKeys());
  }

  @Test
  void bailsWhenOverThreshold() {
    List<Object[]> input =
        List.of(new Object[] {1L, "a"}, new Object[] {2L, "b"}, new Object[] {3L, "c"});

    assertThrows(
        SideInputBailout.class,
        () ->
            SideInputDrainEnumerable.drain(
                Linq4j.asEnumerable(input), /*keyCol=*/ 0, /*threshold=*/ 2));
  }

  @Test
  void emptyLeftGivesEmptyKeys() {
    List<Object[]> input = List.of();

    SideInputDrainEnumerable.Result result =
        SideInputDrainEnumerable.drain(Linq4j.asEnumerable(input), /*keyCol=*/ 0, /*threshold=*/ 10);

    assertEquals(0, result.rows().size());
    assertEquals(0, result.distinctKeys().length);
  }
}
