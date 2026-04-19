/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.exception.CalciteUnsupportedException;

public class CalciteRelNodeVisitorTest {

  @Test
  public void visitRelation_does_not_reject_clickhouse_datasource() {
    // CH datasources must not throw CalciteUnsupportedException; they should fall through
    // to the scan path. With a null mock context.relBuilder, we expect NPE (not
    // UnsupportedException).
    DataSourceService dss = mock(DataSourceService.class);
    DataSource ch = mock(DataSource.class);
    when(ch.getConnectorType()).thenReturn(DataSourceType.CLICKHOUSE);
    when(dss.dataSourceExists("my_ch")).thenReturn(true);
    when(dss.getDataSource("my_ch")).thenReturn(ch);

    CalciteRelNodeVisitor visitor = new CalciteRelNodeVisitor(dss);
    Relation node = new Relation(QualifiedName.of("my_ch", "analytics", "events"));

    // Null context forces NPE when relBuilder is accessed — that's fine for this unit test.
    // The key assertion is: it is NOT CalciteUnsupportedException. M0.5 IT verifies full path.
    assertThrows(NullPointerException.class, () -> visitor.visitRelation(node, null));
  }

  @Test
  public void visitRelation_still_rejects_unknown_datasource_type() {
    DataSourceService dss = mock(DataSourceService.class);
    DataSource unknown = mock(DataSource.class);
    when(unknown.getConnectorType()).thenReturn(DataSourceType.PROMETHEUS);
    when(dss.dataSourceExists("promds")).thenReturn(true);
    when(dss.getDataSource("promds")).thenReturn(unknown);

    CalciteRelNodeVisitor visitor = new CalciteRelNodeVisitor(dss);
    Relation node = new Relation(QualifiedName.of("promds", "metric"));

    assertThrows(CalciteUnsupportedException.class, () -> visitor.visitRelation(node, null));
  }
}
