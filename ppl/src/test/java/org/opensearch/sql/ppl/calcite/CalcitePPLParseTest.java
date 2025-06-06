/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLParseTest extends CalcitePPLAbstractTest {
  public CalcitePPLParseTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testParse() {
    String ppl =
        "source=EMP | parse DATE_FORMAT(HIREDATE, '%Y-%m-%d') '(?<year>\\d{4})-\\d{2}-\\d{2}' |"
            + " fields JOB, year";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(JOB=[$2], year=[REGEXP_EXTRACT(DATE_FORMAT($4, '%Y-%m-%d':VARCHAR),"
            + " '(?<year>\\d{4})-\\d{2}-\\d{2}':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `JOB`, REGEXP_EXTRACT(`DATE_FORMAT`(`HIREDATE`, '%Y-%m-%d'),"
            + " '(?<year>\\d{4})-\\d{2}-\\d{2}') `year`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testParseOverriding() {
    String ppl =
        "source=EMP | parse DATE_FORMAT(HIREDATE, '%Y-%m-%d') '(?<MGR>\\d{4})-\\d{2}-\\d{2}' |"
            + " fields JOB, MGR";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(JOB=[$2], MGR=[REGEXP_EXTRACT(DATE_FORMAT($4, '%Y-%m-%d':VARCHAR),"
            + " '(?<MGR>\\d{4})-\\d{2}-\\d{2}':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `JOB`, REGEXP_EXTRACT(`DATE_FORMAT`(`HIREDATE`, '%Y-%m-%d'),"
            + " '(?<MGR>\\d{4})-\\d{2}-\\d{2}') `MGR`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
