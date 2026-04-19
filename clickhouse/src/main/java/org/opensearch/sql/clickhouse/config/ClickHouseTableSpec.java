/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ClickHouseTableSpec {
  @JsonProperty("name") private String name;
  @JsonProperty("columns") private List<ClickHouseColumnSpec> columns;

  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Database {
    @JsonProperty("name") private String name;
    @JsonProperty("tables") private List<ClickHouseTableSpec> tables;
  }

  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Schema {
    @JsonProperty("databases") private List<Database> databases;
  }
}
