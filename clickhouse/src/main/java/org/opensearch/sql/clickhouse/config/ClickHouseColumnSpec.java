/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.clickhouse.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ClickHouseColumnSpec {
  @JsonProperty("name") private String name;
  @JsonProperty("ch_type") private String chType;
  @JsonProperty("expr_type") private String exprType;
}
