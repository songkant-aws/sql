/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.clickhouse.calcite.federation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

/**
 * Smoke test for idempotent federation rule registration. Real behavioural validation (i.e. the
 * rule actually firing during Volcano planning) happens in ClickHouseFederationIT; here we only
 * verify that invoking the hook repeatedly is safe.
 */
class CalciteFederationRegistrationTest {
  @Test
  void ensureRegisteredIsIdempotent() {
    assertDoesNotThrow(
        () -> {
          CalciteFederationRegistration.ensureRegistered();
          CalciteFederationRegistration.ensureRegistered();
        });
  }
}
