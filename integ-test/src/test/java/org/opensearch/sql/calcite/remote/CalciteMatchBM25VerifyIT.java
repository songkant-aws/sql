/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Hard-evidence verification that PPL {@code | where match(...) | sort -_score | head N} actually
 * ranks results by Lucene BM25Similarity on the OpenSearch backing cluster — not classic TF-IDF,
 * and not an unscored filter.
 *
 * <p>This matters because the PPL-federation value proposition ("use OpenSearch BM25 to pick the
 * most relevant N entities, then enrich their facts in ClickHouse") depends on it. If a future
 * change silently demotes {@code match} into a filter-context clause, or the cluster is configured
 * with a non-BM25 similarity, this test surfaces it.
 *
 * <p>Evidence chain:
 *
 * <ol>
 *   <li>{@code GET /{index}/_settings} and {@code _mapping} — confirms no custom similarity is
 *       configured on the field, so it uses the cluster default.
 *   <li>{@code GET /{index}/_explain/{id}} — Lucene prints the full scoring formula. The presence
 *       of {@code k1 * (1 - b + b * dl / avgdl)} in the tf term, with {@code k1=1.2, b=0.75}, is
 *       the BM25 signature. Classic TF-IDF has no k1/b parameters and uses {@code sqrt(freq)} for
 *       tf and {@code log(N/n) + 1} for idf.
 *   <li>PPL-side {@code _score} values match the raw DSL {@code _score} values byte-for-byte,
 *       proving PPL does not intercept or remap scoring.
 * </ol>
 *
 * <p>If this test starts failing, before assuming a regression, check whether a future OpenSearch
 * release changed default similarity parameters (unlikely — k1=1.2, b=0.75 have been Lucene's
 * default since 2014).
 */
public class CalciteMatchBM25VerifyIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK);
  }

  @Test
  public void verify_bm25_is_used() throws IOException {
    // 1. Index settings / mapping — confirm no custom similarity override.
    Response settingsResp =
        client().performRequest(new Request("GET", "/" + TEST_INDEX_BANK + "/_settings"));
    String settingsBody = new String(settingsResp.getEntity().getContent().readAllBytes());
    Response mappingResp =
        client().performRequest(new Request("GET", "/" + TEST_INDEX_BANK + "/_mapping"));
    String mappingBody = new String(mappingResp.getEntity().getContent().readAllBytes());
    // If either body ever mentions "similarity" it means a non-default is configured and the
    // subsequent BM25-signature check no longer proves cluster-default behavior.
    assertThat(
        "index settings must not declare a custom similarity",
        settingsBody.contains("\"similarity\""),
        equalTo(false));
    assertThat(
        "index mapping must not declare a custom similarity on any field",
        mappingBody.contains("\"similarity\""),
        equalTo(false));

    // 2. Lucene _explain — the scoring formula reveals the similarity algorithm.
    Request explain = new Request("POST", "/" + TEST_INDEX_BANK + "/_explain/6");
    explain.setJsonEntity("{\"query\": {\"match\": {\"address\": \"Street\"}}}");
    Response expResp = client().performRequest(explain);
    String expBody = new String(expResp.getEntity().getContent().readAllBytes());
    // BM25 signature: the tf term uses k1 and b parameters with Lucene's defaults (1.2 / 0.75).
    // Classic TF-IDF has neither parameter, so matching these strings refutes classic.
    assertThat(
        "Lucene _explain must describe BM25 tf formula",
        expBody,
        containsString("freq / (freq + k1 * (1 - b + b * dl / avgdl))"));
    assertThat(
        "BM25 k1 default must be 1.2",
        expBody,
        containsString("\"value\":1.2,\"description\":\"k1, term saturation parameter\""));
    assertThat(
        "BM25 b default must be 0.75",
        expBody,
        containsString("\"value\":0.75,\"description\":\"b, length normalization parameter\""));
    assertThat(
        "Lucene _explain must describe BM25 idf formula",
        expBody,
        containsString("log(1 + (N - n + 0.5) / (n + 0.5))"));

    // 3. PPL _score matches raw DSL _score byte-for-byte — proves PPL preserves scoring.
    Request dsl = new Request("POST", "/" + TEST_INDEX_BANK + "/_search");
    dsl.setJsonEntity(
        "{\"size\": 5, "
            + "\"query\": {\"match\": {\"address\": \"Street\"}}, "
            + "\"sort\": [{\"_score\": {\"order\": \"desc\"}}], "
            + "\"track_scores\": true, "
            + "\"_source\": [\"address\"]}");
    JSONObject dslJson =
        new JSONObject(
            new String(client().performRequest(dsl).getEntity().getContent().readAllBytes()));
    JSONArray dslHits = dslJson.getJSONObject("hits").getJSONArray("hits");

    JSONObject pplResult =
        executeQuery(
            String.format(
                "source=%s | where match(address, 'Street') | sort -_score | fields _score,"
                    + " address | head 5",
                TEST_INDEX_BANK));
    JSONArray pplRows = pplResult.getJSONArray("datarows");

    assertThat(
        "PPL and raw DSL must return the same number of hits",
        pplRows.length(),
        equalTo(dslHits.length()));
    for (int i = 0; i < pplRows.length(); i++) {
      Object pplScore = pplRows.getJSONArray(i).get(0);
      Object dslScore = dslHits.getJSONObject(i).get("_score");
      assertThat(
          "row " + i + ": PPL _score must equal raw DSL _score (same BM25 number)",
          pplScore.toString(),
          equalTo(dslScore.toString()));
    }
  }
}
