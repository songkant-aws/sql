# PPL `match` uses BM25

## Claim

```
source=X | where match(field, "term") | sort -_score | head N
```

returns the **top-N most relevant documents ranked by Lucene BM25 similarity** — not classic
TF-IDF, not an unscored filter. This is load-bearing for the PPL-federation story ("use
OpenSearch BM25 to pick the most relevant N entities, then enrich their facts in ClickHouse via
IN-list pushdown").

## Evidence

Verified by `CalciteMatchBM25VerifyIT` on a clean OpenSearch test cluster (no custom similarity
configured on the index or field). The test drives three independent probes:

1. **`GET /{index}/_settings` + `_mapping`** — confirms no `"similarity"` override on the field,
   so it uses the cluster default.
2. **`GET /{index}/_explain/{id}`** — Lucene prints the full scoring formula. For the `match`
   query on `address:"Street"` against doc 6, the explain output is:

   ```
   weight(address:street in 1) [PerFieldSimilarity], result of:
     score(freq=1.0), computed as boost * idf * tf from:
       idf, computed as log(1 + (N - n + 0.5) / (n + 0.5))   from N=7, n=3   → 0.8267
       tf,  computed as freq / (freq + k1 * (1 - b + b * dl / avgdl))
         k1 = 1.2   (term saturation parameter)
         b  = 0.75  (length normalization parameter)
         dl = avgdl = 3.0
       → 0.4545
     final = 0.8267 × 0.4545 = 0.37576297
   ```

   The presence of `k1` and `b` parameters is the BM25 signature. Classic TF-IDF has neither —
   it uses `sqrt(freq)` for tf and `log(N/n) + 1` for idf. `k1=1.2, b=0.75` are Lucene's
   `BM25Similarity` defaults (unchanged since 2014).

3. **PPL-side `_score` matches raw DSL `_score` byte-for-byte.** PPL does not intercept, rescore,
   or remap Lucene scores. Same BM25 number.

## Required PPL shape

- `| sort -_score` is **required**. Without it, Calcite emits `sort(_doc ASC)` in the DSL and
  results come back in internal doc-ID order, not relevance order. (The Calcite engine does
  propagate scoring-aware sort when `-_score` is explicit — see `CalciteLogicalIndexScan.java`
  and the `SORT->[{"_score":{"order":"desc"}}]` in the physical plan dump.)
- `score(match(...), boost)` **is not valid PPL grammar**. The legacy SQL engine accepted it to
  force `track_scores:true`; PPL has no such wrapper and does not need one.

## What this does NOT cover

- **Non-default similarity settings.** If someone configures an index with
  `index.similarity.default.type: classic` or per-field `similarity: boolean`, this guarantee
  breaks. The IT asserts the test cluster has no such override; production deployments should
  verify the same.
- **The 50 000-row system limit.** `sort -_score | head N` is still bounded upstream by
  `plugins.ppl.join.subsearch_maxout` / `QUERY_SIZE_LIMIT`. Relevant for federation: the
  IN-list pushdown rule only fires when the left side's bound is provable; `| head N` satisfies
  this via `BoundedCardinalityExtractor.extract(Sort(fetch=N))`.

## When to re-run

- After any Calcite version bump (scoring-aware sort paths).
- After any change to `CalciteLogicalIndexScan`, `OpenSearchRequestBuilder`, or `PredicateAnalyzer`
  that touches how `match` is pushed down.
- If an OpenSearch release ever changes `BM25Similarity` defaults (this has not happened in
  Lucene's history and is extremely unlikely).

Run: `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.CalciteMatchBM25VerifyIT"`
