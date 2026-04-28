# Bug: SideInputInListRule emits `WHERE parent_asin IN (NULL)` at runtime

## TL;DR

`SideInputInListRule` fires correctly at plan time (physical plan contains `JdbcSideInputFilter` with `ARRAY_IN($0, ?0)`), but at runtime the array parameter `?0` is bound to `[NULL]` instead of the actual left-side distinct keys. The SQL sent to ClickHouse is:

```sql
SELECT `parent_asin`, `rating`
FROM `fed`.`reviews`
WHERE `parent_asin` IN (NULL) AND `timestamp` > 1640995200000
LIMIT 50000
```

Which returns zero rows because `col IN (NULL)` is never true in SQL three-valued logic.

The same query with the rule bailed out (via left-side `head 100000 > threshold`) returns the expected ~115 rows and reads ~550K rows from CH.

## Environment

- Branch: `feat/ppl-federation` at commit `d015c3148` or later
- OpenSearch: 3.6.0 release Docker image + plugin built from this branch
- Plugin build: `./gradlew :opensearch-sql-plugin:bundlePlugin -Dopensearch.version=3.6.0 -Dbuild.snapshot=false`
- ClickHouse: `clickhouse/clickhouse-server:24.8`, `clickhouse-jdbc:0.6.5`
- Single-node each; not cluster

## Test data

### OpenSearch index `products`

Amazon Reviews 2023 Home_and_Kitchen metadata subset is enough; **any index with a `title` text field and a `parent_asin` keyword field will do**. Minimum 50 docs matching the BM25 query below.

Index mapping (`products-mapping.json`):

```json
{
  "settings": {
    "index": {
      "number_of_shards": 3,
      "number_of_replicas": 0
    }
  },
  "mappings": {
    "properties": {
      "parent_asin":    { "type": "keyword" },
      "title":          { "type": "text" },
      "description":    { "type": "text" },
      "features":       { "type": "text" },
      "categories":     { "type": "keyword" },
      "main_category":  { "type": "keyword" },
      "store":          { "type": "keyword" },
      "average_rating": { "type": "float" },
      "rating_number":  { "type": "integer" },
      "price":          { "type": "float" }
    }
  }
}
```

For a minimal repro you can synthesize 50-100 products locally:

```bash
for i in $(seq 1 100); do
  asin="B$(printf '%09d' $i)"
  curl -s -X POST localhost:9200/products/_doc \
    -H 'Content-Type: application/json' \
    -d "{\"parent_asin\":\"$asin\",\"title\":\"stainless steel cookware set $i\"}"
done
curl -s -X POST localhost:9200/products/_refresh
```

### ClickHouse table `fed.reviews`

Minimum shape (matches what CalciteFederationIT scale-tests use):

```sql
CREATE DATABASE IF NOT EXISTS fed;

CREATE TABLE fed.reviews (
    parent_asin       String,
    asin              String,
    user_id           String,
    rating            Float32,
    helpful_vote      Int32,
    timestamp         Int64,
    verified_purchase UInt8,
    title             String,
    text              String
) ENGINE = MergeTree()
ORDER BY (parent_asin, timestamp)
SETTINGS index_granularity = 8192;
```

Seed at minimum 10-20 reviews whose `parent_asin` matches the product asins above, and `timestamp > 1640995200000` (2022-01-01 in epoch ms):

```sql
INSERT INTO fed.reviews VALUES
  ('B000000001','B000000001','user_a',5.0,0,1700000000000,1,'great','...'),
  ('B000000002','B000000002','user_b',4.0,0,1700000000000,1,'ok','...'),
  ...
```

### OpenSearch CLICKHOUSE datasource registration

```bash
curl -s -X POST 'http://localhost:9200/_plugins/_query/_datasources' \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "ch",
    "connector": "CLICKHOUSE",
    "properties": {
      "clickhouse.uri": "jdbc:ch://<CH_HOST>:8123/default?compress=0",
      "clickhouse.auth.type": "basic",
      "clickhouse.auth.username": "default",
      "clickhouse.auth.password": "<password>",
      "clickhouse.schema": "{\"databases\":[{\"name\":\"fed\",\"tables\":[{\"name\":\"reviews\",\"columns\":[{\"name\":\"parent_asin\",\"ch_type\":\"String\",\"expr_type\":\"STRING\"},{\"name\":\"asin\",\"ch_type\":\"String\",\"expr_type\":\"STRING\"},{\"name\":\"user_id\",\"ch_type\":\"String\",\"expr_type\":\"STRING\"},{\"name\":\"rating\",\"ch_type\":\"Float32\",\"expr_type\":\"FLOAT\"},{\"name\":\"helpful_vote\",\"ch_type\":\"Int32\",\"expr_type\":\"INTEGER\"},{\"name\":\"timestamp\",\"ch_type\":\"Int64\",\"expr_type\":\"LONG\"},{\"name\":\"verified_purchase\",\"ch_type\":\"UInt8\",\"expr_type\":\"BOOLEAN\"},{\"name\":\"title\",\"ch_type\":\"String\",\"expr_type\":\"STRING\"},{\"name\":\"text\",\"ch_type\":\"String\",\"expr_type\":\"STRING\"}]}]}]}"
    }
  }'
```

Note: the plugin requires `plugins.query.datasources.encryption.masterkey` set to **exactly 32 characters** in `opensearch.yml`. A base64-of-32-bytes key is 44 chars and fails with `java.security.InvalidKeyException: Invalid AES key length: 44 bytes`. Use something like:

```bash
head -c 24 /dev/urandom | base64 | tr -d '=+/' | cut -c1-32
```

## Failing query (repro)

```ppl
source=products | where match(title, 'stainless steel cookware') | sort -_score | head 50
| inner join left=p right=r on p.parent_asin = r.parent_asin
  [ source=ch.fed.reviews
    | where timestamp > 1640995200000
    | fields parent_asin, rating ]
| stats avg(rating) as avg_r, count() as n by parent_asin
```

## Expected vs actual

### Expected

- At most 50 rows returned, one row per distinct `parent_asin` that has ≥1 review after 2022-01-01
- CH `system.query_log` shows a query of the form `... WHERE parent_asin IN ('B000...','B000...',...)` with the actual 50 ASINs

### Actual

Result set is empty (`rows: 0`).

CH `system.query_log` shows:

```sql
SELECT `parent_asin`, `rating`
FROM `fed`.`reviews`
WHERE `parent_asin` IN (NULL) AND `timestamp` > 1640995200000
LIMIT 50000
```

`read_rows: 0`, latency ~200ms.

## Control: same query with `head 100000` bypasses the rule

```ppl
source=products | where match(title, 'stainless steel cookware') | sort -_score | head 100000
| inner join left=p right=r on p.parent_asin = r.parent_asin
  [ source=ch.fed.reviews
    | where timestamp > 1640995200000
    | fields parent_asin, rating ]
| stats avg(rating) as avg_r, count() as n by parent_asin
```

- Explain: no `ARRAY_IN` in physical plan (rule bails, left bound > threshold)
- Result: ~115 rows, correct
- CH `system.query_log`: `... WHERE timestamp > 1640995200000 LIMIT 50000`, `read_rows: ~548,864`

This confirms the data + schema + datasource wiring are correct; only the rule's runtime binding is broken.

## Explain output (confirms rule fired)

### Logical plan

```
LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])
  LogicalProject(avg_r=[$1], n=[$2], parent_asin=[$0])
    LogicalAggregate(group=[{0}], avg_r=[AVG($1)], n=[COUNT()])
      LogicalProject(parent_asin=[$1], rating=[$11])
        LogicalJoin(condition=[=($1, $10)], joinType=[inner])
          LogicalProject(features=[$0], parent_asin=[$1], ..., title=[$9])
            LogicalSort(sort0=[$12], dir0=[DESC-nulls-last], fetch=[50])
              LogicalFilter(condition=[match(MAP('field', $9), MAP('query', 'stainless steel cookware':VARCHAR))])
                CalciteLogicalIndexScan(table=[[OpenSearch, products]])
          LogicalSystemLimit(fetch=[50000], type=[JOIN_SUBSEARCH_MAXOUT])
            LogicalProject(parent_asin=[$0], rating=[$3])
              LogicalFilter(condition=[>($5, 1640995200000)])
                JdbcTableScan(table=[[ClickHouse, ch, fed, reviews]])
```

### Physical plan

```
EnumerableLimit(fetch=[10000])
  EnumerableCalc(..., avg_r=[...], n=[...], parent_asin=[...])
    EnumerableAggregate(group=[{0}], agg#0=[$SUM0($2)], agg#1=[COUNT()])
      EnumerableHashJoin(condition=[=($0, $1)], joinType=[inner])
        CalciteEnumerableIndexScan(table=[[OpenSearch, products]],
            PushDownContext=[..., FILTER->match(...), SORT->..., LIMIT->50, PROJECT->[parent_asin]],
            requestedTotalSize=50)
        JdbcToEnumerableConverter
          JdbcSort(fetch=[50000])
            JdbcProject(parent_asin=[$0], rating=[$3])
              JdbcFilter(condition=[AND(ARRAY_IN($0, ?0), >($5, 1640995200000))])
                JdbcTableScan(table=[[ClickHouse, ch, fed, reviews]])
```

`ARRAY_IN($0, ?0)` is present → `SideInputInListRule` fired successfully at plan time.
The bug is in the runtime binding of `?0`.

## Suspect code paths

The runtime binding lives in:

- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/federation/SideInputBindableWrapper.java`
  - `bindWithSideInput(Bindable, DataContext)` — orchestrates drain + bind
  - `execLeft(RelNode, DataContext)` — runs the left enumerable
  - `extractLeftKeyIndex(Join)` — derives which left column is the join key
  - `bindInListParam(DataContext, Collection)` — installs the array under `"?0"`
- `clickhouse/src/main/java/org/opensearch/sql/clickhouse/calcite/federation/SideInputDrainEnumerable.java`
  - `drain(Enumerable<Object[]>, int keyIdx, long maxSize)` — collects distinct keys

### Hypothesis 1: `execLeft` doesn't produce the correct enumerable

The physical plan shows the left side is a `CalciteEnumerableIndexScan` with a pushdown context that contains the BM25 filter + sort + limit. If `execLeft` constructs its own bindable from the left `RelNode` but doesn't thread `DataContext` through correctly, the OS pushdown executor may produce zero rows.

Worth printing `leftEnum.count()` or iterating one batch in debug before drain.

### Hypothesis 2: `extractLeftKeyIndex` returns a wrong column index

If it returns an index whose column is not `parent_asin` on the left (e.g. the `title` column), `drain` pulls nulls/empties because the row at that index on the OS-side projection may be all nulls if the planner re-projected.

Check the `LogicalProject` at the top of the left subtree:

```
LogicalProject(features=[$0], parent_asin=[$1], ..., title=[$9])
```

`parent_asin` is at index 1 in the projection. Join condition is `=($1, $10)` where $10 is the right side. So left key index is 1. But at the physical level the scan's pushdown has already stripped fields (`PROJECT->[parent_asin]`), so at runtime the index might be 0, not 1. If `extractLeftKeyIndex` returns 1 and the runtime row has only `[parent_asin]`, accessing index 1 on a length-1 row yields null.

This looks like the most likely root cause.

### Hypothesis 3: `drain` produces the right keys but `bindInListParam` loses them

Check that the collection passed to `bindInListParam` is the same object returned by `drain`. Also check `SqlArrayBinding.wrapAsSqlArrayIfPossible` — if the JDBC Connection-based path silently fails and falls back to a raw Object[], the array may be empty.

The SQL shape `IN (NULL)` (single literal) suggests the array passed to the driver has length 1 with a single null element, rather than length 0. That points at `wrapAsSqlArrayIfPossible` or the Object[] fallback producing `[null]` when the input collection has distinct keys but they're all null — consistent with Hypothesis 2 (pulling the wrong column yields all nulls, which dedupe to `{null}`).

## Recommended debugging approach

### Quick (printf)

Add `System.err.println` to five points and rebuild:

```java
// SideInputBindableWrapper.bindWithSideInput (around line 105)
System.err.println("[SideInput] entered, join.getCondition=" + join.getCondition()
    + ", left.rowType=" + join.getLeft().getRowType());

// around line 119 (after extractLeftKeyIndex)
System.err.println("[SideInput] leftKeyIdx=" + leftKeyIdx
    + ", left.rowType.fieldList=" + join.getLeft().getRowType().getFieldList());

// around line 148 (after execLeft)
// Turn the Enumerable into a list for inspection; remove in production.
java.util.List<Object[]> leftRows = leftEnum.toList();
System.err.println("[SideInput] leftEnum yielded " + leftRows.size() + " rows; sample row[0]="
    + (leftRows.isEmpty() ? "empty" : java.util.Arrays.toString(leftRows.get(0))));
// rewrap to a fresh Enumerable so downstream drain still works:
leftEnum = org.apache.calcite.linq4j.Linq4j.asEnumerable(leftRows);

// around line 150 (after drain)
System.err.println("[SideInput] drained distinctKeys size=" + drained.distinctKeys().size()
    + ", sample=" + (drained.distinctKeys().isEmpty() ? "empty" : drained.distinctKeys().iterator().next()));

// around line 184 (in bindInListParam)
System.err.println("[SideInput] binding ?0 => " + drainedKeysCollection);
```

Then:

```bash
./gradlew :opensearch-sql-plugin:bundlePlugin \
  -Dopensearch.version=3.6.0 -Dbuild.snapshot=false
# reinstall plugin, restart OS, rerun the PPL query
docker logs os 2>&1 | grep '\[SideInput\]'
```

### Deeper (IDE remote debug)

Launch OS docker with JVM debug port:

```
-e "OPENSEARCH_JAVA_OPTS=-Xms8g -Xmx8g -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
-p 127.0.0.1:5005:5005
```

SSH tunnel `-L 5005:localhost:5005`, attach IDE remote debugger to `localhost:5005`, break in `bindWithSideInput`.

### IT-based (fully local, no EC2)

The repo has `integ-test/src/test/java/org/opensearch/sql/clickhouse/ClickHouseFederationIT.java` with `testBoundedLeftJoinAggregatedCh` — a happy-path test using a real CH testcontainer. That test currently passes (uses **inner-aggregate** shape), so either:

- The inner-agg vs outer-agg shape in this bug triggers a different runtime code path, or
- The test's assertion checks happen to be satisfied even with IN (NULL) (e.g. only asserts that the CH query_log contains "IN" pattern, not that rows returned match the expected ASINs)

A new test case with the outer-aggregate shape above should reproduce, locally, without needing EC2 or a remote CH. Run:

```bash
./gradlew :integ-test:integTest \
  --tests "org.opensearch.sql.clickhouse.ClickHouseFederationIT" \
  -PuseClickhouseBinary=true
```

The repro in an IT gives you breakpoints with full stack in the IDE, which is probably faster than the printf loop.

## Success criterion for the fix

CH `system.query_log` should show the SQL with the real 50 asins:

```sql
WHERE `parent_asin` IN ('B000000001', 'B000000002', ..., 'B000000050')
  AND `timestamp` > 1640995200000
```

And the PPL query should return the correct set of rows (same row count as the `head 100000` bypass version, assuming the top-50 BM25 subset covers all distinct asins with reviews — at minimum non-zero).
