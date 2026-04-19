# ClickHouse Datasource for OpenSearch SQL Plugin — Design Spec

- **Date**: 2026-04-19
- **Status**: Draft (pending user review)
- **Scope**: Read-only ClickHouse connector for PPL via Calcite JdbcSchema pushdown
- **Out of scope**: write path, cross-engine federation, CDC, CH-specific PPL functions, AI routing, vector search, CH cluster topology

---

## 0. Summary

Add a new `clickhouse` Gradle module to the OpenSearch SQL plugin that lets users run PPL queries against a registered ClickHouse datasource. Queries flow through the existing PPL → Calcite pipeline; the connector plugs in a `JdbcConvention` backed by a ClickHouse-specific `SqlDialect`, so Calcite's built-in `JdbcRules` push `where`, `fields`, `eval`, `stats`, `sort`, `head`, `dedup`, `rename` down to ClickHouse automatically. Unsupported expressions fall back to the Enumerable layer via Calcite's native `JdbcToEnumerableConverter`. Schema is explicitly declared at registration time (no runtime discovery, no caching).

The design reuses existing OpenSearch SQL patterns (datasource registration API, `StorageEngine`/`Table` abstractions, audit conventions) and keeps the new code isolated in its own module.

---

## 1. Module Layout & Dependencies

### 1.1 New Gradle module: `clickhouse/`

```
clickhouse/
├── build.gradle
└── src/main/java/org/opensearch/sql/clickhouse/
    ├── ClickHousePlugin.java          # Guice bindings (wired from plugin/)
    ├── storage/
    │   ├── ClickHouseStorageEngine.java    # implements core.StorageEngine
    │   ├── ClickHouseTable.java            # implements core.Table; toRel() -> JdbcTableScan
    │   └── ClickHouseSchemaFactory.java    # declared schema -> Calcite SchemaPlus
    ├── calcite/
    │   ├── ClickHouseConvention.java       # extends JdbcConvention
    │   ├── ClickHouseSqlDialect.java       # extends SqlDialect (CH-specific)
    │   └── ClickHouseRules.java            # (optional) extra rules
    ├── client/
    │   ├── ClickHouseClient.java           # HikariCP DataSource wrapper + rate limiting
    │   └── ClickHouseClientFactory.java
    ├── auth/
    │   └── ClickHouseAuthProvider.java     # basic / TLS / JWT / multi-host
    ├── config/
    │   └── ClickHouseDataSourceConfig.java # connection + schema + rate limits
    ├── metrics/
    │   └── ClickHouseMetrics.java          # pool / query / pushdown / ratelimit
    └── audit/
        └── ClickHouseAuditLogger.java      # async queue -> OS system index
```

Touched existing modules:

- `datasources/` — add `DataSourceType.CLICKHOUSE` and register the connector.
- `plugin/` — wire `ClickHousePlugin` in `OpenSearchPluginModule`.
- `integ-test/` — new subpackage `clickhouse/` with Testcontainers-based ITs.

### 1.2 `clickhouse/build.gradle` dependencies

```gradle
dependencies {
  implementation project(':core')
  implementation project(':datasources')
  implementation 'org.apache.calcite:calcite-core'        // already in core's deps
  implementation 'com.clickhouse:clickhouse-jdbc:0.6.x'   // pin minor; compatible with CH 24.x
  implementation 'com.zaxxer:HikariCP:5.x'
  testImplementation project(':integ-test')
  testImplementation 'org.testcontainers:clickhouse:1.19.7'
}
```

Dependency direction: `plugin → clickhouse → core, datasources`. `clickhouse` does NOT depend on `opensearch` or `prometheus`.

Calcite `JdbcSchema`, `JdbcTable`, `JdbcRules`, `JdbcConvention` live inside `calcite-core` — no separate artifact.

---

## 2. Datasource Registration & Configuration

### 2.1 New `DataSourceType.CLICKHOUSE`

Declared in `datasources` module. Registered via the existing
`POST /_plugins/_query/_datasources` API — same auth and storage (system index) as Prometheus.

### 2.2 Registration payload

```json
{
  "name": "my_ch",
  "connector": "clickhouse",
  "properties": {
    "clickhouse.uri": "jdbc:clickhouse://host1:8123,host2:8123/default",
    "clickhouse.auth.type": "basic",
    "clickhouse.auth.username": "u",
    "clickhouse.auth.password": "p",
    "clickhouse.auth.token": "",
    "clickhouse.tls.enabled": "true",
    "clickhouse.tls.trust_store_path": "/path/to/truststore.jks",
    "clickhouse.tls.trust_store_password": "...",
    "clickhouse.jdbc.socket_timeout": "30000",
    "clickhouse.jdbc.compress": "true",
    "clickhouse.pool.max_size": "10",
    "clickhouse.pool.min_idle": "2",
    "clickhouse.rate_limit.qps": "50",
    "clickhouse.rate_limit.concurrent": "20",
    "clickhouse.slow_query.threshold_ms": "5000",
    "clickhouse.schema": "<JSON string — see 2.3>"
  }
}
```

`clickhouse.auth.type` ∈ `{basic, jwt}`. Exactly one of `{password, token}` must be supplied.

### 2.3 Explicit schema declaration

Schema is declared at registration time (not discovered at runtime, not cached with TTL). Example:

```json
{
  "databases": [{
    "name": "analytics",
    "tables": [{
      "name": "events",
      "columns": [
        {"name": "event_id",   "ch_type": "UInt64",        "expr_type": "LONG"},
        {"name": "ts",         "ch_type": "DateTime64(3)", "expr_type": "TIMESTAMP"},
        {"name": "user_email", "ch_type": "String",        "expr_type": "STRING"},
        {"name": "amount",     "ch_type": "Float64",       "expr_type": "DOUBLE"}
      ]
    }]
  }]
}
```

Note: the `UInt64` row in the example above is illustrative; per §5 it is on the **reject** list, so a registration using it will fail validation. Users who need that column must `CAST` it server-side and expose the casted view.

### 2.4 Validation (`ClickHouseDataSourceConfig.validate()`) — fail-fast at registration

1. URI parseable; at least one host.
2. `auth.type` present; required fields for the chosen type present (either `password` or `token`).
3. Schema JSON structurally valid; every `(ch_type, expr_type)` pair is in the whitelist (§5.1). Any violation → reject, error message lists **all** violating columns at once.
4. Open a one-shot JDBC connection and run `SELECT 1`. Failure → reject with classified error (§6).

### 2.5 PPL usage

```ppl
source = my_ch.analytics.events
| where amount > 10
| stats count() by user_email
```

`my_ch` is the datasource name; `analytics.events` is the CH `database.table`.

---

## 3. Query Execution Pipeline

### 3.1 End-to-end flow

```
PPL: source = my_ch.analytics.events | where amount > 10 | stats count() by user_email
    ↓ PPLSyntaxParser + AstBuilder        (ppl module, existing)
  UnresolvedPlan
    ↓ Analyzer (resolves my_ch.analytics.events via DataSourceService)
  LogicalPlan (root: LogicalRelation → ClickHouseTable)
    ↓ CalciteRelNodeVisitor                (core module, existing)
  RelNode tree (LogicalFilter → LogicalAggregate → TableScan)
    ↓ Planner (VolcanoPlanner)
    ↓ JdbcRules match ClickHouseConvention
  JdbcTableScan → JdbcFilter → JdbcAggregate   (all in JDBC convention)
    ↓ JdbcToEnumerableConverter              (inserted if any local-only op remains)
    ↓ JdbcImplementor.visit()  →  CH SQL string
    ↓ HikariCP DataSource → ClickHouse JDBC
  ResultSet
    ↓ ClickHouseResultSetIterator           (CH JDBC types → ExprValue)
  PhysicalPlan (EnumerableIterator wrapper)
    ↓ local-only operators (if any) continue executing on OS side
  ExecutionEngine → QueryResponse
```

### 3.2 Class responsibilities

| Class | Responsibility |
|---|---|
| `ClickHouseStorageEngine` | Built by `DataSourceService` at registration. Holds `ClickHouseDataSourceConfig`, `ClickHouseClient`, and declared-schema registry. `getTable(name)` returns `ClickHouseTable`. |
| `ClickHouseTable` | Implements `core.Table`. `toRel(ctx)` delegates to Calcite `JdbcTable.toRel()`, producing a `JdbcTableScan` bound to the per-datasource `ClickHouseConvention` instance. |
| `ClickHouseSchemaFactory` | Converts `ClickHouseDataSourceConfig.schema` (explicit declaration) into a Calcite `SchemaPlus`. Each table becomes a `JdbcTable` we construct directly (we do NOT call `JdbcSchema.create()` — that would run JDBC metadata discovery, which §2.3 rejects). |
| `ClickHouseConvention` | `extends JdbcConvention`. Holds a `ClickHouseSqlDialect` instance. `register(planner)` registers `JdbcRules.RULES` scoped to this `VolcanoPlanner` instance — no global `RelOptRules` mutation. |
| `ClickHouseSqlDialect` | `extends SqlDialect`. Overrides identifier quoting (backticks), `LIMIT` syntax, function name mapping, type CAST rewriting (see §5.4). Controls which `RexCall`s are pushdown-eligible via `supportsFunction()`. |

### 3.3 Operator pushdown matrix

All pushdown is driven by Calcite's built-in `JdbcRules`, gated by `ClickHouseSqlDialect`. A `LogicalProject` or `LogicalFilter` is pushdown-eligible iff every `RexCall` inside it is supported by the dialect. If even one call is unsupported, the whole rel stays in the Enumerable layer and runs on OpenSearch.

| PPL operator | Calcite Rel | Pushdown behavior |
|---|---|---|
| `where` | `LogicalFilter` | ✅ via `JdbcFilterRule`, subject to dialect whitelist |
| `fields` / `eval` / `rename` | `LogicalProject` | ✅ via `JdbcProjectRule`, subject to dialect whitelist |
| `stats` / group-by | `LogicalAggregate` | ✅ via `JdbcAggregateRule`, standard aggregates only (`COUNT/SUM/AVG/MIN/MAX`) |
| `sort` | `LogicalSort` | ✅ via `JdbcSortRule` |
| `head` / `limit` | `LogicalSort` with fetch | ✅ via `JdbcSortRule` |
| `dedup` | `LogicalAggregate`/`LogicalProject` | ✅ |
| `join` | `LogicalJoin` | ❌ out of scope (future) |
| `patterns`, `ml`, complex custom RelNodes | — | ❌ no JdbcRule exists; stays in Enumerable |

### 3.4 Fallback mechanism

No custom converters or special handling. VolcanoPlanner cost-selects between `JdbcRules` and Enumerable rules; `JdbcToEnumerableConverter` (Calcite-provided) inserts itself when needed. A query mixing pushdown-eligible ops with a `patterns` call ends up with the prefix pushed to CH and the remainder executed locally.

### 3.5 Day-1 SqlDialect function whitelist

- Arithmetic: `+ - * / %`
- Comparison: `= <> < <= > >= IS NULL IS NOT NULL`
- Logical: `AND OR NOT`
- Casts: `CAST` with types from §5.1
- Aggregates: `COUNT`, `COUNT DISTINCT`, `SUM`, `AVG`, `MIN`, `MAX`
- String: `SUBSTRING`, `LOWER`, `UPPER`, `LENGTH`, `TRIM`, `CONCAT`, `LIKE`
- Conditional: `COALESCE`, `CASE WHEN ... THEN ... ELSE ... END`
- Date/time: `DATE_TRUNC`, literal `TIMESTAMP '...'` (→ `toDateTime64(..., 3)`)

Extensions added per demand; pushdown ratio metric (§6.2) drives prioritization.

---

## 4. Authentication, Connection Pool, Rate Limiting

### 4.1 Authentication (`ClickHouseAuthProvider`)

Composable (not mutually exclusive):

| Type | Config keys | Implementation |
|---|---|---|
| basic | `auth.username`, `auth.password` | JDBC `Properties`: `user`, `password` |
| JWT | `auth.type=jwt`, `auth.token` | JDBC `Properties`: `access_token` (requires CH server that supports token auth; probed at registration) |
| TLS | `tls.enabled=true`, `tls.trust_store_path`, `tls.trust_store_password` | JDBC URL `?ssl=true&sslmode=strict`; a per-datasource `SSLContext` is built and injected via `Properties`. Does NOT touch `javax.net.ssl.*` system properties. |
| multi-host | `uri` contains `host1,host2,...` | Native CH JDBC failover; driver parameter `failover=3` controls retry count |

Registration-time probe (`SELECT 1`) is authoritative; failure classifies into `ClickHouseConnectionException` / `ClickHouseAuthException` (§6).

### 4.2 Connection pool (`ClickHouseClient`)

One HikariCP `DataSource` per `ClickHouseStorageEngine` instance.

```
maximumPoolSize         = pool.max_size       (default 10)
minimumIdle             = pool.min_idle       (default 2)
connectionTimeout       = 5_000 ms
idleTimeout             = 10 * 60_000 ms
maxLifetime             = 30 * 60_000 ms
leakDetectionThreshold  = 60_000 ms
validationQuery         = "SELECT 1"
```

Lifecycle tied to the storage engine: deleting the datasource calls `close()` and releases connections. Calcite receives the HikariCP `DataSource` via `JdbcSchema.dataSource(hikariDS, dialect, null, schemaName)` — HikariCP is transparent to Calcite.

### 4.3 Rate limiting (per-datasource)

Two gates at `ClickHouseClient.executeQuery()`:

1. **Concurrency** — `Semaphore(rate_limit.concurrent)`, default 20. Non-blocking `tryAcquire()`; failure throws `ClickHouseRateLimitException(reason="concurrent")`.
2. **QPS** — Guava `RateLimiter.create(rate_limit.qps)`, default 50. `tryAcquire(timeout=200ms)`; timeout throws `ClickHouseRateLimitException(reason="qps")`.

Both parameters hot-reloadable via datasource update API → `ClickHouseClient.reconfigure()`.

### 4.4 Slow query detection

`ClickHouseClient` timestamps query start; on `ResultSet` close or exception, computes duration. If `duration > slow_query.threshold_ms` (default 5000), the audit log entry (§6.3) is tagged `slow=true`. Detection is out-of-band and does not block the response path.

---

## 5. Type Mapping (Strict Whitelist, No Degradation)

### 5.1 Supported types

| ClickHouse type | `ExprType` | Java (ResultSet) | Notes |
|---|---|---|---|
| `Int8`, `Int16`, `Int32` | `INTEGER` | `int` | |
| `Int64` | `LONG` | `long` | |
| `UInt8` | `BOOLEAN` (if values ∈ {0,1}) or `SHORT` | `short` | User picks one in `expr_type` at registration |
| `UInt16`, `UInt32` | `LONG` | `long` | `UInt32` upcast prevents overflow |
| `Float32` | `FLOAT` | `float` | |
| `Float64` | `DOUBLE` | `double` | |
| `String`, `FixedString(N)` | `STRING` | `String` | |
| `Bool` | `BOOLEAN` | `boolean` | |
| `Date` | `DATE` | `LocalDate` | |
| `Date32` | `DATE` | `LocalDate` | |
| `DateTime` | `TIMESTAMP` | `Instant` (second precision) | |
| `DateTime64(p)` | `TIMESTAMP` | `Instant` (`p ≤ 9`) | precision recorded in declaration |

### 5.2 Explicitly rejected (registration fails)

`UInt64`, `Int128/256`, `UInt128/256`, `Decimal(P,S)` / `Decimal32/64/128/256`, `Enum8`/`Enum16`, `LowCardinality(T)`, `Nullable(T)`, `Array(T)`, `Tuple(...)`, `Map(K,V)`, `UUID`, `IPv4`/`IPv6`, `Nested`, `AggregateFunction`.

Future (v2) candidates for promotion: `Nullable(T)` and `LowCardinality(T)` — unwrap and recurse into the inner type. Not in this iteration.

### 5.3 Validation timing

1. **At registration** — `ClickHouseDataSourceConfig.validate()` checks every declared `(ch_type, expr_type)` pair. Any rejection → whole registration fails.
2. **At runtime** — `ResultSet.getObject(i)` is read according to the declared `expr_type`. If the actual JDBC-returned type doesn't match the declaration (schema drift), throws `ClickHouseSchemaDriftException` (§6.1).

### 5.4 Dialect CAST rewrites

- `CAST(x AS BIGINT)` → `CAST(x AS Int64)`
- `CAST(x AS DOUBLE)` → `CAST(x AS Float64)`
- `CAST(x AS VARCHAR)` → `CAST(x AS String)`
- `CAST(x AS TIMESTAMP)` → `CAST(x AS DateTime64(3))`
- Literal `TIMESTAMP '2024-01-01 00:00:00'` → `toDateTime64('2024-01-01 00:00:00', 3)`

---

## 6. Error Handling, Metrics, Audit

### 6.1 Exception hierarchy

```
ClickHouseException (RuntimeException)
├── ClickHouseConnectionException       // network / TLS handshake failure
├── ClickHouseAuthException              // basic / JWT auth failure
├── ClickHouseTimeoutException           // query timeout / socket_timeout
├── ClickHouseDialectException           // dialect translation failure
├── ClickHouseSchemaException            // invalid declared schema at registration
├── ClickHouseSchemaDriftException       // declared type ≠ actual JDBC type at runtime
├── ClickHouseRateLimitException         // qps / concurrent limit exceeded
└── ClickHouseQueryException             // CH-side SQL error (syntax, permission, missing table)
```

Every exception carries `errorCode` (e.g., `CH_CONN_001`), `userMessage` (sanitized, user-facing), `diagnostics` (internal, logged).

### 6.2 REST-layer mapping

| Exception | HTTP | OpenSearch wrapper |
|---|---|---|
| `ClickHouseConnectionException` | 503 | domain-specific |
| `ClickHouseAuthException` | 503 | domain-specific |
| `ClickHouseTimeoutException` | 503 | domain-specific |
| `ClickHouseDialectException` | 400 | `SemanticCheckException` |
| `ClickHouseQueryException` | 400 | `SemanticCheckException` |
| `ClickHouseSchemaException` | 400 | `SemanticCheckException` |
| `ClickHouseSchemaDriftException` | 400 | `SemanticCheckException` |
| `ClickHouseRateLimitException` | 429 | domain-specific |

### 6.3 Metrics (`ClickHouseMetrics`)

Exposed via `org.opensearch.sql.common.setting.Settings` + OpenSearch `MetricService`. Per-datasource label.

| Metric | Type | Description |
|---|---|---|
| `ch_pool_active` | gauge | HikariCP active connection count |
| `ch_pool_idle` | gauge | HikariCP idle connection count |
| `ch_pool_wait_ms` | histogram | Connection acquisition wait time |
| `ch_query_total` | counter | Labels: `status ∈ {success, error_<class>}` |
| `ch_query_latency_ms` | histogram | End-to-end (queue + execute + deserialize) |
| `ch_pushdown_ratio` | counter | Labels: `layer ∈ {jdbc, enumerable}`, counted per node |
| `ch_ratelimit_rejected` | counter | Labels: `reason ∈ {qps, concurrent}` |
| `ch_slow_query_total` | counter | Queries exceeding threshold |
| `ch_audit_dropped` | counter | Audit records dropped due to backpressure |

Pushdown ratio computed by walking the final planned tree and counting nodes whose convention is `ClickHouseConvention` vs. Enumerable.

### 6.4 Audit log (`ClickHouseAuditLogger`)

Async bounded queue (default capacity 10000) → bulk-written to OS system index `.plugins-sql-clickhouse-audit-YYYY.MM.dd` (daily rollover).

Record format:

```json
{
  "timestamp": "2026-04-19T10:00:00Z",
  "datasource": "my_ch",
  "user": "songkant",
  "query_id": "uuid",
  "ppl_query": "source=my_ch.analytics.events | ...",
  "ch_sql": "SELECT ... FROM analytics.events WHERE ...",
  "duration_ms": 1234,
  "pushdown": {"jdbc": 4, "enumerable": 1},
  "status": "success",
  "error_code": null,
  "row_count": 1000,
  "slow": false
}
```

PII: PPL and generated CH SQL are written as-is, consistent with existing SQL plugin audit behavior. Future integration with `PPLQueryDataAnonymizer` is possible but not in scope.

Backpressure: queue full → drop oldest, increment `ch_audit_dropped`. Never blocks the query path.

---

## 7. Testing Strategy

### 7.1 Unit tests (`clickhouse/src/test`)

No ClickHouse container. Mockito / H2-as-fake-JDBC / pure logic.

| Test class | Coverage |
|---|---|
| `ClickHouseDataSourceConfigTest` | Config parsing, field validation, auth.type combinations, schema JSON parsing, whitelist rejection |
| `ClickHouseTypeMapperTest` | Every whitelisted type maps correctly; every rejected type throws `ClickHouseSchemaException` |
| `ClickHouseSqlDialectTest` | CAST / LIMIT / identifier quoting / scalar function translation; literal timestamp rewrite |
| `ClickHouseSchemaFactoryTest` | Declared schema → Calcite `SchemaPlus`; table lookup |
| `ClickHouseTableTest` | `toRel()` produces `JdbcTableScan` whose convention is `ClickHouseConvention` |
| `ClickHouseClientTest` | HikariCP configuration; semaphore + rate-limiter behavior; over-limit throws |
| `ClickHouseAuthProviderTest` | basic / JWT / TLS correctly injected into `Properties` (no actual CH) |
| `ClickHouseAuditLoggerTest` | Async queue, drop-oldest on full, metric increment |
| `ClickHouseMetricsTest` | Metric increment/decrement semantics |
| `ClickHousePushdownPlanTest` | Given PPL → RelNode → `VolcanoPlanner` optimized plan, assert which nodes are in JDBC convention (uses in-memory Calcite test planner — no real CH) |

### 7.2 Integration tests (`integ-test/src/test/.../clickhouse/`)

`@Testcontainers` + `ClickHouseContainer("clickhouse/clickhouse-server:24.3")`, one container per fixture class.

| IT class | Scenarios |
|---|---|
| `ClickHouseRegistrationIT` | Success; fail-fast on bad URI / bad creds / rejected types; TLS (container SSL enabled); JWT (CH 24.x) |
| `ClickHouseBasicQueryIT` | `source=ch.db.t \| head 10` returns correct rows; one column per whitelisted type |
| `ClickHousePushdownIT` | One sub-test per pushdown operator in §3.3: seed data → run PPL → inspect `EXPLAIN` for JDBC node count + result correctness |
| `ClickHouseFallbackIT` | PPL containing CH-unsupported function (e.g., `patterns`) → plan has Enumerable node + correct result |
| `ClickHouseAuthIT` | basic ok/bad, JWT ok/bad, TLS ok / cert mismatch |
| `ClickHouseMultiHostIT` | docker-compose two CH instances; kill host1, query still routes to host2 |
| `ClickHouseRateLimitIT` | Concurrency / QPS trigger 429; under-limit queries unaffected |
| `ClickHouseSchemaDriftIT` | Declared `String`, CH column altered to `Int` → query throws `SchemaDriftException` |
| `ClickHouseErrorMappingIT` | Each exception class → correct HTTP status + error code |
| `ClickHouseAuditIT` | After a query, the audit index contains the matching record; slow query flagged `slow=true` |
| `ClickHouseMetricsIT` | `_plugins/_nodes/stats` exposes `ch_*` metrics after a query |

Calcite suite: `ClickHouseBasicQueryIT`, `ClickHousePushdownIT`, `ClickHouseFallbackIT` added to `CalciteNoPushdownIT.@Suite.SuiteClasses` so they run under both pushdown-on and pushdown-off.

### 7.3 CI constraints

- Unit tests: `./gradlew :clickhouse:test` under 2 minutes.
- Integration tests: `./gradlew :integ-test:integTest --tests "*clickhouse*"` — separate job; failure does not block unrelated IT.
- Coverage threshold: `clickhouse` module line coverage ≥ 80%, enforced in `jacoco.gradle`.

### 7.4 Performance benchmarks (`benchmarks/`, non-blocking)

Dataset: 1M-row `events` table.
Queries: `count()`; `group by + avg`; `where + head 1000`.
Compare: Calcite pushdown vs. all-local vs. direct CH baseline.
Reference target (not a CI gate): pushdown path ≥ 5× faster than all-local.

---

## 8. Roadmap & Risks

### 8.1 Milestones

| Milestone | Deliverable | Gate |
|---|---|---|
| **M1 — Scaffolding** | `clickhouse/` module, `build.gradle`, `settings.gradle` entry, empty `ClickHouseStorageEngine`, `DataSourceType.CLICKHOUSE`; `./gradlew :clickhouse:build` passes | CI green |
| **M2 — Registration & Connection** | `ClickHouseDataSourceConfig` + `ClickHouseClient` (HikariCP) + `ClickHouseAuthProvider` (basic/TLS/JWT/multi-host); `POST _datasources` succeeds + `SELECT 1` probe | `ClickHouseRegistrationIT` green |
| **M3 — Schema & Types** | `ClickHouseSchemaFactory` + `ClickHouseTypeMapper` + whitelist validation | `ClickHouseTypeMapperTest` + `ClickHouseSchemaDriftIT` green |
| **M4 — Core Pushdown** | `ClickHouseConvention` + `ClickHouseSqlDialect` day-1 set; `ClickHouseTable.toRel() → JdbcTableScan`; end-to-end query returns rows | `ClickHouseBasicQueryIT` + `ClickHousePushdownIT` (filter/project/sort/limit) green |
| **M5 — Aggregate & Fallback** | `JdbcAggregateRule` wired; Enumerable fallback verified; dialect function whitelist extended | `ClickHousePushdownIT` (aggregate) + `ClickHouseFallbackIT` green |
| **M6 — Rate limit & Error classification** | Semaphore + RateLimiter; 8 exception classes + REST mapping; slow-query detection | `ClickHouseRateLimitIT` + `ClickHouseErrorMappingIT` green |
| **M7 — Metrics & Audit** | `ClickHouseMetrics` + `ClickHouseAuditLogger` (async queue + daily rollover) | `ClickHouseAuditIT` + `ClickHouseMetricsIT` green |
| **M8 — Docs & Benchmarks** | User doc `docs/user/ppl/admin/connectors/clickhouse_connector.rst`, dev doc, benchmark scripts | §7.4 reference target met |

Dependencies: M1 → M2 → M3 → M4 → {M5, M6} → M7 → M8. M5 and M6 may run in parallel.

### 8.2 Out of scope

- Write path (bulk insert into ClickHouse)
- Cross-engine federated queries (OS × CH join)
- Data sync / CDC
- CH-specific PPL functions (`arrayJoin`, `quantile`, window functions)
- AI-driven query routing
- Vector search (usearch / annoy indexes)
- CH cluster topology awareness (shard / replica)

### 8.3 Risks & mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| `JdbcSchema` and `OpenSearchSchema` coexistence causes Calcite root-schema conflicts | Medium | High | First POC in M4 registers CH alongside an OS index and queries both; `ClickHouseConvention` is per-datasource, not shared |
| CH dialect gaps hurt pushdown ratio | Medium | Medium | Extend dialect over M4/M5; `ch_pushdown_ratio` metric drives prioritization; doc lists supported functions |
| Planner-rule interference across CH + OS + Enumerable conventions | Medium | High | Rules registered on per-query `VolcanoPlanner`, never on global `RelOptRules`; `ClickHousePushdownPlanTest` covers mixed-convention plans |
| HikariCP pool exhaustion | Low | Medium | Default 10, configurable; metric alerting; `leakDetectionThreshold` enabled |
| CH JDBC driver vs. server version mismatch | Low | Low | Pin driver minor (0.6.x); CI runs IT against CH 24.3 and 25.x |
| Audit index growth | Medium | Low | Daily rollover + documented ISM template (7-day warm→cold, 30-day delete); not enforced |
| Declared schema diverges from actual CH | Medium | Medium | `ClickHouseSchemaDriftException` with clear error; doc shows how to generate declaration JSON via `DESCRIBE TABLE` |
| JWT unsupported on older CH versions | Low | Low | `ClickHouseAuthProvider` probe fails fast with clear message; doc states required CH version |

### 8.4 Acceptance criteria

All must hold:

1. All M1–M8 ITs green.
2. Module line coverage ≥ 80%.
3. `ClickHouseBasicQueryIT` passes under both pushdown on and pushdown off.
4. `ch_pushdown_ratio` shows JDBC layer ≥ 80% in §7.4 benchmarks.
5. User doc + developer doc merged.
6. `./gradlew build` fully green with no skipped checks.
