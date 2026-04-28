-- ClickHouse schema for path-b / path-c demo.
--
-- ORDER BY choice: (parent_asin, timestamp)
--
-- parent_asin is first so `WHERE parent_asin IN (...)` filters (the core of
-- the Path C IN-list pushdown benefit) can use the MergeTree primary key
-- index to skip partitions and granules. timestamp is second to help
-- secondary time-range filters in the benchmark query.
--
-- LowCardinality(Bool) for verified_purchase: the column has two values,
-- so dictionary encoding saves disk and speeds up filters.

CREATE DATABASE IF NOT EXISTS fed;

DROP TABLE IF EXISTS fed.reviews;

CREATE TABLE fed.reviews (
    parent_asin       String,
    asin              String,
    user_id           String,
    rating            Float32,
    -- Int32 not UInt32: the Amazon Reviews 2023 corpus contains a handful
    -- of negative helpful_vote values (data-quality artifact of the
    -- original source), which CH rejects with CANNOT_PARSE_NUMBER against
    -- an unsigned column.
    helpful_vote      Int32,
    timestamp         Int64,
    verified_purchase UInt8,
    title             String CODEC(ZSTD(3)),
    text              String CODEC(ZSTD(3))
) ENGINE = MergeTree()
ORDER BY (parent_asin, timestamp)
SETTINGS index_granularity = 8192;
