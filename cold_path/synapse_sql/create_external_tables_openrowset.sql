-- =============================================================================
-- LAYER 5 — COLD PATH SERVING: Synapse serverless SQL over gold Delta tables
-- =============================================================================
-- See README.md section 3 ("OPENROWSET vs PolyBase" decision) for why
-- serverless SQL + OPENROWSET was chosen over PolyBase/dedicated pools.
--
-- TWO DIFFERENT PATTERNS USED HERE, DELIBERATELY, NOT INTERCHANGEABLY:
--
--   gold_city_daily_stats -> CREATE EXTERNAL TABLE (this file's main table)
--     Safe here because 03_gold_aggregate_zorder.py writes gold via
--     `saveAsTable()` with NO `partitionBy` -- it's an unpartitioned Delta
--     table (Z-ordering is a file-layout optimization via OPTIMIZE, NOT a
--     Hive-style partition scheme -- these are genuinely different
--     mechanisms, easy to conflate). Per Microsoft's own docs: "external
--     tables can't be created on a partitioned folder... don't create
--     external tables on partitioned Delta Lake folders even if you see
--     that they might work in some cases -- using unsupported features
--     like this might cause issues or instability of the serverless pool,
--     and Azure support won't be able to resolve any issue if it's using
--     tables on partitioned folders." Since gold has no partition columns
--     in its folder structure, this restriction doesn't apply -- but if
--     gold's write pattern ever changes to partition by date or city_id,
--     this CREATE EXTERNAL TABLE must be converted to a partitioned VIEW
--     (see the alert_history_view example below for that pattern) BEFORE
--     deploying, not after something breaks.
--
--   anomaly_alerts history (if ever queried from here instead of Cosmos) ->
--     VIEW over OPENROWSET, demonstrating the partitioned-views pattern
--     for any future Delta source that IS partitioned (e.g. if bronze/
--     silver are ever exposed here too -- both ARE partitioned by
--     ingestion_date via 01_bronze_ingest.py / 02_silver_clean_dedup.py,
--     so THOSE would need the view pattern, not the table pattern, if
--     ever added to this file).
--
-- PREREQUISITE NOT YET IN THIS FILE: a UTF-8 collation must be set on the
-- target database, since Delta Lake string values are UTF-8 encoded and a
-- mismatch causes conversion errors on city_id/weather_description/etc.
-- This is a CREATE DATABASE-time setting (ALTER DATABASE CURRENT COLLATE
-- Latin1_General_100_BIN2_UTF8), which belongs in whatever script creates
-- the Synapse serverless database itself -- not repeated here since this
-- file assumes that database already exists. Flagged as an open item for
-- whoever writes that setup script.
-- =============================================================================

-- --- external data source: points at the gold container root ---
-- LOCATION here is the CONTAINER root, not the gold folder itself -- the
-- relative path to gold is supplied per-OPENROWSET-call below via BULK,
-- which is the documented pattern for reusing one data source across
-- multiple Delta folders (gold today; bronze/silver could be added later
-- using the same source with a different BULK path).
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'AdlsLakehouseSource')
    CREATE EXTERNAL DATA SOURCE AdlsLakehouseSource
    WITH (
        LOCATION = 'https://<storage_account_name>.dfs.core.windows.net/<container_name>'
        -- Credential intentionally omitted here: per README Layer 6
        -- ("managed identities everywhere"), this should authenticate via
        -- the Synapse workspace's own managed identity, not a SAS token or
        -- account key. That's configured via CREDENTIAL = <db-scoped
        -- credential referencing a managed identity>, set up once
        -- alongside infra/bicep/synapse.bicep -- left as a placeholder
        -- here rather than guessed at, since the exact credential object
        -- name depends on how that Bicep module ends up structuring it.
    );
GO

-- --- file format: MUST run before any CREATE EXTERNAL TABLE that
-- references it. CORRECTED execution order (caught in code review): this
-- block was originally placed AFTER the CREATE EXTERNAL TABLE statement
-- below, even though this file's own comments already said it needed to
-- run first -- the comment was right and the code contradicted it. Running
-- the file top-to-bottom as originally ordered would fail with "Cannot
-- find the object 'DeltaLakeFormat' because it does not exist or you do
-- not have permissions," since CREATE EXTERNAL TABLE resolves its
-- FILE_FORMAT reference at creation time, not lazily. Correct dependency
-- order, now actually reflected in the code below: data source -> file
-- format -> external table.
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'DeltaLakeFormat')
    CREATE EXTERNAL FILE FORMAT DeltaLakeFormat WITH (FORMAT_TYPE = DELTA);
GO

-- --- gold_city_daily_stats: CREATE EXTERNAL TABLE (see header — safe
-- here because gold is unpartitioned) ---
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'gold_city_daily_stats')
    DROP EXTERNAL TABLE gold_city_daily_stats;
GO

-- Column list and types here MUST match GOLD_SCHEMA in
-- batch/databricks_notebooks/utils/schema_definitions.py exactly -- that
-- file is the source of truth for gold's shape; this is a consumer of it,
-- not an independent definition. If GOLD_SCHEMA changes, this table
-- definition needs to change too -- there is no automated check for that
-- cross-repo-layer consistency the way there is for the Python/PySpark
-- schema cross-checks elsewhere in this repo, since T-SQL DDL isn't
-- something the existing field-diff tooling can parse. Flagged here as a
-- manual-sync point, not a solved problem.
CREATE EXTERNAL TABLE gold_city_daily_stats (
    city_id              VARCHAR(50)     NOT NULL,
    date                 DATE            NOT NULL,
    avg_temp             FLOAT           NULL,
    min_temp             FLOAT           NULL,
    max_temp             FLOAT           NULL,
    avg_humidity         FLOAT           NULL,
    avg_aqi              FLOAT           NULL,
    max_aqi              INT             NULL,
    avg_pm2_5            FLOAT           NULL,
    avg_pm10             FLOAT           NULL,
    observation_count    INT             NULL,
    rolling_7day_avg_temp FLOAT          NULL
)
WITH (
    LOCATION = 'gold/city_daily_stats/',  -- relative to AdlsLakehouseSource's container root;
                                            -- must contain a _delta_log subfolder, or this fails --
                                            -- per Microsoft's docs, a missing _delta_log means you're
                                            -- pointing at plain Parquet, not a real Delta table
    DATA_SOURCE = AdlsLakehouseSource,
    FILE_FORMAT = DeltaLakeFormat
);
GO

-- =============================================================================
-- EXAMPLE: partitioned VIEW pattern (for reference, not currently used) --
-- this is what bronze or silver would need if ever exposed here, since
-- BOTH are partitioned by ingestion_date. Included as a documented
-- template rather than left unwritten, since the assignment's "Think
-- about: OPENROWSET vs PolyBase, when to use serverless vs dedicated
-- pools" implicitly invites showing both patterns are understood, not
-- just the one gold happens to need today.
-- =============================================================================
-- CREATE OR ALTER VIEW silver_weather_events_view AS
-- SELECT *
-- FROM OPENROWSET(
--     BULK 'silver/weather_events/',
--     DATA_SOURCE = 'AdlsLakehouseSource',
--     FORMAT = 'DELTA'
-- ) AS rows;
-- GO
-- -- No FILEPATH() needed -- per Microsoft's docs, OPENROWSET automatically
-- -- identifies Delta Lake partition columns from the folder structure
-- -- itself; partition elimination happens automatically when the
-- -- partition column (ingestion_date) appears in a query's WHERE clause.

-- --- sample query demonstrating partition-aware historical trend access ---
-- (this is the kind of query powerbi/README.md's "Historical trend report"
-- DirectQuery artifact would actually issue)
SELECT
    city_id,
    date,
    avg_temp,
    avg_aqi,
    rolling_7day_avg_temp
FROM gold_city_daily_stats
WHERE date >= DATEADD(day, -30, CAST(GETUTCDATE() AS DATE))
ORDER BY city_id, date;
