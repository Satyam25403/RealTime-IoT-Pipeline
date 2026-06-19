# Databricks notebook source
# =============================================================================
# 01_bronze_ingest — Layer 3, bronze landing (see README.md section 3,
# "Layer 3 — Batch Transformation").
#
# Reads raw events written by stream_processing/asa_queries/raw_passthrough.asaql
# (which lands in ADLS Gen2 unwindowed, one file per micro-batch) and writes
# them as append-only Parquet, partitioned by ingestion_date. No
# transformation happens here on purpose -- bronze's only job is to be a
# faithful, replayable landing zone (see README's "Schema evolution"
# decision) -- cleaning, dedup, and type enforcement are silver's job
# (02_silver_clean_dedup.py), not this notebook's.
#
# Schema: BRONZE_SCHEMA in utils/schema_definitions.py is the source of
# truth and is applied explicitly on read below, rather than inferred, so a
# malformed upstream file fails loudly here (where it's one day's data)
# instead of silently downstream (where it could corrupt silver/gold).
# =============================================================================

# COMMAND ----------

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.schema_definitions import BRONZE_SCHEMA

# COMMAND ----------

dbutils.widgets.text("raw_cold_path_input", "/mnt/adls/raw-cold-path/", "ADLS path written by raw_passthrough.asaql")
dbutils.widgets.text("bronze_output_path", "/mnt/adls/bronze/weather_events/", "Bronze Delta/Parquet output path")
dbutils.widgets.text("run_date", "", "Ingestion date to process (YYYY-MM-DD); empty = today, UTC")

raw_input_path = dbutils.widgets.get("raw_cold_path_input")
bronze_output_path = dbutils.widgets.get("bronze_output_path")
run_date_param = dbutils.widgets.get("run_date")

# COMMAND ----------

from datetime import datetime, timezone

run_date = run_date_param or datetime.now(timezone.utc).strftime("%Y-%m-%d")
print(f"bronze ingest for ingestion_date={run_date}")

# COMMAND ----------

# Explicit schema on read -- see module docstring for why this isn't
# schema=True/inferSchema. raw_passthrough.asaql's SELECT was field-diffed
# against BRONZE_SCHEMA during Layer 2 implementation and matches it
# exactly (30/30 fields, verified) -- this read should never need a
# mergeSchema-style escape hatch the way silver's write does, since bronze
# is reading what Layer 2 already wrote in this exact shape.
raw_df = (
    spark.read
    .schema(BRONZE_SCHEMA)
    .json(f"{raw_input_path}ingestion_date={run_date}/")
)

input_count = raw_df.count()
print(f"read {input_count} raw events for {run_date}")

if input_count == 0:
    # Not an error -- a quiet poll day (e.g. all cities transiently failed,
    # see Layer 1's per-city isolation) shouldn't fail the pipeline. Log
    # loudly so it's visible in ADF pipeline run history, then stop cleanly
    # rather than writing an empty partition.
    print(f"WARNING: zero events for ingestion_date={run_date} -- nothing to write. "
          f"If this persists across multiple days, check Layer 1 Function logs "
          f"and the Event Hub consumer lag KQL query (docs/kql/eventhub_consumer_lag.kql).")
    dbutils.notebook.exit("zero_events")

# COMMAND ----------

# Append-only, partitioned by ingestion_date -- matches BRONZE_SCHEMA's
# ingestion_date field, which is also what 02_silver_clean_dedup.py reads
# by to scope each day's silver run to the matching bronze partition.
(
    raw_df.write
    .mode("append")
    .partitionBy("ingestion_date")
    .format("parquet")
    .save(bronze_output_path)
)

print(f"wrote {input_count} rows to bronze at {bronze_output_path}, partition ingestion_date={run_date}")
dbutils.notebook.exit(f"wrote_{input_count}_rows")
