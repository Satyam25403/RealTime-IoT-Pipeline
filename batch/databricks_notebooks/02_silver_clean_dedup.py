# Databricks notebook source
# =============================================================================
# 02_silver_clean_dedup — Layer 3, silver cleaning + dedup (see README.md
# section 3, "Layer 3 — Batch Transformation" and "Schema evolution").
#
# Reads today's bronze partition, deduplicates on (city_id,
# observation_timestamp), converts bronze's raw Unix `dt` into a proper
# TimestampType, casts/cleans types, and writes Delta with mergeSchema
# enabled -- but ONLY after validate_silver_required_fields() has split off
# any row missing a critical field into quarantine. This ordering is the
# actual enforcement of the README's schema-evolution decision: mergeSchema
# alone would happily accept a row with nulls in city_id or temp; the
# validation gate is what stops that from silently entering silver.
# =============================================================================

# COMMAND ----------

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.schema_definitions import SILVER_SCHEMA, validate_silver_required_fields, SchemaValidationError

# COMMAND ----------

dbutils.widgets.text("bronze_input_path", "/mnt/adls/bronze/weather_events/", "Bronze Parquet path")
dbutils.widgets.text("silver_output_path", "/mnt/adls/silver/weather_events/", "Silver Delta output path")
dbutils.widgets.text("quarantine_path", "/mnt/adls/silver/_quarantine/weather_events/", "Quarantine Delta output path")
dbutils.widgets.text("run_date", "", "ingestion_date partition to process (YYYY-MM-DD); empty = today, UTC")

bronze_input_path = dbutils.widgets.get("bronze_input_path")
silver_output_path = dbutils.widgets.get("silver_output_path")
quarantine_path = dbutils.widgets.get("quarantine_path")
run_date_param = dbutils.widgets.get("run_date")

# COMMAND ----------

from datetime import datetime, timezone

run_date = run_date_param or datetime.now(timezone.utc).strftime("%Y-%m-%d")
print(f"silver clean+dedup for ingestion_date={run_date}")

# COMMAND ----------

bronze_df = spark.read.parquet(bronze_input_path).filter(f"ingestion_date = '{run_date}'")
bronze_count = bronze_df.count()
print(f"read {bronze_count} bronze rows for {run_date}")

if bronze_count == 0:
    print(f"WARNING: zero bronze rows for {run_date} -- nothing to process. "
          f"Check 01_bronze_ingest.py's run for this same date succeeded.")
    dbutils.notebook.exit("zero_rows")

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, col, to_date

# CRITICAL FIX (caught in code review): this conversion step must run
# BEFORE validate_silver_required_fields(), not after. SILVER_REQUIRED_
# FIELDS includes "observation_timestamp", which doesn't exist anywhere in
# BRONZE_SCHEMA -- it's a SILVER-side column produced by converting
# bronze's raw Unix `dt` into a proper timestamp. Validating bronze_df
# directly (the original ordering) meant "observation_timestamp" was
# ALWAYS structurally absent at validation time, so
# validate_silver_required_fields() raised SchemaValidationError on every
# single run with no exception -- a 100%-of-the-time failure, not a
# data-quality edge case. Converting first means the column genuinely
# exists by the time the gate checks it, and a row where `dt` was null/
# unparseable now correctly shows up as a NULL observation_timestamp --
# which the gate is designed to catch and quarantine, rather than crashing
# the whole batch on a structural false alarm.
converted = (
    bronze_df
    .withColumn("observation_timestamp", from_unixtime(col("dt")).cast("timestamp"))
    .withColumn("ingestion_date", to_date(col("ingestion_date")))
)

# COMMAND ----------

# --- schema validation gate (the actual enforcement point) ---
# Runs against `converted`, not `bronze_df` -- see the fix note above for
# why this ordering matters.
try:
    good_rows, bad_rows = validate_silver_required_fields(converted)
except SchemaValidationError as exc:
    # Structural failure -- bronze's actual columns no longer match what
    # SILVER_REQUIRED_FIELDS expects. This is a code/schema drift problem,
    # not a data-quality problem, so the whole run fails loudly rather than
    # silently writing a partial/wrong silver table. See the exception's
    # docstring in schema_definitions.py for the distinction from the
    # per-row quarantine path below.
    print(f"FATAL: {exc}")
    raise

bad_count = bad_rows.count()
if bad_count > 0:
    print(f"quarantining {bad_count} row(s) missing a required field "
          f"({bad_count}/{bronze_count} = {bad_count/bronze_count:.1%} of today's batch)")
    (
        bad_rows.write
        .mode("append")
        .partitionBy("ingestion_date")
        .format("delta")
        .option("mergeSchema", "true")
        .save(quarantine_path)
    )
else:
    print("no rows quarantined -- every row had all required fields populated")

# COMMAND ----------

# Dedup on (city_id, observation_timestamp) -- if the same city polled twice
# with the same observation timestamp (e.g. a retried Function execution
# that both attempts ended up publishing), keep one. dropDuplicates keeps
# an arbitrary-but-deterministic-per-run row; this is acceptable here since
# duplicate rows for the same (city, timestamp) should be identical in
# practice -- they're re-publishes of the same OWM response, not
# conflicting data that needs a tie-breaker rule.
#
# Operates on good_rows (post-validation), not the full converted set --
# quarantined rows must never be deduped into the silver-bound output.
deduped = good_rows.dropDuplicates(["city_id", "observation_timestamp"])

deduped_count = deduped.count()
good_count = good_rows.count()
print(f"{good_count} good rows -> {deduped_count} after dedup "
      f"({good_count - deduped_count} duplicate(s) removed)")

# COMMAND ----------

# Select + cast to SILVER_SCHEMA's exact column list -- drops bronze-only
# columns (dt, raw_payload_json, source, schema_version, lat, lon,
# temp_min, temp_max, visibility) that silver intentionally doesn't carry
# forward. SILVER_SCHEMA in schema_definitions.py is the source of truth
# for exactly which columns those are.
silver_columns = [f.name for f in SILVER_SCHEMA.fields]
final_df = deduped.select(*silver_columns)

# COMMAND ----------

# mergeSchema enabled per the README's schema-evolution decision -- OWM
# could add new fields over time, and we want those to land here without a
# code change, but ONLY for rows that already passed the required-fields
# gate above. mergeSchema does NOT bypass that gate; it only affects how
# NEW columns (not in SILVER_SCHEMA today) are handled on write.
(
    final_df.write
    .mode("append")
    .partitionBy("ingestion_date")
    .format("delta")
    .option("mergeSchema", "true")
    .save(silver_output_path)
)

print(f"wrote {deduped_count} rows to silver at {silver_output_path}")
dbutils.notebook.exit(f"wrote_{deduped_count}_rows_quarantined_{bad_count}")
