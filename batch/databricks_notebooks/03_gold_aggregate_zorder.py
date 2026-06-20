# Databricks notebook source
# =============================================================================
# 03_gold_aggregate_zorder — Layer 3, gold aggregation + Z-ordering (see
# README.md section 3, "Layer 3 — Batch Transformation" and "Z-ordering").
#
# Reads today's silver partition, aggregates to one row per (city_id, date)
# matching GOLD_SCHEMA, computes a 7-day rolling average alongside today's
# daily stats, writes/merges into the gold Delta table, then runs
# OPTIMIZE ... ZORDER BY (city_id) and registers the table in Unity Catalog.
#
# Note GOLD_SCHEMA is a DIFFERENT GRAIN than silver -- this notebook does a
# groupBy, not a row-level passthrough, which is why it needs a MERGE
# (upsert) rather than a plain append: re-running this notebook for the
# same date must replace that date's aggregate, not duplicate it.
# =============================================================================

# COMMAND ----------

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.schema_definitions import GOLD_SCHEMA

# COMMAND ----------

dbutils.widgets.text("silver_input_path", "/mnt/adls/silver/weather_events/", "Silver Delta path")
dbutils.widgets.text("gold_table_name", "main.weather_lakehouse.gold_city_daily_stats", "Unity Catalog 3-part table name")
dbutils.widgets.text("run_date", "", "ingestion_date to aggregate (YYYY-MM-DD); empty = today, UTC")

silver_input_path = dbutils.widgets.get("silver_input_path")
gold_table_name = dbutils.widgets.get("gold_table_name")
run_date_param = dbutils.widgets.get("run_date")

# COMMAND ----------

from datetime import datetime, timezone, timedelta

run_date = run_date_param or datetime.now(timezone.utc).strftime("%Y-%m-%d")
rolling_window_start = (datetime.strptime(run_date, "%Y-%m-%d") - timedelta(days=6)).strftime("%Y-%m-%d")
print(f"gold aggregate for date={run_date}, 7-day rolling window starts {rolling_window_start}")

# COMMAND ----------

from pyspark.sql.functions import avg, min as spark_min, max as spark_max, count, lit, to_date, col

# Today's daily stats: one groupBy over JUST today's silver partition.
todays_silver = spark.read.format("delta").load(silver_input_path).filter(
    f"ingestion_date = '{run_date}'"
)

todays_count = todays_silver.count()
if todays_count == 0:
    print(f"WARNING: zero silver rows for {run_date} -- nothing to aggregate. "
          f"Check 02_silver_clean_dedup.py's run for this same date succeeded "
          f"and didn't quarantine 100% of rows.")
    dbutils.notebook.exit("zero_rows")

daily_stats = (
    todays_silver.groupBy("city_id")
    .agg(
        avg("temp").alias("avg_temp"),
        spark_min("temp").alias("min_temp"),
        spark_max("temp").alias("max_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("aqi").alias("avg_aqi"),
        spark_max("aqi").alias("max_aqi"),
        avg("pm2_5").alias("avg_pm2_5"),
        avg("pm10").alias("avg_pm10"),
        count("*").alias("observation_count"),
    )
    .withColumn("date", to_date(lit(run_date)))
)

# COMMAND ----------

# 7-day rolling average: a SEPARATE read over the trailing 7-day window of
# silver (today inclusive), aggregated to one avg_temp per city, then
# joined onto daily_stats. This is intentionally a second pass rather than
# trying to compute both in one groupBy -- the rolling figure needs a wider
# input window than the daily figures do, and conflating them risks
# silently computing the "daily" stats over 7 days of data instead of 1.
rolling_window_silver = spark.read.format("delta").load(silver_input_path).filter(
    f"ingestion_date >= '{rolling_window_start}' AND ingestion_date <= '{run_date}'"
)

rolling_avg = (
    rolling_window_silver.groupBy("city_id")
    .agg(avg("temp").alias("rolling_7day_avg_temp"))
)

# COMMAND ----------

gold_rows = daily_stats.join(rolling_avg, on="city_id", how="left")

# Select + cast to GOLD_SCHEMA's exact column order/types -- the source of
# truth for what gold actually looks like, also used by
# cold_path/synapse_sql/create_external_tables_openrowset.sql's OPENROWSET
# definition, so drift here would break Layer 5 silently.
gold_columns = [f.name for f in GOLD_SCHEMA.fields]
gold_rows = gold_rows.select(*gold_columns)

# Computed once here and reused below -- the original version called
# gold_rows.count() a second time in the final exit message, AFTER the
# OPTIMIZE step, triggering an unnecessary second full scan (caught in code
# review). Not a correctness bug -- the row count doesn't change between
# here and the exit message -- just a wasted scan on a table that's about
# to be Z-ordered anyway.
gold_row_count = gold_rows.count()
print(f"computed gold aggregates for {gold_row_count} cities on {run_date}")

# COMMAND ----------

# MERGE (upsert) on (city_id, date), not append -- re-running this notebook
# for a date that was already processed (e.g. after fixing a silver
# quarantine issue and backfilling) must REPLACE that date's row per city,
# not create a duplicate. This is the main reason gold needs Delta's MERGE
# rather than the append-only pattern bronze/silver use -- gold's grain
# (one row per city per day) makes "re-running = duplicate" a real risk
# that bronze/silver's append-only, immutable-event grain doesn't have.
from delta.tables import DeltaTable

if spark.catalog.tableExists(gold_table_name):
    gold_table = DeltaTable.forName(spark, gold_table_name)
    (
        gold_table.alias("target")
        .merge(gold_rows.alias("source"), "target.city_id = source.city_id AND target.date = source.date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"merged into existing table {gold_table_name}")
else:
    (
        gold_rows.write
        .format("delta")
        .saveAsTable(gold_table_name)
    )
    print(f"created new Unity Catalog table {gold_table_name}")

# COMMAND ----------

# Z-ORDER on city_id -- see README.md's "Z-ordering" decision: city_id is
# the dominant filter/join column for both Synapse OPENROWSET (Layer 5) and
# Power BI DirectQuery, so co-locating rows by city_id lets data-skipping
# actually work at query time.
#
# NOTE: Databricks now recommends liquid clustering over Z-ordering for new
# tables (see README Layer 3 implementation notes) -- Z-ordering is used
# here because the assignment explicitly asks "Think about: Z-ordering,"
# and this directly answers that, but liquid clustering is worth
# considering as a documented alternative if this table outlives the
# assignment's scope.
spark.sql(f"OPTIMIZE {gold_table_name} ZORDER BY (city_id)")
print(f"Z-ordered {gold_table_name} on city_id")

dbutils.notebook.exit(f"merged_{gold_row_count}_rows_for_{run_date}")
