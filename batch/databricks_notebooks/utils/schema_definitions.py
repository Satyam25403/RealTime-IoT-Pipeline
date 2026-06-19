"""
Shared schema definitions for bronze / silver / gold (Layer 3 — see README.md
section 3, "Schema evolution"). Imported by all three notebooks so the three
stages can't silently drift apart from each other.

Source of truth for the raw field names: docs/openweathermap_api_reference.md
(free Current Weather + Air Pollution APIs). If that reference doc's field
list ever changes, update BRONZE_SCHEMA here first, then propagate.

Three schemas, three different jobs:

  BRONZE_SCHEMA
    One row per (city, poll). Mirrors the enriched event shape produced by
    ingestion/function_app/shared/enrichment.py as closely as possible — this
    is the "faithful landing zone," so it is deliberately permissive
    (nullable=True almost everywhere) since bronze must accept whatever the
    Function actually sent, including partial/malformed records.

  SILVER_REQUIRED_FIELDS
    Not a full schema — a minimum-required-field check run BEFORE the
    mergeSchema write described in the README. A row missing any of these
    fails validation and is routed to a quarantine path instead of silver,
    rather than silently entering the table with nulls in critical columns.
    This is what actually enforces the "Schema evolution" decision in the
    README — without this list, "enable mergeSchema with a check" is just
    prose with nothing behind it.

  SILVER_SCHEMA
    One row per (city_id, observation_timestamp) after cleaning + dedup.
    Stricter types than bronze (e.g. observation_timestamp as TimestampType,
    not the raw Unix int).

  GOLD_SCHEMA
    One row per (city_id, date) — a DIFFERENT GRAIN than bronze/silver.
    Daily aggregates + rolling averages, the shape Z-ordered on city_id and
    queried by Synapse OPENROWSET / Power BI DirectQuery.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    LongType,
    TimestampType,
    DateType,
)


# ---------------------------------------------------------------------------
# BRONZE — raw landing zone, one row per (city, poll)
# ---------------------------------------------------------------------------
# Field provenance noted inline: [W] = Current Weather API, [A] = Air
# Pollution API, [E] = added by enrichment.py at ingestion time.
# See docs/openweathermap_api_reference.md sections 1, 2, and the
# enrichment.py docstring for exactly which raw fields these come from.

BRONZE_SCHEMA = StructType([
    # --- enrichment metadata [E] ---
    StructField("city_id", StringType(), nullable=True),          # from cities.json, NOT from OWM
    StructField("poll_timestamp", TimestampType(), nullable=True), # when the Function polled, UTC
    StructField("source", StringType(), nullable=True),            # "azure-function-timer"
    StructField("schema_version", StringType(), nullable=True),
    StructField("ingestion_date", DateType(), nullable=True),      # bronze partition column

    # --- Current Weather API fields [W] ---
    StructField("dt", LongType(), nullable=True),                  # Unix UTC, OWM's own observation time
    StructField("lat", DoubleType(), nullable=True),
    StructField("lon", DoubleType(), nullable=True),
    StructField("temp", DoubleType(), nullable=True),               # main.temp
    StructField("feels_like", DoubleType(), nullable=True),         # main.feels_like
    StructField("temp_min", DoubleType(), nullable=True),           # main.temp_min — see API ref note: not a forecast
    StructField("temp_max", DoubleType(), nullable=True),           # main.temp_max
    StructField("pressure", IntegerType(), nullable=True),          # main.pressure, hPa
    StructField("humidity", IntegerType(), nullable=True),          # main.humidity, %
    StructField("visibility", IntegerType(), nullable=True),        # metres, may be absent
    StructField("wind_speed", DoubleType(), nullable=True),         # wind.speed
    StructField("wind_deg", IntegerType(), nullable=True),          # wind.deg
    StructField("clouds_all", IntegerType(), nullable=True),        # clouds.all, %
    StructField("weather_main", StringType(), nullable=True),       # weather[0].main
    StructField("weather_description", StringType(), nullable=True),# weather[0].description

    # --- Air Pollution API fields [A] ---
    StructField("aqi", IntegerType(), nullable=True),               # list[0].main.aqi — OWM's 1-5 scale, NOT EPA 0-500
    StructField("co", DoubleType(), nullable=True),
    StructField("no", DoubleType(), nullable=True),
    StructField("no2", DoubleType(), nullable=True),
    StructField("o3", DoubleType(), nullable=True),
    StructField("so2", DoubleType(), nullable=True),
    StructField("pm2_5", DoubleType(), nullable=True),
    StructField("pm10", DoubleType(), nullable=True),
    StructField("nh3", DoubleType(), nullable=True),

    # --- raw passthrough escape hatch ---
    # Anything OWM adds that we haven't mapped yet lands here as a JSON
    # string rather than being silently dropped — this is what lets bronze
    # stay genuinely "raw and replayable" even as the API evolves.
    StructField("raw_payload_json", StringType(), nullable=True),
])


# ---------------------------------------------------------------------------
# SILVER — minimum required fields (the actual schema-evolution gate)
# ---------------------------------------------------------------------------
# A bronze row missing ANY of these fails validation in
# 02_silver_clean_dedup.py and is written to a quarantine path instead of
# silver. Deliberately a SHORT list: only what downstream aggregation and
# the dedup key actually depend on. Optional/enrichment fields (wind, AQI
# sub-components, etc.) can be null without blocking the write — losing
# wind_speed for one row is a data-quality issue; losing city_id or temp
# breaks the dedup key and every aggregate built on top of it.

SILVER_REQUIRED_FIELDS = [
    "city_id",              # dedup key, gold partition/Z-order key, Cosmos partition key upstream
    "observation_timestamp",# dedup key (post-conversion from bronze's `dt`)
    "temp",                 # rolling average + anomaly detection input
    "aqi",                  # anomaly detection input
]


SILVER_SCHEMA = StructType([
    StructField("city_id", StringType(), nullable=False),
    StructField("observation_timestamp", TimestampType(), nullable=False),  # converted from bronze `dt`
    StructField("ingestion_date", DateType(), nullable=False),              # retained for partitioning
    StructField("temp", DoubleType(), nullable=False),
    StructField("feels_like", DoubleType(), nullable=True),
    StructField("pressure", IntegerType(), nullable=True),
    StructField("humidity", IntegerType(), nullable=True),
    StructField("wind_speed", DoubleType(), nullable=True),
    StructField("wind_deg", IntegerType(), nullable=True),
    StructField("clouds_all", IntegerType(), nullable=True),
    StructField("weather_main", StringType(), nullable=True),
    StructField("weather_description", StringType(), nullable=True),
    StructField("aqi", IntegerType(), nullable=False),
    StructField("co", DoubleType(), nullable=True),
    StructField("no", DoubleType(), nullable=True),
    StructField("no2", DoubleType(), nullable=True),
    StructField("o3", DoubleType(), nullable=True),
    StructField("so2", DoubleType(), nullable=True),
    StructField("pm2_5", DoubleType(), nullable=True),
    StructField("pm10", DoubleType(), nullable=True),
    StructField("nh3", DoubleType(), nullable=True),
])


# ---------------------------------------------------------------------------
# GOLD — daily aggregates, one row per (city_id, date). Z-ordered on city_id.
# ---------------------------------------------------------------------------
# This is NOT the same grain as silver — it's the output of a groupBy, not a
# cleaned passthrough. Registered in Unity Catalog; queried by Synapse
# OPENROWSET and Power BI DirectQuery (see README Layer 5).

GOLD_SCHEMA = StructType([
    StructField("city_id", StringType(), nullable=False),
    StructField("date", DateType(), nullable=False),
    StructField("avg_temp", DoubleType(), nullable=True),
    StructField("min_temp", DoubleType(), nullable=True),
    StructField("max_temp", DoubleType(), nullable=True),
    StructField("avg_humidity", DoubleType(), nullable=True),
    StructField("avg_aqi", DoubleType(), nullable=True),
    StructField("max_aqi", IntegerType(), nullable=True),       # worst air quality moment of the day
    StructField("avg_pm2_5", DoubleType(), nullable=True),
    StructField("avg_pm10", DoubleType(), nullable=True),
    StructField("observation_count", IntegerType(), nullable=True),  # how many polls contributed — data-quality signal
    StructField("rolling_7day_avg_temp", DoubleType(), nullable=True),
])


class SchemaValidationError(Exception):
    """Raised when a bronze batch is missing one of SILVER_REQUIRED_FIELDS
    entirely (the column doesn't exist at all). This is a STRUCTURAL
    failure — almost certainly an upstream change to enrichment.py or the
    OWM API response shape that BRONZE_SCHEMA hasn't been updated to match
    — and the whole batch fails fast rather than silently writing partial
    data. This is different from a row having a NULL value in a column that
    DOES exist, which is handled per-row by routing to quarantine instead
    (see validate_silver_required_fields)."""


def validate_silver_required_fields(df):
    """
    Called from 02_silver_clean_dedup.py before the mergeSchema write.
    Checks every column in SILVER_REQUIRED_FIELDS is present AND non-null
    for every row; rows failing the null check are split off to a
    quarantine path (e.g. silver/_quarantine/) rather than dropped silently
    or allowed to corrupt the dedup key.

    Two distinct failure modes, handled differently on purpose:
      1. A column in SILVER_REQUIRED_FIELDS doesn't exist on df at all ->
         SchemaValidationError, whole batch fails fast. This means bronze's
         actual shape no longer matches BRONZE_SCHEMA, which is a code
         problem (enrichment.py and schema_definitions.py have drifted),
         not a data-quality problem -- continuing would silently write
         columns that don't mean what the rest of the pipeline assumes.
      2. A column exists but is NULL for a given row -> that row is
         quarantined, the rest of the batch proceeds. This is the normal,
         expected failure mode (e.g. one city's poll returned a partial API
         response) and must NOT abort the whole day's batch over one bad row
         -- same per-unit isolation principle as Layer 1's per-city error
         handling, applied here at the row level instead of the city level.

    Args:
        df: a Spark DataFrame matching (a superset of) BRONZE_SCHEMA.

    Returns:
        (good_rows, bad_rows) -- two DataFrames partitioning df. good_rows
        has no nulls in any SILVER_REQUIRED_FIELDS column; bad_rows is
        everything else, intended for 02_silver_clean_dedup.py to write to
        a quarantine path rather than discard.

    Raises:
        SchemaValidationError: if any SILVER_REQUIRED_FIELDS column is
            entirely absent from df.columns.
    """
    missing_cols = [c for c in SILVER_REQUIRED_FIELDS if c not in df.columns]
    if missing_cols:
        raise SchemaValidationError(
            f"bronze batch is missing required column(s) entirely: {missing_cols}. "
            f"This means BRONZE_SCHEMA and the actual bronze data have drifted apart -- "
            f"check enrichment.py still produces these fields, or that "
            f"SILVER_REQUIRED_FIELDS/BRONZE_SCHEMA in this file haven't gone stale."
        )

    from functools import reduce
    from operator import or_

    null_conditions = [df[c].isNull() for c in SILVER_REQUIRED_FIELDS]
    is_bad_row = reduce(or_, null_conditions)

    bad_rows = df.filter(is_bad_row)
    good_rows = df.filter(~is_bad_row)
    return good_rows, bad_rows
