"""
Enrichment step — Layer 1 ingestion.

Takes the raw {"weather": ..., "air_pollution": ...} dict from owm_client.py
and flattens + enriches it into the bronze event shape. The output of
enrich() is a flat dict whose keys match BRONZE_SCHEMA field-for-field in
batch/databricks_notebooks/utils/schema_definitions.py — that file is the
source of truth for field names; if you add a field here, add it there too,
and vice versa.

Why flatten here rather than let Spark do it later: this dict is what
actually gets JSON-serialized and published to Event Hub (see
eventhub_publisher.py). Keeping the wire format flat and bronze-schema-
shaped means bronze ingest (01_bronze_ingest.py) can read events with
almost no transformation — it's a landing zone, not a transform step.

Anything from the raw OWM payloads that ISN'T explicitly mapped to a named
field still survives: the full raw merged payload is preserved as
`raw_payload_json` (a JSON string), matching BRONZE_SCHEMA's escape hatch
for fields the API adds in the future that this code doesn't know about yet.
"""

import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger("enrichment")

SCHEMA_VERSION = "1.0"
SOURCE = "azure-function-timer"


def enrich(raw: dict, city: dict) -> dict:
    """
    Args:
        raw: {"weather": {...}, "air_pollution": {...}} from
            owm_client.fetch_weather_and_air_quality().
        city: the matching entry from cities.json (city_id, lat, lon, name).

    Returns:
        Flat dict matching BRONZE_SCHEMA field names. Missing/absent optional
        fields are set to None rather than omitted, so every published event
        has a consistent key set regardless of what OWM happened to include
        for that observation (e.g. `visibility` is sometimes absent).
    """
    weather = raw.get("weather", {}) or {}
    air = raw.get("air_pollution", {}) or {}

    main = weather.get("main", {}) or {}
    wind = weather.get("wind", {}) or {}
    clouds = weather.get("clouds", {}) or {}
    weather_list = weather.get("weather", []) or []
    weather_first = weather_list[0] if weather_list else {}

    air_list = air.get("list", []) or []
    air_first = air_list[0] if air_list else {}
    air_main = air_first.get("main", {}) or {}
    components = air_first.get("components", {}) or {}

    now_utc = datetime.now(timezone.utc)

    event = {
        # --- enrichment metadata [E] ---
        "city_id": city["city_id"],
        "poll_timestamp": now_utc.isoformat(),
        "source": SOURCE,
        "schema_version": SCHEMA_VERSION,
        "ingestion_date": now_utc.date().isoformat(),

        # --- Current Weather API fields [W] ---
        "dt": weather.get("dt"),
        "lat": weather.get("coord", {}).get("lat", city.get("lat")),
        "lon": weather.get("coord", {}).get("lon", city.get("lon")),
        "temp": main.get("temp"),
        "feels_like": main.get("feels_like"),
        "temp_min": main.get("temp_min"),
        "temp_max": main.get("temp_max"),
        "pressure": main.get("pressure"),
        "humidity": main.get("humidity"),
        "visibility": weather.get("visibility"),  # often absent — stays None, matches BRONZE_SCHEMA nullable=True
        "wind_speed": wind.get("speed"),
        "wind_deg": wind.get("deg"),
        "clouds_all": clouds.get("all"),
        "weather_main": weather_first.get("main"),
        "weather_description": weather_first.get("description"),

        # --- Air Pollution API fields [A] ---
        # NOTE: OWM's aqi is a 1-5 scale, not the US EPA 0-500 scale — see
        # docs/openweathermap_api_reference.md section 2. Do not let this
        # silently get compared against EPA-scale thresholds downstream.
        "aqi": air_main.get("aqi"),
        "co": components.get("co"),
        "no": components.get("no"),
        "no2": components.get("no2"),
        "o3": components.get("o3"),
        "so2": components.get("so2"),
        "pm2_5": components.get("pm2_5"),
        "pm10": components.get("pm10"),
        "nh3": components.get("nh3"),

        # --- escape hatch for unmapped fields ---
        "raw_payload_json": json.dumps(raw, separators=(",", ":")),
    }

    _warn_if_missing_critical_fields(event, city["city_id"])
    return event


def _warn_if_missing_critical_fields(event: dict, city_id: str) -> None:
    """Not a hard validation gate (that's SILVER_REQUIRED_FIELDS in
    schema_definitions.py, enforced at the silver stage) — just an early,
    cheap warning at ingestion time so a broken poll is visible in Function
    logs immediately rather than only discovered a day later when the batch
    job quarantines the row."""
    critical = ("temp", "aqi")
    missing = [f for f in critical if event.get(f) is None]
    if missing:
        logger.warning(
            "city=%s enriched event missing critical field(s): %s — "
            "will be quarantined at silver stage if this persists",
            city_id, missing,
        )
