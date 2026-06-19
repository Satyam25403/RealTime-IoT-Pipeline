"""
Pure-Python windowing logic — the core of the local logic-parity test for
Layer 2. Deliberately has ZERO PySpark/Spark dependency, so it can be
unit-tested with plain pytest/assert in any environment, including this
one, without standing up a Spark cluster just to check arithmetic.

pyspark_structured_streaming_equivalent.py wraps these functions in actual
Structured Streaming windowing calls against the Event Hub emulator's
Kafka-compatible endpoint — but the AGGREGATION MATH itself lives here,
tested here, and is imported there. If these functions are correct and the
Spark wrapper just calls them per-window, the wrapper only has wiring left
to get wrong, not arithmetic.

Each function here mirrors exactly one .asaql query in ../asa_queries/ —
the docstring on each function names which one, so drift between "what the
real ASA query does" and "what we verified locally" is easy to spot.

Note on PartitionId: the real .asaql files group by `city_id, PartitionId`
(required once the README's 4-partition Event Hub topology was accounted
for correctly — see the header comments in anomaly_detection.asaql and
rolling_averages.asaql for why). This file's functions group only by
city_id, which is correct for the AGGREGATION MATH itself — a window's
average/max/min over a set of events doesn't depend on which Event Hub
partition those events physically arrived on, only on which city and time
window they belong to. PartitionId is an ASA runtime/parallelism concern,
not an arithmetic one, so it's intentionally not modeled here.
"""

from typing import Optional


def compute_rolling_average_window(events: list[dict]) -> Optional[dict]:
    """
    Mirrors stream_processing/asa_queries/rolling_averages.asaql's SELECT.
    Given all events for ONE city within one 10-minute hopping window,
    returns the same aggregate fields that query produces.

    Args:
        events: list of enriched event dicts (shape matches
            ingestion/function_app/shared/enrichment.py's output), all
            assumed to already belong to the same city and the same window
            — windowing/grouping itself happens in the Spark wrapper, not
            here; this function is pure aggregation over a pre-grouped list.

    Returns:
        dict matching rolling_averages.asaql's output columns, or None if
        events is empty (an empty window produces no output row, matching
        ASA's behavior of only emitting once a window has content).
    """
    if not events:
        return None

    temps = [e["temp"] for e in events if e.get("temp") is not None]
    humidities = [e["humidity"] for e in events if e.get("humidity") is not None]
    aqis = [e["aqi"] for e in events if e.get("aqi") is not None]
    pm2_5s = [e["pm2_5"] for e in events if e.get("pm2_5") is not None]
    pm10s = [e["pm10"] for e in events if e.get("pm10") is not None]

    return {
        "city_id": events[0]["city_id"],
        "avg_temp": sum(temps) / len(temps) if temps else None,
        "min_temp": min(temps) if temps else None,
        "max_temp": max(temps) if temps else None,
        "avg_humidity": sum(humidities) / len(humidities) if humidities else None,
        "avg_aqi": sum(aqis) / len(aqis) if aqis else None,
        "avg_pm2_5": sum(pm2_5s) / len(pm2_5s) if pm2_5s else None,
        "avg_pm10": sum(pm10s) / len(pm10s) if pm10s else None,
        "observation_count": len(events),
    }


def compute_anomaly_detection_window(events: list[dict]) -> Optional[dict]:
    """
    Mirrors stream_processing/asa_queries/anomaly_detection.asaql's SELECT
    + HAVING. Given all events for ONE city within one 15-minute sliding
    window, returns the aggregate fields AND the two trigger flags — but
    ONLY if the HAVING condition would actually fire (temp swing > 8, or
    max AQI >= 5). Returns None otherwise, matching ASA's behavior of never
    emitting a row for a window that doesn't satisfy HAVING.

    Thresholds (8 degrees C swing, AQI >= 5) are duplicated here from the
    .asaql file rather than imported from it, since the .asaql file isn't
    Python-importable — if you change the threshold in one place, change
    it in the other. This duplication is the one thing this test can't
    protect you from automatically; it's flagged here so it isn't missed.
    """
    if not events:
        return None

    temps = [e["temp"] for e in events if e.get("temp") is not None]
    aqis = [e["aqi"] for e in events if e.get("aqi") is not None]
    pm2_5s = [e["pm2_5"] for e in events if e.get("pm2_5") is not None]

    if not temps or not aqis:
        return None

    window_max_temp = max(temps)
    window_min_temp = min(temps)
    temp_swing = window_max_temp - window_min_temp
    window_max_aqi = max(aqis)

    temperature_swing_triggered = temp_swing > 8
    aqi_threshold_triggered = window_max_aqi >= 5

    if not (temperature_swing_triggered or aqi_threshold_triggered):
        return None  # HAVING condition didn't fire — ASA emits nothing for this window

    return {
        "city_id": events[0]["city_id"],
        "window_max_temp": window_max_temp,
        "window_min_temp": window_min_temp,
        "temp_swing": temp_swing,
        "window_max_aqi": window_max_aqi,
        "window_max_pm2_5": max(pm2_5s) if pm2_5s else None,
        "temperature_swing_triggered": temperature_swing_triggered,
        "aqi_threshold_triggered": aqi_threshold_triggered,
    }
