"""
Layer 1 entrypoint — timer-triggered poll of OpenWeatherMap for every city
in cities.json, enrichment, and publish to Event Hub. See README.md section
3 ("Layer 1 — Ingestion") for the architecture decisions this implements,
and docs/openweathermap_api_reference.md for the API contract.

Per-city error isolation is the main correctness property of this function:
one city's API failure must never prevent the other cities in the same poll
cycle from being collected and published. This is implemented by catching
CityFetchError inside the loop, not around it.
"""

import json
import logging
import os

import azure.functions as func

from shared.owm_client import fetch_weather_and_air_quality, CityFetchError
from shared.enrichment import enrich
from shared.eventhub_publisher import publish, PublishError
from shared.key_vault import get_owm_api_key

logger = logging.getLogger("TimerTriggerCityPoll")

CITIES_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cities.json")


def _load_cities() -> list:
    with open(CITIES_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def main(mytimer: func.TimerRequest) -> None:
    if mytimer.past_due:
        logger.warning("timer trigger is past due — a previous invocation may have run long")

    cities = _load_cities()
    logger.info("poll cycle starting for %d cities", len(cities))

    try:
        api_key = get_owm_api_key()
    except RuntimeError as exc:
        # No key means NO city can be polled — this is the one failure mode
        # that DOES abort the whole cycle, since per-city isolation can't
        # help when the precondition for every city is missing.
        logger.error("aborting poll cycle: %s", exc)
        raise

    enriched_events = []
    failed_cities = []

    for city in cities:
        city_id = city["city_id"]
        try:
            raw = fetch_weather_and_air_quality(city, api_key)
            event = enrich(raw, city)
            enriched_events.append(event)
        except CityFetchError as exc:
            # Logged with full detail inside owm_client already (including
            # the special-cased 401 warning that affects all cities) — here
            # we just track it so the cycle-level summary is accurate and
            # move on to the next city.
            logger.warning("city=%s failed this poll cycle: %s", city_id, exc)
            failed_cities.append(city_id)
            continue
        except Exception as exc:
            # Defense-in-depth, added after code review: the per-city
            # isolation property this function exists to guarantee must
            # hold even for failure modes we haven't specifically
            # anticipated. CityFetchError alone only covers owm_client's
            # own errors — it does NOT cover a bug or unexpected input
            # shape inside enrich() (a real example was found and fixed:
            # OWM returning an explicit "coord": null, not just omitting
            # the key, raised an unguarded AttributeError that would have
            # crashed this entire poll cycle, every city, not just one).
            # Catching Exception broadly here is deliberate despite being
            # generally bad practice — the alternative (one city's
            # malformed response taking down all 12 cities' data for this
            # cycle) is strictly worse, and the full traceback is still
            # logged so the underlying bug remains visible and fixable.
            logger.exception(
                "city=%s failed this poll cycle with an UNEXPECTED error "
                "(not a CityFetchError) — see traceback above; this "
                "indicates a bug, not a normal API failure",
                city_id,
            )
            failed_cities.append(city_id)
            continue

    if not enriched_events:
        logger.error(
            "poll cycle produced ZERO enriched events (%d/%d cities failed) — "
            "nothing to publish this cycle",
            len(failed_cities), len(cities),
        )
        return

    try:
        sent_count = publish(enriched_events)
    except PublishError as exc:
        # Per the publish() docstring: this poll cycle's data is lost, but
        # we don't crash-loop the Function over it — log loudly and let the
        # next scheduled timer invocation try fresh.
        logger.error(
            "publish failed for this entire poll cycle (%d enriched events lost): %s",
            len(enriched_events), exc,
        )
        return

    logger.info(
        "poll cycle complete: %d/%d cities succeeded, %d published, failed_cities=%s",
        len(cities) - len(failed_cities), len(cities), sent_count, failed_cities,
    )
