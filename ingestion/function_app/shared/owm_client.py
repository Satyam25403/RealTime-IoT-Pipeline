"""
OpenWeatherMap API client — Layer 1 ingestion.

Uses the FREE Current Weather Data + Air Pollution APIs (NOT One Call 4.0,
which requires a card on file). Full endpoint/param/response/error reference:
see docs/openweathermap_api_reference.md.

Endpoints called per city per poll (2 calls total):
  GET https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={key}&units=metric
  GET https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}

Retry policy (see docs/openweathermap_api_reference.md section 4):
  429, 5xx -> retryable, exponential backoff, max 3 attempts
  400, 401, 404 -> fail fast, no retry (data/config error, not transient)

A single city's failure never raises out past fetch_weather_and_air_quality
in a way that kills the whole poll cycle — callers (TimerTriggerCityPoll)
catch CityFetchError per city and move on to the next one.
"""

import logging
import time
from typing import Optional

import requests

logger = logging.getLogger("owm_client")

WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
AIR_POLLUTION_URL = "https://api.openweathermap.org/data/2.5/air_pollution"

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 3
BASE_BACKOFF_SECONDS = 2  # exponential: 2s, 4s, 8s
REQUEST_TIMEOUT_SECONDS = 10


class CityFetchError(Exception):
    """Raised when a city's data could not be fetched after retries (if
    retryable) or immediately (if a fail-fast error). Callers catch this
    per-city — it must never propagate up and abort the whole poll cycle."""

    def __init__(self, city_id: str, message: str, status_code: Optional[int] = None):
        self.city_id = city_id
        self.status_code = status_code
        super().__init__(f"[{city_id}] {message}")


def _get_with_retry(url: str, params: dict, city_id: str, label: str) -> dict:
    """Single GET with the retry policy described in the module docstring.
    `label` is just "weather" or "air_pollution", used in log/error messages
    so failures are traceable to the right endpoint."""
    last_exception: Optional[Exception] = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            # Network-level failure (DNS, connection reset, timeout) is
            # treated as retryable — it's almost always transient.
            last_exception = exc
            logger.warning(
                "city=%s endpoint=%s attempt=%d/%d network error: %s",
                city_id, label, attempt, MAX_ATTEMPTS, exc,
            )
            if attempt < MAX_ATTEMPTS:
                time.sleep(BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
                continue
            raise CityFetchError(city_id, f"{label} network error after {MAX_ATTEMPTS} attempts: {exc}") from exc

        if response.status_code == 200:
            return response.json()

        if response.status_code in RETRYABLE_STATUS_CODES:
            last_exception = None
            logger.warning(
                "city=%s endpoint=%s attempt=%d/%d status=%d (retryable)",
                city_id, label, attempt, MAX_ATTEMPTS, response.status_code,
            )
            if attempt < MAX_ATTEMPTS:
                time.sleep(BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
                continue
            raise CityFetchError(
                city_id,
                f"{label} still failing after {MAX_ATTEMPTS} attempts, last status {response.status_code}",
                status_code=response.status_code,
            )

        # 400 / 401 / 404 / any other non-retryable code: fail fast.
        # 401 in particular gets a distinct log level — an invalid/inactive
        # key is an operator problem, not a per-city data problem, and is
        # worth being loud about (see docs/openweathermap_api_reference.md
        # section 4 — "fail loudly, this needs human attention").
        body_snippet = response.text[:200]
        if response.status_code == 401:
            logger.error(
                "city=%s endpoint=%s status=401 — API key invalid or not yet active. "
                "This affects ALL cities, not just this one. body=%s",
                city_id, label, body_snippet,
            )
        else:
            logger.error(
                "city=%s endpoint=%s status=%d (non-retryable) body=%s",
                city_id, label, response.status_code, body_snippet,
            )
        raise CityFetchError(
            city_id,
            f"{label} non-retryable status {response.status_code}: {body_snippet}",
            status_code=response.status_code,
        )

    # Should be unreachable given the loop above always returns or raises,
    # but keeps mypy/pylint happy and fails safe rather than returning None.
    raise CityFetchError(city_id, f"{label} exhausted retries unexpectedly: {last_exception}")


def fetch_weather_and_air_quality(city: dict, api_key: str) -> dict:
    """
    Fetches both endpoints for one city and merges them into a single dict
    with two top-level keys: "weather" and "air_pollution", each holding the
    raw JSON response from that endpoint. Does NOT reshape the data —
    reshaping into the bronze schema happens in enrichment.py, by design
    (see that module's docstring), so this function's only job is "get the
    two raw payloads, or raise CityFetchError."

    Args:
        city: one entry from cities.json, must have "city_id", "lat", "lon".
        api_key: OWM API key (caller resolves this from Key Vault or env var
            — see shared/key_vault.py).

    Returns:
        {"weather": {...raw response...}, "air_pollution": {...raw response...}}

    Raises:
        CityFetchError: if either endpoint fails after the retry policy above.
            Caller (TimerTriggerCityPoll/__init__.py) catches this per-city.
    """
    city_id = city["city_id"]
    params = {"lat": city["lat"], "lon": city["lon"], "appid": api_key, "units": "metric"}

    weather = _get_with_retry(WEATHER_URL, params, city_id, "weather")
    # air_pollution takes the same lat/lon/appid but no `units` param (the
    # API doesn't support unit conversion for pollutant concentrations —
    # see docs/openweathermap_api_reference.md section 2).
    air_params = {"lat": city["lat"], "lon": city["lon"], "appid": api_key}
    air_pollution = _get_with_retry(AIR_POLLUTION_URL, air_params, city_id, "air_pollution")

    return {"weather": weather, "air_pollution": air_pollution}
