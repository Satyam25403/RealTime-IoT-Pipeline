"""
PLANNED — OpenWeatherMap API client.

Uses the FREE Current Weather Data + Air Pollution APIs (NOT One Call 4.0,
which requires a card on file). Full endpoint/param/response/error reference:
see docs/openweathermap_api_reference.md — read that before implementing.

Endpoints called per city per poll (2 calls total):
  GET https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={key}&units=metric
  GET https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}

TODO:
- fetch_weather_and_air_quality(city: dict) -> dict
  Calls both endpoints above for a given lat/lon, merges into one dict before
  passing to shared.enrichment.enrich().
- Retry logic: exponential backoff, max 3 attempts. Retry on 429 and 5xx only
  (see docs/openweathermap_api_reference.md section 4 for the full error
  handling table) — 400/401/404 should fail fast for that city, not retry.
- A single city's failure (after retries exhausted) must not raise out of
  this function in a way that aborts the rest of the poll cycle — return
  None or raise a dedicated CityFetchError that __init__.py catches per-city.
- Read the API key from Key Vault (via the user-assigned managed identity in
  Azure) or from local.settings.json / .env when running locally — see
  README.md section 5a for the local connection string.
- Stay inside the rate budget documented in docs/openweathermap_api_reference.md
  section 3 (2 calls × num_cities per poll cycle, well under the 60/min cap
  for a 10-15 city list).
"""
