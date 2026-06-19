# OpenWeatherMap API reference (Layer 1 ingestion)

## Important: which API this project actually uses

The assignment brief specifies **"openweathermap.org/api — free tier, up to 60
calls/min."** That figure is the signature of OpenWeather's **classic Free
Access APIs** (Current Weather Data + Air Pollution), not One Call API 4.0.

These are two genuinely different products and it's worth being precise about
which is which, since mixing them up has real cost consequences:

| | Current Weather + Air Pollution (used here) | One Call API 4.0 |
|---|---|---|
| Cost | Free, no card required | Requires a card on file — "One Call by Call" subscription |
| Free allowance | 60 calls/min, 1,000,000 calls/month | 1,000 calls/day, then pay-per-call |
| Base path | `/data/2.5/weather`, `/data/2.5/air_pollution` | `/data/4.0/onecall/*` |
| Matches assignment's "60 calls/min, free tier" wording | Yes, exactly | No — different limit shape entirely |

**Decision: this project uses the free Current Weather Data API and the free
Air Pollution API exclusively for Layer 1 ingestion.** One Call 4.0 is
documented below as an optional future upgrade path (richer payload: UV
index, dew point, minute-level precipitation, government alerts) but is
deliberately *not* wired into the pipeline, because requiring a credit card
on file conflicts with this project's "moderate cost" goal and with running
indefinitely on a free account with no billing exposure.

This is also why `ingestion/function_app/shared/owm_client.py` calls two
endpoints per city per poll (one weather call, one air-quality call), not
one — see the rate-budget math below.

---

## 1. Current Weather Data API (free)

**Endpoint**
```
GET https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API key}
```

**Parameters**

| Name | Required | Description |
|---|---|---|
| `lat` | required | Latitude, decimal (-90 to 90) |
| `lon` | required | Longitude, decimal (-180 to 180) |
| `appid` | required | API key, from account → API keys tab |
| `units` | optional | `standard` (Kelvin, default), `metric` (Celsius), `imperial` (Fahrenheit) |
| `lang` | optional | Output language for the `description` field |
| `mode` | optional | `json` (default), `xml`, or `html` (HTML only supported here, not on Air Pollution) |

We call by `lat`/`lon` (from `cities.json`), not by city name — coordinate
lookup is the most reliable option and avoids relying on OpenWeather's
deprecated built-in geocoder.

**Example response**
```json
{
  "coord": { "lon": 10.99, "lat": 44.34 },
  "weather": [
    { "id": 501, "main": "Rain", "description": "moderate rain", "icon": "10d" }
  ],
  "base": "stations",
  "main": {
    "temp": 298.48,
    "feels_like": 298.74,
    "temp_min": 297.56,
    "temp_max": 300.05,
    "pressure": 1015,
    "humidity": 64,
    "sea_level": 1015,
    "grnd_level": 933
  },
  "visibility": 10000,
  "wind": { "speed": 0.62, "deg": 349, "gust": 1.18 },
  "rain": { "1h": 3.16 },
  "clouds": { "all": 100 },
  "dt": 1661870592,
  "sys": { "type": 2, "id": 2075663, "country": "IT", "sunrise": 1661834187, "sunset": 1661882248 },
  "timezone": 7200,
  "id": 3163858,
  "name": "Zocca",
  "cod": 200
}
```

**Fields used by this project** (mapped onto the enrichment schema in
`shared/enrichment.py`):

| Field | Meaning | Used as |
|---|---|---|
| `dt` | Observation time, Unix UTC | `observation_timestamp` (after conversion) |
| `main.temp` | Temperature (units per `units` param) | rolling-average + anomaly input |
| `main.feels_like` | Perceived temperature | stored, not currently aggregated |
| `main.humidity` | Humidity, % | stored |
| `main.pressure` | Sea-level pressure, hPa | stored |
| `weather[0].main` / `.description` | Condition group / text | stored for dashboard display |
| `wind.speed`, `wind.deg` | Wind speed + direction | stored |
| `clouds.all` | Cloudiness, % | stored |
| `coord.lat` / `coord.lon` | Echo of request coords | sanity-check against `cities.json` |

Note: `temp_min`/`temp_max` here are **not** a forecast range — for this
endpoint they mean the min/max temperature currently observed across a
geographically large city's weather stations, relevant mainly to sprawling
metros. Don't confuse this with the daily forecast min/max from other
products.

---

## 2. Air Pollution API (free)

**Endpoint**
```
GET https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API key}
```

Same `lat`/`lon`/`appid` parameters as above; no `units` parameter (pollutant
concentrations are always returned in μg/m³ except CO, which is in μg/m³ as
well per the official spec — no unit conversion is offered for this product).

**Example response**
```json
{
  "coord": [50.0, 50.0],
  "list": [
    {
      "dt": 1606147200,
      "main": { "aqi": 4.0 },
      "components": {
        "co": 203.609,
        "no": 0.0,
        "no2": 0.396,
        "o3": 75.102,
        "so2": 0.648,
        "pm2_5": 23.253,
        "pm10": 92.214,
        "nh3": 0.117
      }
    }
  ]
}
```

**Fields used by this project**:

| Field | Meaning |
|---|---|
| `list[0].main.aqi` | OpenWeather's own 1–5 AQI scale (1=Good … 5=Very Poor) — *not* the US EPA 0–500 scale, see note below |
| `list[0].components.pm2_5` / `.pm10` | Particulate matter — primary anomaly-detection input alongside temperature |
| `list[0].components.*` (co, no, no2, o3, so2, nh3) | Stored for completeness, not currently used in anomaly thresholds |
| `list[0].dt` | Observation time, Unix UTC — should match (or closely follow) the weather call's `dt` for the same poll cycle |

**Important scale note**: OpenWeather's `aqi` field is a 1–5 index unique to
OpenWeather, not the more commonly seen US EPA 0–500 AQI. If this project's
anomaly thresholds or dashboard ever need to be compared against AQI values
from another source, this distinction needs to be called out — it's a common
source of silently wrong-looking dashboards.

Forecast (`/air_pollution/forecast`) and historical
(`/air_pollution/history`) variants of this endpoint exist and are also free,
but are out of scope for Layer 1, which only needs current conditions.

---

## 3. Rate budget — staying inside 60 calls/min

Each poll cycle costs **2 API calls per city** (one weather, one air
pollution). With the 60 calls/min ceiling shared across both endpoints:

```
max_cities_per_poll = 60 / 2 = 30 cities, IF polling every 60 seconds
```

But polling every 60 seconds for every city simultaneously is needlessly
aggressive for a weather use case (conditions don't meaningfully change
minute to minute) and leaves zero headroom for retries. The Function's timer
schedule (`function.json`, currently `0 */5 * * * *` — every 5 minutes) gives
much more headroom:

```
calls_per_5min_window = num_cities × 2
60-call ceiling applies per minute, not per 5-minute window, so as long as
all of a poll cycle's calls complete within roughly a few seconds, even a
15-city list (30 calls) finishes well inside one minute, leaving the
remaining ~4 minutes and 55 seconds of the window completely idle.
```

With `cities.json` at 10–15 cities, a single poll cycle uses 20–30 calls,
roughly half the per-minute ceiling — comfortable headroom for retries on
transient failures without ever approaching a 429.

**On 429 (rate limit exceeded)**: `shared/owm_client.py` should treat this as
retryable with exponential backoff (not a per-city fatal error — see the
docstring in that file), since a transient burst (e.g. Azure Function cold
start triggering several cities' calls in close succession) is the most
likely cause, not a structural overage.

---

## 4. Error response shape (both endpoints)

```json
{
  "cod": 400,
  "message": "Invalid date format",
  "parameters": ["date"]
}
```

| Code | Common cause | Handling in `owm_client.py` |
|---|---|---|
| 400 | Malformed request parameters | Log and skip this city for this poll cycle — likely a `cities.json` data error, not transient |
| 401 | Invalid or not-yet-active API key | New keys can take time to activate — fail loudly, this needs human attention, don't silently retry forever |
| 404 | Bad coordinates (rare when using lat/lon directly) | Log and skip this city |
| 429 | Rate limit exceeded | Retry with exponential backoff, max 3 attempts (see rate budget above) |
| 5xx | OpenWeather-side outage | Retry with backoff; if still failing after max attempts, skip this city for this cycle and let the next scheduled poll try again |

A single city's failure must never abort the whole poll cycle — this is
already called out in `TimerTriggerCityPoll/__init__.py`'s docstring and is
worth re-emphasizing here: it's the difference between losing one city's
data point for 5 minutes versus losing the entire fleet's data for 5 minutes.

---

## 5. Optional upgrade path: One Call API 4.0

Documented for completeness, not implemented. If this project ever needs UV
index, dew point, minute-level precipitation, or government weather alerts
in a single call, One Call 4.0 is the product that provides them — but
switching requires accepting the "One Call by Call" subscription (card on
file, 1,000 free calls/day, then pay-per-call). Endpoints, for reference:

- Current: `GET /data/4.0/onecall/current?lat={lat}&lon={lon}&appid={key}`
- Timelines: `GET /data/4.0/onecall/timeline/{1min|15min|1h|1day}?lat={lat}&lon={lon}&appid={key}`
- Alert detail: `GET /data/4.0/onecall/alert/{alert_id}?appid={key}`

These endpoints use **pagination** (`next`/`prev` URLs in the response body)
for the timeline variants — each paginated follow-up request counts as a
separate billed call, which is an extra cost trap worth knowing about before
ever switching to this product. Response record limits per call: 1-minute
timeline returns up to 60 records, 15-minute up to 50, 1-hour up to 20,
1-day up to 10.

Should this upgrade ever happen, only `shared/owm_client.py` needs to change
— `shared/enrichment.py` and everything downstream already operates on the
enriched/normalized event shape, not the raw API response shape, by design.
