"""
PLANNED — Layer 1 entrypoint (see README.md section 3, "Layer 1 — Ingestion").

Responsibilities (implement in this order):
1. Load cities.json (the virtual sensor list).
2. For each city, call shared.owm_client.fetch_weather_and_air_quality(city).
3. Pass the raw response through shared.enrichment.enrich() to attach
   poll_timestamp, source, and schema_version metadata.
4. Batch-publish enriched events via shared.eventhub_publisher.publish(),
   partitioned by city_id (see README Layer 1 partition key decision).
5. Log structured success/failure per city — failures for one city must not
   abort the rest of the batch.

Timer schedule (function.json) must respect the OWM free-tier cap of
60 calls/min across ALL cities combined, not 60 calls/min per city.
"""

import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    raise NotImplementedError("TODO: implement polling loop — see module docstring")
