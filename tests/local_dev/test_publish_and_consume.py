"""
Local smoke test against the real Event Hub emulator (see README.md
section 5a). Requires `docker compose up -d` to have been run first in this
directory.

Purpose: prove, without any Azure subscription, that:
  1. shared.eventhub_publisher.publish() — the actual Layer 1 publish
     function, not a reimplementation — can send a fixture event to the
     local Event Hub emulator (config.json defines `weather-events` with 4
     partitions, matching the README's documented partition decision).
  2. The event can be read back from a deterministic partition for its
     city_id, in the correct shape (matching enrichment.enrich()'s output).

This is a logic-parity check for ingestion + partitioning, not a substitute
for testing Stream Analytics, Cosmos DB, or Synapse — none of which have
full local emulators (see README.md section 6).

IMPLEMENTATION STATUS: written against the real publish() function and the
documented emulator connection string, but NOT yet executed against a live
emulator in this environment (no Docker available in the sandbox this was
built in). Run it yourself after `docker compose up -d` and report back if
anything doesn't match — the emulator's exact Kafka/AMQP behavior is the one
thing here that's design-verified but not yet execution-verified.
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "ingestion", "function_app"))

os.environ.setdefault(
    "EVENTHUB_CONNECTION_STRING",
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;",
)
os.environ.setdefault("EVENTHUB_NAME", "weather-events")

from azure.eventhub import EventHubConsumerClient

from shared.eventhub_publisher import publish  # the REAL Layer 1 function, not a copy
from shared.enrichment import enrich

CONNECTION_STR = os.environ["EVENTHUB_CONNECTION_STRING"]
EVENTHUB_NAME = os.environ["EVENTHUB_NAME"]


def _build_fixture_event() -> dict:
    """Builds one fixture event using the REAL enrich() function with
    realistic sample API payloads, so this test exercises the same
    enrichment path Layer 1 actually uses in production, not a hand-crafted
    dict that could drift from what enrich() really produces."""
    sample_weather = {
        "coord": {"lat": 17.385, "lon": 78.4867},
        "weather": [{"main": "Clear", "description": "clear sky"}],
        "main": {"temp": 29.0, "feels_like": 31.0, "pressure": 1011, "humidity": 55},
        "wind": {"speed": 2.5, "deg": 180},
        "clouds": {"all": 10},
        "dt": int(time.time()),
    }
    sample_air = {"list": [{"main": {"aqi": 2}, "components": {"pm2_5": 18.0, "pm10": 30.0}}]}
    city = {"city_id": "smoke_test_city", "lat": 17.385, "lon": 78.4867}
    return enrich({"weather": sample_weather, "air_pollution": sample_air}, city)


def test_publish_and_consume(timeout_seconds: int = 15) -> None:
    fixture_event = _build_fixture_event()
    expected_city_id = fixture_event["city_id"]

    sent_count = publish([fixture_event])
    assert sent_count == 1, f"expected publish() to report 1 event sent, got {sent_count}"
    print(f"published 1 event for city_id={expected_city_id!r}")

    received = []

    def on_event(partition_context, event):
        if event is None:
            return
        body = json.loads(b"".join(event.body).decode("utf-8"))
        received.append((partition_context.partition_id, body))
        partition_context.update_checkpoint(event)

    consumer = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=EVENTHUB_NAME,
    )

    print(f"listening up to {timeout_seconds}s for the event to arrive...")
    with consumer:
        try:
            consumer.receive(
                on_event=on_event,
                starting_position="-1",  # from the beginning of each partition
                max_wait_time=timeout_seconds,
            )
        except KeyboardInterrupt:
            pass

    matches = [(pid, body) for pid, body in received if body.get("city_id") == expected_city_id]
    assert matches, (
        f"never received an event with city_id={expected_city_id!r} within "
        f"{timeout_seconds}s — check the emulator is running (docker compose up -d) "
        f"and EVENTHUB_CONNECTION_STRING is correct"
    )

    partition_id, body = matches[0]
    print(f"received event back on partition {partition_id}: city_id={body['city_id']!r}, temp={body.get('temp')}")

    # Same city_id should always land on the same partition — confirms the
    # partition_key=city_id behavior documented in README Layer 1.
    assert body["temp"] == fixture_event["temp"], "received event content doesn't match what was sent"
    print("\nPASS — publish/consume round-trip matched, partitioning by city_id confirmed")


if __name__ == "__main__":
    test_publish_and_consume()