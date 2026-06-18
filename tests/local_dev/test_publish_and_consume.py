"""
PLANNED — local smoke test (see README.md section 5a).

Purpose: prove, without any Azure subscription, that:
  1. A test event matching the Layer 1 enrichment shape can be published to
     the local Event Hub emulator (config.json defines `weather-events` with
     4 partitions, matching the README's documented partition decision).
  2. The event can be read back from the correct partition (keyed by city_id)
     in the correct shape.

This is a logic-parity check for ingestion + partitioning, not a substitute
for testing Stream Analytics, Cosmos DB, or Synapse — none of which have full
local emulators (see README.md section 6).

TODO:
- from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData
- CONNECTION_STR = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
- producer = EventHubProducerClient.from_connection_string(CONNECTION_STR, eventhub_name="weather-events")
- Build a batch with partition_key=<city_id>, send a fixture event matching
  shared.enrichment.enrich()'s output shape.
- consumer = EventHubConsumerClient.from_connection_string(CONNECTION_STR, consumer_group="$Default", eventhub_name="weather-events")
- Read back, assert the event matches what was sent and landed on a
  deterministic partition for that city_id.
"""

def test_publish_and_consume():
    raise NotImplementedError("TODO: implement per module docstring")


if __name__ == "__main__":
    test_publish_and_consume()
