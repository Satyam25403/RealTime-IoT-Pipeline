"""
PLANNED — Event Hub publisher.

TODO:
- publish(events: list[dict]) -> None
  Batches events using EventHubProducerClient, sets partition_key=city_id per
  event (see README Layer 1 — partition key decision), sends the batch.
- Connection string comes from EVENTHUB_CONNECTION_STRING env var — points at
  the local emulator during dev (see tests/local_dev/docker-compose.yml) and
  at the real Event Hub namespace in Azure (via main.bicep outputs) in prod.
"""
