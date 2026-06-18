"""
Event Hub publisher — Layer 1 ingestion.

Publishes enriched events (output of enrichment.enrich()) to the
`weather-events` Event Hub. Each event is sent with partition_key=city_id —
this is the README's Layer 1 partitioning decision: all events for a given
city must land on the same partition and stay in order, since the
hopping/sliding window queries in Layer 2 aggregate per city and would
produce wrong results if a city's events could be split across partitions
in a non-deterministic order.

Connection string resolution: EVENTHUB_CONNECTION_STRING env var. Points at
the local emulator during dev (see tests/local_dev/docker-compose.yml +
config.json, which defines `weather-events` with 4 partitions matching this
module's assumptions) and at the real Event Hub namespace in Azure (via
main.bicep outputs, injected as a Function App setting) in production. The
SAME connection-string-based auth works in both environments because the
emulator speaks the real Event Hubs wire protocol — no separate code path
needed here, which is exactly the point of using an official emulator
instead of a hand-rolled mock.
"""

import logging
import os
from typing import Iterable

from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import EventHubError

logger = logging.getLogger("eventhub_publisher")

EVENTHUB_NAME = os.environ.get("EVENTHUB_NAME", "weather-events")


class PublishError(Exception):
    """Raised when the batch could not be sent after the client's own retry
    policy is exhausted. The EventHubProducerClient SDK already retries
    transient send failures internally (configurable via retry_total on the
    client), so this wraps only the final, unrecoverable failure — callers
    should treat this as 'none of this poll cycle's events made it to Event
    Hub,' not as a per-event failure."""


def publish(events: Iterable[dict]) -> int:
    """
    Batches the given enriched events and sends them to Event Hub, one
    EventData per event, partitioned by city_id.

    Args:
        events: enriched dicts from enrichment.enrich(), each containing a
            "city_id" key.

    Returns:
        Number of events successfully included in the sent batch.

    Raises:
        PublishError: if the send fails after the SDK's own retry policy.
            Note this is an all-or-nothing failure for THIS BATCH — if it's
            important that a network blip during publish doesn't lose an
            entire poll cycle's events, the caller (TimerTriggerCityPoll)
            should treat a PublishError as "this poll cycle's data is lost,
            log loudly, let the next scheduled poll continue" rather than
            attempting to buffer/retry the whole batch itself, which would
            add complexity disproportionate to a few-KB/s workload.
    """
    events = list(events)
    if not events:
        logger.info("publish() called with zero events — nothing to send")
        return 0

    connection_str = os.environ["EVENTHUB_CONNECTION_STRING"]
    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_str, eventhub_name=EVENTHUB_NAME
    )

    sent_count = 0
    try:
        with producer:
            # Events for different cities can legitimately need different
            # partitions, and the SDK's create_batch(partition_key=...) ties
            # a batch to ONE partition key. So we group by city_id and send
            # one batch per city rather than one batch for the whole poll
            # cycle — still a handful of small batches at this data volume,
            # not a meaningful overhead.
            events_by_city: dict = {}
            for event in events:
                events_by_city.setdefault(event["city_id"], []).append(event)

            for city_id, city_events in events_by_city.items():
                batch = producer.create_batch(partition_key=city_id)
                for event in city_events:
                    try:
                        batch.add(EventData(_to_json_bytes(event)))
                        sent_count += 1
                    except ValueError:
                        # Single event too large for the batch's max size —
                        # send what we have so far and start a new batch
                        # rather than dropping the event.
                        producer.send_batch(batch)
                        batch = producer.create_batch(partition_key=city_id)
                        batch.add(EventData(_to_json_bytes(event)))
                producer.send_batch(batch)

    except EventHubError as exc:
        logger.error("Event Hub publish failed after SDK retries: %s", exc)
        raise PublishError(f"Failed to publish batch: {exc}") from exc

    logger.info("published %d event(s) across %d city/partition-key group(s)",
                sent_count, len(events_by_city) if events else 0)
    return sent_count


def _to_json_bytes(event: dict) -> bytes:
    import json
    return json.dumps(event, separators=(",", ":")).encode("utf-8")