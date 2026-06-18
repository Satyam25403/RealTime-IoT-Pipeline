"""
PLANNED — Enrichment step.

TODO:
- enrich(raw_response: dict, city: dict) -> dict
  Adds: poll_timestamp (UTC ISO8601), source="azure-function-timer",
  schema_version, and the city_id (used later as the Event Hub partition key).
"""
