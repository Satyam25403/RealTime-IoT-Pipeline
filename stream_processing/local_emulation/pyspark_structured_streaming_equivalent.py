"""
PLANNED — Local logic-parity test for Layer 2.

Azure Stream Analytics has no full local emulator, so this PySpark Structured
Streaming job re-implements the SAME windowing semantics (hopping window for
rolling averages, sliding window for anomaly detection) against the local
Event Hub emulator / Kafka-compatible endpoint, purely to validate the LOGIC
before translating it into ASA query language (see ../asa_queries/*.asaql).

This file is a validation tool, not a production component — it is never
deployed to Azure.

TODO:
- Read from the Event Hub emulator's Kafka-compatible port (see
  tests/local_dev/docker-compose.yml).
- Mirror rolling_averages.asaql using df.withWatermark(...).groupBy(window(...))
  with a hop expressed as two overlapping tumbling windows, since native
  PySpark lacks a direct hopping-window primitive.
- Mirror anomaly_detection.asaql using a sliding window with the same
  thresholds, and assert output rows match expected fixtures.
"""
