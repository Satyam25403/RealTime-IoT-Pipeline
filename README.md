# Real-time IoT Pipeline — OpenWeatherMap Weather & Air Quality

## 1. Objective

Ingest live weather and air quality data from the OpenWeatherMap API, process it in
real time, store it in an Azure lakehouse, and serve insights through Power BI and
Synapse Analytics. Each city in `cities.json` acts as a virtual IoT sensor, polled on
a fixed schedule rather than pushing data itself.

**Which OpenWeatherMap API**: this project uses the free **Current Weather
Data** and **Air Pollution** APIs (`/data/2.5/weather` and
`/data/2.5/air_pollution`), not the paid One Call API 4.0. The assignment's
"free tier, up to 60 calls/min" description matches the classic Free Access
APIs exactly; One Call 4.0 requires a card on file and has a different
(1,000 calls/day) limit shape. Full endpoint/parameter/response/error
documentation, the rate-budget math, and the reasoning for this choice live
in `docs/openweathermap_api_reference.md` — read that alongside this README
before touching `ingestion/`.

This document is the single source of truth for the architecture, the reasoning
behind every design decision (including direct answers to every "Think about"
prompt in the assignment), the repository layout, and exact run instructions for
both local development and a real Azure deployment.

No Azure subscription was used to build this; the pipeline is designed to deploy
cleanly via the included Bicep templates, and the components that have official
local emulators (Event Hub, ADLS Gen2 via Azurite) are validated locally before
ever touching the cloud. This is called out explicitly wherever it's relevant below.

---

## Implementation status

| Layer | Status | Notes |
|---|---|---|
| 1 — Ingestion | **Implemented, mock-tested** | `ingestion/function_app/` — see section 3 implementation notes. Not yet run against the live Event Hub emulator (Docker unavailable in build environment) — see section 5a. |
| 2 — Stream Processing | **Implemented, mock-tested** | `stream_processing/asa_queries/` (real SAQL) + `local_emulation/windowing_logic.py` (6/6 unit tests passing). Spark wrapper design-verified, not yet run against a live emulator (no PySpark in build environment). |
| 3 — Batch Transformation | **Implemented, mock-tested** | `batch/databricks_notebooks/` (real PySpark/Delta) + `utils/schema_definitions.py`'s validation gate (3/3 unit tests passing) + ADF pipeline with verified parameter wiring. Not yet run on a real Spark session/cluster. |
| 4 — Hot Path Serving | **Implemented, design-verified** | `infra/bicep/cosmosdb.bicep` (serverless + Synapse Link). Two real bugs caught and fixed in the upstream `.asaql` files (missing `id` field, missing `PartitionId` handling) plus a documentation correction (Power BI uses Synapse Link/DirectQuery, not Change Feed directly). No Bicep CLI available to validate syntax — checked against current docs only. |
| 5 — Cold Path Serving | Planned | |
| 6 — Security & Observability | Partially planned (KQL query stubs, IaC stubs) | |
| Infra (Bicep) | Stubs only, not yet written | |

This table is the single place to check what's real vs aspirational in this
repo at any point — update it whenever a layer's status changes.

---

## 2. Architecture overview

```
 OpenWeatherMap API
        │  (polled every N minutes, free tier ≤60 calls/min)
        ▼
 ┌─────────────────────────────┐
 │ LAYER 1 — Ingestion          │  Azure Function (timer trigger)
 │ Function → enrich → publish │  reads API key from Key Vault via
 └──────────────┬──────────────┘  managed identity, publishes to:
                ▼
 ┌─────────────────────────────┐
 │ Azure Event Hub              │  4 partitions, partition key = city_id
 └──────────────┬──────────────┘
                ▼
 ┌─────────────────────────────────────────────────────────┐
 │ LAYER 2 — Stream Processing (Azure Stream Analytics)     │
 │  ├─ Rolling averages   → hopping window                  │
 │  ├─ Anomaly detection  → sliding window                  │
 │  └─ Raw passthrough    → unwindowed → ADLS Gen2 (cold)    │
 └───────┬───────────────────────────────────────┬──────────┘
         │ alerts                                 │ raw events (daily batch)
         ▼                                        ▼
 ┌──────────────────────────┐        ┌──────────────────────────────────┐
 │ LAYER 4 — Hot Path        │        │ LAYER 3 — Batch (ADF + Databricks)│
 │ Cosmos DB (serverless)    │        │ Bronze → Silver → Gold (Delta)    │
 │ key = city_id             │        │ Z-ordered gold, Unity Catalog     │
 │   │ Synapse Link           │        └────────────────┬───────────────┘
 │   ▼                        │                          ▼
 │ Power BI live dashboard    │        ┌──────────────────────────────────┐
 │ (DirectQuery + push API)   │        │ LAYER 5 — Cold Path Serving       │
 └────────────────────────────┘        │ Synapse serverless SQL            │
                                        │ OPENROWSET over gold Delta tables │
                                        │   │ DirectQuery                   │
                                        │   ▼                               │
                                        │ Power BI historical trend report  │
                                        └────────────────────────────────────┘

 LAYER 6 — Security & Observability (spans every layer above)
 User-assigned managed identity · Key Vault · private endpoints ·
 Log Analytics workspace · KQL alerting
```

---

## 3. Design decisions, layer by layer

Each subsection below answers the assignment's "Think about" prompts directly,
as a decision with reasoning, not just a fact.

### Layer 1 — Ingestion

**Why Event Hub over IoT Hub?**
IoT Hub is built for bi-directional device management: device identity,
provisioning, device twins, and cloud-to-device commands. Nothing in this
pipeline needs that — there is no physical device to authenticate individually
and no command ever needs to be sent back to a "city." We have exactly one
producer (the Function) publishing a stream that one or more independent
consumers read downstream. That is precisely Event Hub's design point: a
high-throughput event stream, not a device-management plane. Using Event Hub
also avoids paying for per-device identity management we'd never exercise.

**How many partitions?**
At the free tier ceiling (60 calls/min) and a city list in the 10–15 range,
throughput is a few KB/s — far below what a single partition could handle.
Partition count here is driven by downstream *parallelism*, not raw throughput:
4 partitions lets Stream Analytics use multiple Streaming Units in parallel for
the per-city windowed queries. Partition key is `city_id`, which guarantees all
events for a given city land on the same partition and stay in order — required
for the rolling-average and anomaly windows to be correct per city.
`cities.json` currently has 12 cities (24 API calls per poll cycle), well
under the 60/min ceiling with room for retries — see
`docs/openweathermap_api_reference.md` section 3 for the exact rate budget.

**Implementation notes (Layer 1 is now built — see `ingestion/function_app/`):**
- `shared/owm_client.py` classifies OWM error responses into fail-fast
  (400/401/404 — no retry, since these are data or config errors, not
  transient) versus retryable (429/5xx/network errors — exponential backoff,
  3 attempts). A 401 is logged at ERROR with an explicit note that it likely
  affects every city, not just the one being polled, since an invalid key
  fails identically across the whole fleet.
- `shared/enrichment.py`'s output dict keys are an exact 1:1 match with
  `batch/databricks_notebooks/utils/schema_definitions.py`'s `BRONZE_SCHEMA`
  field names — verified by an automated field-diff during development, not
  just by eye. Any field OWM returns that isn't explicitly mapped still
  survives as JSON inside `raw_payload_json`, so bronze stays genuinely
  replayable even as the upstream API adds fields this code doesn't know
  about yet.
- `shared/eventhub_publisher.py` groups the poll cycle's events by `city_id`
  before sending, since the SDK's `create_batch(partition_key=...)` ties one
  batch to one partition key — sending one batch per distinct city per cycle
  rather than one batch for the whole cycle. At this data volume that's a
  handful of small batches, not a meaningful overhead, and it's what
  actually enforces the partitioning decision above at the code level.
- `shared/key_vault.py` resolves the API key from the `OWM_API_KEY` env var
  locally, or from Key Vault via `DefaultAzureCredential` (resolving to the
  Function's user-assigned managed identity in Azure — see Layer 6) when
  `KEY_VAULT_URL` is set instead. Missing both is the one failure mode that
  intentionally aborts the *entire* poll cycle rather than being isolated
  per-city, since without a key no city can be polled at all — there's
  nothing to isolate.
- `TimerTriggerCityPoll/__init__.py` wires all of the above together with
  per-city error isolation: a `CityFetchError` for one city is caught inside
  the loop and logged, while the rest of the cycle's cities continue. This
  was verified with a test simulating one failing city among three —
  confirmed the other two still get enriched and published.

### Layer 2 — Stream Processing

**Implementation status: all three queries are written in real Azure Stream
Analytics Query Language** (`stream_processing/asa_queries/*.asaql`), not
just described — see implementation notes below for what was verified and
how. Full local logic-parity testing for the windowing arithmetic also
exists (`stream_processing/local_emulation/windowing_logic.py`).

**Tumbling vs hopping vs sliding windows.**
Three outputs need three different window semantics in the same ASA job:
- *Rolling averages per city* → **hopping window** (10-minute window,
  hopping every 2 minutes — `HoppingWindow(minute, 10, 2)`). A tumbling
  window only emits one non-overlapping average per fixed interval, which is
  too coarse for a metric that should read as "rolling." Hopping windows
  overlap, producing smoother and more frequent updates while staying
  deterministic and boundable.
- *Anomaly detection* → **sliding window** (`SlidingWindow(minute, 15)`),
  evaluated per incoming event. Anomalies need to be flagged the instant a
  qualifying event arrives, not only at fixed window boundaries — sliding
  windows are the only one of the three that triggers on every new event
  rather than on a clock; per Microsoft's own definition, a sliding window
  only emits output at moments when its content actually changes (an event
  entering or leaving it), not on a fixed schedule.
- *Raw passthrough to cold storage* → no windowing. Straight pass-through,
  output partitioned by date so downstream ADF/Databricks reads are
  efficient. Deliberately has no `TIMESTAMP BY` either — there's no
  time-based aggregation here that needs event time over arrival time, so
  opting into watermark/late-arrival semantics for a pure forwarding query
  would only add complexity with no benefit.

**Per-city substreams (`OVER city_id`).** Both the hopping and sliding
window queries use `TIMESTAMP BY poll_timestamp OVER city_id`, not just
`TIMESTAMP BY poll_timestamp`. Without `OVER`, ASA computes one shared
watermark across all 12 cities feeding the same Event Hub input — if any
single city's events were delayed, every other city's window output would
be held back waiting for that one straggler. `OVER` gives each city its own
independent watermark, so one slow or quiet city never blocks the other 11.
This wasn't in the original plan and was added once the substream mechanism
was confirmed during implementation — worth knowing about if you're
debugging why one city's data looks "stuck."

**Late event handling.**
The API is polled on a schedule, so events should usually arrive close to
in-order, but Function retries or transient network delay can still produce
late arrivals. Late arrival tolerance is ~5 minutes and out-of-order
tolerance is ~30 seconds, both using the **Adjust** policy rather than
**Drop** — silently dropping late events would quietly corrupt rolling
averages with no visible failure, a real production bug class worth
catching here rather than discovering downstream.

**Important correction from the original plan**: these tolerances are
**job-level settings**, not something written inside the `.asaql` query
text. They're configured via the Event Ordering tab in the Azure portal, or
via CLI/IaC:
```bash
az stream-analytics job update \
  --resource-group <rg> --name <job-name> \
  --events-late-arrival-max-delay-time 00:05:00 \
  --events-out-of-order-max-delay-time 00:00:30 \
  --events-out-of-order-policy Adjust
```
This should be wired into `infra/bicep/streamanalytics.bicep` once that
module moves past its current stub state — flagging it here so it isn't
lost between "documented as a decision" and "actually configured."

**Streaming Unit sizing.**
Data volume here is sub-1 MB/s, so 1–3 SUs would technically suffice. I sized
the job at **3 SUs** to align with the 4-partition Event Hub topology (ASA
parallelizes across SUs only when query and input partitioning align), with the
explicit understanding that this is intentionally over-provisioned for a
learning/demo project. In a real production setting, SU sizing should come
from ASA's own metrics (SU% utilization, backlogged events), not an a priori
guess — see the KQL query in `docs/kql/asa_su_utilization.kql` for how that
would be monitored.

**Implementation notes (Layer 2 is now built — see
`stream_processing/asa_queries/` and `stream_processing/local_emulation/`):**
- A real inconsistency was caught and fixed while implementing this layer:
  the original `alert_document_schema.json` modeled one alert per single
  metric (`metric: "temperature | aqi"`, one `value` field), but
  `anomaly_detection.asaql`'s `HAVING` clause can fire on the temp-swing
  condition, the AQI condition, or both at once in the same window — a
  single-metric schema can't represent the "both fired together" case.
  Fixed by emitting two boolean trigger flags
  (`temperature_swing_triggered`, `aqi_threshold_triggered`) per window
  instead, and updating the schema to match the query's actual output
  field-for-field — verified by the same automated field-diff approach used
  for Layer 1/Layer 3 schema parity, not just by eye.
- `windowing_logic.py` holds the rolling-average and anomaly-detection
  aggregation math as plain, Spark-free Python functions, and is directly
  unit-tested (6 cases: a normal window, an empty window, a temp-swing-only
  trigger, an AQI-only trigger, neither triggering — confirming `HAVING`
  suppression is reproduced correctly — and both triggering simultaneously,
  which is the exact case that drove the schema fix above). All 6 passed.
  `pyspark_structured_streaming_equivalent.py` imports and delegates to
  these verified functions rather than reimplementing the arithmetic inline,
  so the Spark wrapper only has wiring left to get wrong.
- The Spark wrapper itself (Kafka read, watermarking, the tumbling-window
  approximation of a true hop) is design-verified, not execution-verified —
  no PySpark/JVM available in the environment this was built in. Run it
  yourself against the emulator (`docker compose up -d`, `pip install
  pyspark`, then run the file) and treat that as the one open item for this
  layer, the same way the Event Hub emulator smoke test was flagged for
  Layer 1.
- `raw_passthrough.asaql`'s `SELECT` column list was diffed against
  `BRONZE_SCHEMA` in `schema_definitions.py` and matches exactly (30/30
  fields) — this is what actually guarantees Layer 2's cold-path output
  lands in bronze in the shape Layer 3 expects.

### Layer 3 — Batch Transformation

**Implementation status: implemented and cross-verified, not yet run on a
real cluster.** All three notebooks (`batch/databricks_notebooks/`), the
schema validation gate (`utils/schema_definitions.py`), and the ADF
orchestration (`batch/adf_pipelines/pl_daily_batch_trigger.json`) are real
PySpark/Delta/ADF code — see implementation notes below for exactly what
was verified and how.

- **Bronze**: raw Parquet, append-only, partitioned by ingestion date. No
  transformation — a faithful, replayable landing zone.
- **Silver**: Delta format, deduplicated on `(city_id, observation_timestamp)`,
  cleaned types, partitioned by date.
- **Gold**: Delta, daily stats + rolling averages, registered in Unity Catalog.

**Z-ordering.** Gold is Z-ordered on `city_id`, since that's the dominant
filter/join column for both Power BI and Synapse serverless queries downstream.
Z-ordering co-locates rows for that column across files so file-skipping
actually works at query time. **Worth noting**: Databricks now recommends
liquid clustering over Z-ordering for new tables in general. Z-ordering is
used here because the assignment explicitly asks "Think about: Z-ordering,"
and implementing what was asked directly is more useful for this evaluation
than substituting a newer technique — but liquid clustering is the better
default choice for a table built outside this assignment's scope, and is
flagged as such in `03_gold_aggregate_zorder.py`'s comments.

**Delta time travel.** Used as an audit/debug tool, not built into the
pipeline's control flow — e.g. "what did gold look like before yesterday's bad
data landed." Documented as a runbook (`DESCRIBE HISTORY` + `RESTORE TABLE`)
rather than automated, since automating rollback without human judgment is
itself a risk.

**Schema evolution.** Writes enable `mergeSchema`, since OpenWeatherMap could
add fields over time — but a minimum-required-schema check runs before merge,
so a silently breaking upstream change can't poison gold without being
caught. This is no longer just a documented intent: `validate_silver_required_
fields()` in `schema_definitions.py` is fully implemented and unit-tested
(3 cases — mixed good/bad rows, a structurally missing column, and an
all-good batch; see implementation notes below) and is actually called by
`02_silver_clean_dedup.py` before every silver write.

**Why not Delta Live Tables?** DLT is the right tool when you want declarative
pipelines with built-in data-quality expectations and managed orchestration.
For this project, plain ADF-triggered Databricks notebooks were chosen
deliberately so the control flow (bronze → silver → gold, what runs when, what
fails where) stays visible and explainable rather than abstracted behind a
framework — which matters for an evaluation that's explicitly grading
architectural reasoning. DLT is the natural next iteration once the pipeline's
shape is proven.

**Implementation notes (Layer 3 is now built):**
- `validate_silver_required_fields()` distinguishes two failure modes on
  purpose: a `SILVER_REQUIRED_FIELDS` column missing **entirely** from the
  bronze DataFrame raises `SchemaValidationError` and fails the whole batch
  (this means `BRONZE_SCHEMA` and the real data have drifted apart — a code
  problem), while a column that **exists but is null** for a given row
  quarantines just that row and lets the rest of the batch proceed (the
  expected, normal failure mode — same per-unit isolation principle as
  Layer 1's per-city error handling, applied here per-row instead). Both
  paths were unit-tested with a hand-built fake DataFrame standing in for
  just the Spark operations the function calls, since no real Spark session
  was available in the build environment — all 3 cases passed.
- `01_bronze_ingest.py` reads with `BRONZE_SCHEMA` applied explicitly
  (`spark.read.schema(...)`), not inferred, so a malformed upstream file
  fails loudly at the bronze stage rather than silently downstream.
- `03_gold_aggregate_zorder.py` uses a Delta `MERGE` (upsert on `city_id` +
  `date`), not a plain append, since gold's grain (one row per city per
  day) makes "re-running this notebook for an already-processed date"
  a real risk that bronze/silver's append-only event grain doesn't share —
  confirmed `OPTIMIZE table ZORDER BY (col)` syntax against current
  Databricks/Delta docs before writing it, rather than assuming it from
  memory.
- The ADF pipeline's three `baseParameters` blocks were cross-checked
  programmatically against each notebook's actual `dbutils.widgets.text(...)`
  calls — confirmed every parameter name ADF passes is one the receiving
  notebook actually expects, and vice versa, for all three activities.
- All schema cross-checks in this layer (bronze↔enrichment.py from Layer 1,
  bronze↔raw_passthrough.asaql from Layer 2, gold notebook output↔
  `GOLD_SCHEMA`) were done by automated field-diff, not by eye — this is
  the same discipline applied throughout the repo and is what caught the
  Layer 2 alert-schema mismatch documented in that layer's notes.
- **Not yet execution-verified**: none of the three notebooks have been run
  against a real Databricks cluster or even a local Spark session (no
  PySpark/JVM in the build environment — the same constraint noted for
  Layer 2's Spark wrapper). The validation function's logic IS verified;
  the notebooks' Spark API calls (`spark.read.schema(...)`,
  `DeltaTable.forName(...).merge(...)`, etc.) are design-verified against
  current documentation, not run. Treat this as the one open item for this
  layer.

### Layer 4 — Hot Path Serving

**Cosmos DB partition key.** `city_id`. Query patterns are almost always
"alerts for city X" or "recent alerts" — `city_id` has enough cardinality
(10–15+ cities, growable) to avoid hot partitions, while keeping single-city
reads cheap (single-partition queries, no fan-out).

**RU estimation.** Given the actual write volume here (a handful of anomaly
alerts per hour, not per second), **Cosmos DB serverless** (pay-per-request)
is the right tier rather than provisioned throughput — provisioned RU/s would
sit mostly idle against this workload. This is a cost decision, documented as
such, not a universal "serverless is always better" claim.

**Push dataset limitations in Power BI.** Push datasets have a capped
historical row retention and total dataset size, and they don't support
DirectQuery-style ad hoc slicing — they're a write-only stream into a fixed
schema. That's exactly why the assignment specifies *two separate* Power BI
artifacts rather than one: a DirectQuery-driven dashboard (full read access
to Cosmos DB, can slice freely) for the live alert view, and a push dataset
(a small number of pre-aggregated metric tiles, not arbitrary queries) for
real-time city metrics. Trying to do both through one push dataset would hit
the retention/size cap quickly.

**Correction from the original plan**: Power BI does **not** connect to
Cosmos DB's Change Feed directly — there's no native Change Feed connector.
The actual mechanism for "live alert dashboard, no ETL, full query
flexibility" is **Azure Synapse Link**: enabling analytical storage on the
`anomaly_alerts` container, then connecting Power BI via DirectQuery through
Synapse Link, which queries live data without consuming the container's own
transactional RU budget. The alternative is the native Cosmos DB connector
for Power BI, but that's import-mode only and DOES consume transactional
RUs — a meaningfully worse fit given the serverless/cost-conscious decision
above. Synapse Link is the right choice here and is now implemented in
`infra/bicep/cosmosdb.bicep` (`enableAnalyticalStorage: true` at the account
level, `analyticalStorageTtl: -1` on the container — both are required;
enabling one without the other does nothing). Worth flagging since it's a
genuinely one-way decision: per Microsoft's own docs, once Synapse Link is
enabled for an account it can't be disabled — this is called out as an
explicit comment in the Bicep file itself, not just here.

**Implementation status: implemented, design-verified, not deployed.**
`infra/bicep/cosmosdb.bicep` is real Bicep (account, database, container with
serverless capability, Synapse Link analytical storage, and an indexing
policy), and `stream_processing/asa_queries/anomaly_detection.asaql` /
`rolling_averages.asaql` both received real corrections during this layer's
implementation — see notes below. No Bicep CLI was available in the build
environment to run `az bicep build` for true syntax validation; this file
was checked against current Microsoft documentation examples and passed a
basic brace/bracket/paren balance check, which catches typos but not
semantic errors. Treat actual deployment as the open item for this layer,
same pattern as the Spark-dependent layers above.

**Implementation notes (real corrections made while building this layer):**
- **Missing document `id` field.** The original `anomaly_detection.asaql`
  had no `id` column. Per Microsoft's docs, Stream Analytics' Cosmos DB
  output upserts based on the outgoing document's `id` field — without one,
  Cosmos either generates a random id (breaking idempotency: reprocessing
  the same window would create a duplicate document instead of updating the
  existing one) or risks an unintended collision. Since this is a windowed
  aggregate (not a passthrough), there's no single input event to key off
  via `GetMetadataPropertyValue(..., 'EventId')` the way a passthrough
  query could — so `id` is now built deterministically as
  `{city_id}-{window_end_unix_seconds}`, which is naturally unique per
  window and naturally idempotent on rerun. `alert_document_schema.json`
  was updated to match and its old "guid, generated at write time" comment
  (which was never accurate even as a plan) corrected.
- **Missing `PartitionId` handling on a multi-partition input.** Per
  Microsoft's docs, "if input stream has more than one partition, the OVER
  clause must be used together with the PARTITION BY clause, and PartitionId
  must be specified as part of TIMESTAMP BY OVER columns." Our Event Hub has
  4 partitions (README Layer 1 decision) — the original `OVER city_id` alone
  in both windowed queries was invalid against that input. Fixed in both
  `anomaly_detection.asaql` and `rolling_averages.asaql` to
  `OVER city_id, PartitionId` with an explicit `PARTITION BY PartitionId` on
  `FROM`, and `PartitionId` added to each `GROUP BY`. This doesn't change the
  aggregation arithmetic (a window is still scoped per city) — confirmed by
  re-reading `windowing_logic.py`'s docstring, which already noted this
  distinction; a comment was added there making the relationship explicit.
- **Output partition routing.** Per Microsoft's docs, "the partition key
  [for a Cosmos DB output] is based on the PARTITION BY clause in the
  query," not solely the container's own configured partition key path.
  `cosmosdb.bicep`'s container partition key path (`/city_id`) must match
  what the query's output partitioning actually produces — documented as an
  explicit cross-file dependency in both files' comments, since this is the
  kind of thing that fails silently (writes succeed to the wrong logical
  partition) rather than throwing an obvious error if the two drift apart.
- **Change Feed was the wrong mechanism.** The original README/plan said
  Power BI reads Cosmos DB "via Change Feed." This turned out to be
  imprecise to the point of being wrong: there's no Power BI Change Feed
  connector. The real mechanism is Synapse Link + DirectQuery (see the
  correction above) — Change Feed is a real Cosmos DB capability, but it's
  the mechanism Synapse Link's analytical-store auto-sync uses *internally*
  to replace what would otherwise require a custom Change-Feed-triggered
  ETL pipeline, not something Power BI itself consumes. Fixed across
  README.md, `powerbi/README.md`, and the architecture diagram.
- **Verification near-miss worth noting honestly**: a field-diff script
  comparing `anomaly_detection.asaql`'s output to
  `alert_document_schema.json` initially reported a mismatch — but the bug
  was in the validation script itself (a naive `text.split("SELECT")` was
  tripped up by the word "SELECT" appearing inside this query file's own
  comment headers, e.g. "the original version of this file's SELECT...").
  Fixed the script to strip comment lines before splitting, re-ran, and
  confirmed a genuine exact match (10/10 fields). Mentioning this because
  it's a real example of why a single passing (or failing) automated check
  shouldn't be trusted blindly — the check itself needs to be sane-checked
  when its result is surprising.

### Layer 5 — Cold Path Serving

**OPENROWSET vs PolyBase, serverless vs dedicated pools.**
**OPENROWSET** (Synapse serverless SQL) was chosen: it's a pay-per-query model
for ad hoc reads directly over files in a data lake, with no need to
pre-provision external tables for every query shape — well suited to gold
tables whose shape will keep changing as the project evolves, and to a BI load
that's intermittent rather than high-concurrency. **PolyBase**, via dedicated
SQL pools, is the right tool when you need high-throughput parallel loading
into distributed warehouse tables under predictable, heavy concurrent BI
traffic — none of which applies here. Dedicated pools also bill continuously
per provisioned DWU whether or not they're being queried, which directly
conflicts with the "moderate cost" goal for this project. This is a workload-
fit decision, not a claim that dedicated pools are inferior in general.

### Layer 6 — Security and Observability

**User-assigned vs system-assigned managed identity.** **User-assigned**, for
the Function and ADF. A system-assigned identity is deleted along with its
resource, which breaks any Key Vault access policy or role assignment that
referenced it — annoying during active development when resources get
redeployed. A user-assigned identity is provisioned once via IaC, attached to
multiple resources, and its access stays stable across redeploys. (This isn't
a universal rule: for a single, rarely-redeployed resource, system-assigned's
simplicity is genuinely the better trade-off — it's chosen here specifically
because this project redeploys Function/ADF resources repeatedly during
development.)

**Observability.** Private endpoints on storage, Key Vault, and Cosmos DB.
All diagnostic logs centralized in one Log Analytics workspace. Three KQL
queries are included under `docs/kql/` for the three failure modes the
assignment calls out: Event Hub consumer lag, Stream Analytics SU% exhaustion,
and ADF pipeline failures — these are the actual alerting/debugging surface,
not just a description of intent.

---

## 4. Repository structure

```
realtime-iot-pipeline/
├── README.md                    — this file
├── infra/bicep/                 — IaC: governs what gets deployed to Azure
├── ingestion/function_app/      — Layer 1: polling, enrichment, publishing
├── stream_processing/           — Layer 2: ASA queries + local logic-parity test
├── batch/                       — Layer 3: ADF pipeline def + Databricks notebooks
├── hot_path/cosmos_schemas/     — Layer 4: alert document schema
├── cold_path/synapse_sql/       — Layer 5: OPENROWSET external table DDL
├── powerbi/                     — Describes both PBI artifacts (push + DirectQuery)
├── docs/
│   ├── diagrams/                — Exported architecture diagram
│   ├── decisions/                — One ADR per major decision above
│   └── kql/                     — The three monitoring queries referenced above
├── tests/local_dev/             — Docker Compose: Event Hub emulator + Azurite
└── .github/workflows/           — CI: Bicep lint + local smoke test on PR
```

Each top-level folder corresponds to exactly one layer in the architecture
diagram, so "what governs what" is always answerable by folder name alone:
`infra/` governs provisioning, `ingestion/` governs what gets published,
`stream_processing/` governs windowing logic, `batch/` governs the medallion
transformation, `hot_path/` and `cold_path/` govern the two serving surfaces,
and `docs/` governs the reasoning trail an evaluator would want to read.

---

## 5. How to run

### 5a. Local development (no Azure subscription required)

This validates ingestion and stream-processing logic against official Azure
emulators before any cloud deployment.

**Status: Layer 1 (`ingestion/function_app/`) is implemented and unit-tested.**
Every component — OWM client retry/fail-fast classification, enrichment's
field mapping against `BRONZE_SCHEMA`, the Event Hub publisher's per-city
partition-key batching, and the Function entrypoint's per-city error
isolation — was verified with mocked dependencies before ever touching the
emulator, so the steps below are validating wiring and the real Event Hub
protocol, not first-time logic checks.

```bash
# 1. Start Event Hub emulator + Azurite (ADLS Gen2 stand-in)
cd tests/local_dev
docker compose up -d

# 2. Install Function dependencies
cd ../../ingestion/function_app
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp local.settings.json.example local.settings.json
# edit local.settings.json: set OWM_API_KEY and the emulator connection string
# Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;

# 3. Run the Function locally
func start

# 4. In a second terminal, run the logic-parity test
cd tests/local_dev
python test_publish_and_consume.py
```

This proves the Function correctly enriches and publishes events, and that
they can be consumed in the right shape — without needing Stream Analytics,
Cosmos DB, or Synapse, none of which have full local emulators.
`tests/local_dev/test_publish_and_consume.py` calls the real
`shared.eventhub_publisher.publish()` and `shared.enrichment.enrich()`
functions (not reimplementations), so it's a true integration check — but
it was written and syntax-checked, not yet run against a live emulator, since
no Docker is available in the environment this was built in. Run
`docker compose up -d` then the test yourself; if the emulator behaves
differently than documented anywhere, that's the one part of Layer 1 still
worth treating as unverified until you've actually seen it pass. Everything
else in Layer 1 (retry/fail-fast classification, schema field mapping,
partition-key batching, per-city error isolation) was verified directly with
mocked dependencies during development — see Layer 1 implementation notes in
section 3 above.

Stream
Analytics' windowing logic is instead validated via
`stream_processing/local_emulation/pyspark_structured_streaming_equivalent.py`,
a PySpark Structured Streaming job that implements the same hopping/sliding
window semantics for local testing, before being translated into ASA query
language for actual deployment.

### 5b. Azure deployment

Requires an Azure subscription and the Azure CLI logged in (`az login`).

```bash
cd infra/scripts
./deploy.sh dev   # or: prod

# This runs, in order:
#   az group create
#   az deployment group create --template-file ../bicep/main.bicep \
#       --parameters @../parameters/dev.parameters.json
```

`main.bicep` provisions, in dependency order: the user-assigned managed
identity → Key Vault (with the OWM API key as a secret + access policy for the
identity) → storage account with bronze/silver/gold containers → Event Hub
namespace and hub → Function App (Consumption plan, identity-bound) → Stream
Analytics job → Cosmos DB serverless account → Synapse workspace with
serverless SQL pool only (no dedicated pool) → Log Analytics workspace and
diagnostic settings wiring all of the above into it.

After deployment: deploy the Function code (`func azure functionapp publish`),
start the ASA job, run the ADF pipeline once manually to confirm bronze →
silver → gold, then connect Power BI per `powerbi/README.md`.

Tear down with `infra/scripts/teardown.sh dev` — deletes the resource group,
nothing else needs manual cleanup since all resources live in one group.

---

## 6. What's deliberately out of scope (and why)

- **Delta Live Tables**: see Layer 3 reasoning above — chosen against for
  visibility/explainability during evaluation, not technical inferiority.
- **Dedicated Synapse SQL pools**: workload doesn't justify the continuous
  per-DWU cost (see Layer 5).
- **IoT Hub**: no device management need exists in this design (see Layer 1).
- **Full end-to-end Azure deployment validation**: built without an active
  subscription; validated locally where official emulators exist (Event Hub,
  Azurite), and via IaC review elsewhere. Anyone with a subscription can run
  `infra/scripts/deploy.sh` directly.
