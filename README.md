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
 │   │ Change Feed            │        └────────────────┬───────────────┘
 │   ▼                        │                          ▼
 │ Power BI live dashboard    │        ┌──────────────────────────────────┐
 │ (Change Feed + push API)   │        │ LAYER 5 — Cold Path Serving       │
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

### Layer 2 — Stream Processing

**Tumbling vs hopping vs sliding windows.**
Three outputs need three different window semantics in the same ASA job:
- *Rolling averages per city* → **hopping window** (e.g. 10-minute window,
  hopping every 2 minutes). A tumbling window only emits one non-overlapping
  average per fixed interval, which is too coarse for a metric that should
  read as "rolling." Hopping windows overlap, producing smoother and more
  frequent updates while staying deterministic and boundable.
- *Anomaly detection* → **sliding window**, evaluated per incoming event
  (e.g. "did temperature move more than X in the last 15 minutes"). Anomalies
  need to be flagged the instant a qualifying event arrives, not only at fixed
  window boundaries — sliding windows are the only one of the three that
  triggers on every new event rather than on a clock.
- *Raw passthrough to cold storage* → no windowing. Straight pass-through,
  output partitioned by date so downstream ADF/Databricks reads are efficient.

**Late event handling.**
The API is polled on a schedule, so events should usually arrive close to
in-order, but Function retries or transient network delay can still produce
late arrivals. Late arrival tolerance is set to ~5 minutes and out-of-order
tolerance to ~30 seconds, both using the **adjust** policy rather than **drop**.
Silently dropping late events would quietly corrupt rolling averages with no
visible failure — a real production bug class, and one worth catching here
rather than discovering downstream.

**Streaming Unit sizing.**
Data volume here is sub-1 MB/s, so 1–3 SUs would technically suffice. I sized
the job at **3 SUs** to align with the 4-partition Event Hub topology (ASA
parallelizes across SUs only when query and input partitioning align), with the
explicit understanding that this is intentionally over-provisioned for a
learning/demo project. In a real production setting, SU sizing should come
from ASA's own metrics (SU% utilization, backlogged events), not an a priori
guess — see the KQL query in `docs/kql/asa_su_utilization.kql` for how that
would be monitored.

### Layer 3 — Batch Transformation

- **Bronze**: raw Parquet, append-only, partitioned by ingestion date. No
  transformation — a faithful, replayable landing zone.
- **Silver**: Delta format, deduplicated on `(city_id, observation_timestamp)`,
  cleaned types, partitioned by date.
- **Gold**: Delta, daily stats + rolling averages, registered in Unity Catalog.

**Z-ordering.** Gold is Z-ordered on `city_id`, since that's the dominant
filter/join column for both Power BI and Synapse serverless queries downstream.
Z-ordering co-locates rows for that column across files so file-skipping
actually works at query time.

**Delta time travel.** Used as an audit/debug tool, not built into the
pipeline's control flow — e.g. "what did gold look like before yesterday's bad
data landed." Documented as a runbook (`DESCRIBE HISTORY` + `RESTORE TABLE`)
rather than automated, since automating rollback without human judgment is
itself a risk.

**Schema evolution.** Writes enable `mergeSchema`, since OpenWeatherMap could
add fields over time — but a minimum-required-schema check runs before merge,
so a silently breaking upstream change can't poison gold without being caught.

**Why not Delta Live Tables?** DLT is the right tool when you want declarative
pipelines with built-in data-quality expectations and managed orchestration.
For this project, plain ADF-triggered Databricks notebooks were chosen
deliberately so the control flow (bronze → silver → gold, what runs when, what
fails where) stays visible and explainable rather than abstracted behind a
framework — which matters for an evaluation that's explicitly grading
architectural reasoning. DLT is the natural next iteration once the pipeline's
shape is proven.

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
artifacts rather than one: a Change Feed–driven dashboard (full read access to
Cosmos DB, can slice freely) for the live alert view, and a push dataset (a
small number of pre-aggregated metric tiles, not arbitrary queries) for
real-time city metrics. Trying to do both through one push dataset would hit
the retention/size cap quickly.

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
Cosmos DB, or Synapse, none of which have full local emulators. Stream
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