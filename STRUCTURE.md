# Repository structure (planning reference)

realtime-iot-pipeline/
├── README.md                          # Main deliverable: architecture, decisions, run instructions
├── STRUCTURE.md                       # This file — internal planning reference, not graded
├── .env.example                       # All required env vars / connection strings, no secrets
├── .gitignore
│
├── infra/                             # Infrastructure as Code — governs WHAT gets deployed
│   ├── bicep/
│   │   ├── main.bicep                 # Orchestrates all modules, parameterized per environment
│   │   ├── eventhub.bicep             # Namespace, hub, partitions, consumer groups
│   │   ├── storage.bicep              # ADLS Gen2 account, bronze/silver/gold containers
│   │   ├── keyvault.bicep             # Vault + access policies for the UAMI
│   │   ├── function.bicep             # Function App, Consumption plan, app settings
│   │   ├── streamanalytics.bicep      # ASA job shell (input/output bindings, SU count)
│   │   ├── cosmosdb.bicep             # Serverless account, container, partition key
│   │   ├── synapse.bicep              # Synapse workspace, serverless SQL pool (no dedicated pool)
│   │   ├── loganalytics.bicep         # Workspace + diagnostic settings wiring
│   │   └── identity.bicep             # User-assigned managed identity + role assignments
│   ├── scripts/
│   │   ├── deploy.sh                  # az deployment group create wrapper
│   │   └── teardown.sh                # Safe resource group deletion
│   └── parameters/
│       ├── dev.parameters.json
│       └── prod.parameters.json
│
├── ingestion/                         # LAYER 1 — governs what gets published to Event Hub
│   └── function_app/
│       ├── HttpTriggerCityPoll/       # (will rename to TimerTriggerCityPoll)
│       │   ├── __init__.py            # Function entrypoint: poll → enrich → publish
│       │   └── function.json          # Timer trigger binding (CRON schedule)
│       ├── shared/
│       │   ├── owm_client.py          # OpenWeatherMap API wrapper, retry logic
│       │   ├── enrichment.py          # Adds metadata: poll_timestamp, source, schema_version
│       │   └── eventhub_publisher.py  # Batches + sends to Event Hub
│       ├── cities.json                # List of cities polled (the "virtual sensors")
│       ├── host.json
│       ├── requirements.txt
│       └── local.settings.json.example
│
├── stream_processing/                 # LAYER 2 — governs windowing logic, never raw code execution
│   ├── asa_queries/
│   │   ├── rolling_averages.asaql     # Hopping window query
│   │   ├── anomaly_detection.asaql    # Sliding window query
│   │   └── raw_passthrough.asaql      # Unwindowed passthrough to ADLS
│   └── local_emulation/
│       └── pyspark_structured_streaming_equivalent.py  # Logic parity test, runs locally against Event Hub emulator
│
├── batch/                             # LAYER 3 — governs daily bronze→silver→gold transformation
│   ├── databricks_notebooks/
│   │   ├── 01_bronze_ingest.py
│   │   ├── 02_silver_clean_dedup.py
│   │   ├── 03_gold_aggregate_zorder.py
│   │   └── utils/schema_definitions.py
│   └── adf_pipelines/
│       └── pl_daily_batch_trigger.json  # ADF pipeline definition (trigger + Databricks activity)
│
├── hot_path/                          # LAYER 4 — governs anomaly alert storage + serving contract
│   └── cosmos_schemas/
│       └── alert_document_schema.json
│
├── cold_path/                         # LAYER 5 — governs historical query surface
│   └── synapse_sql/
│       └── create_external_tables_openrowset.sql
│
├── powerbi/
│   └── README.md                      # Describes the 2 PBI artifacts: push dataset + DirectQuery report
│
├── docs/
│   ├── diagrams/architecture.png       # Exported from this conversation's diagram
│   ├── openweathermap_api_reference.md # Full API docs: free endpoints used, params,
│   │                                    # responses, errors, rate budget, vs One Call 4.0
│   ├── decisions/ADR-*.md              # One-pager per "Think about" question, decision-log style
│   └── kql/
│       ├── eventhub_consumer_lag.kql
│       ├── asa_su_utilization.kql
│       └── adf_pipeline_failures.kql
│
├── tests/
│   └── local_dev/
│       ├── docker-compose.yml          # Event Hub emulator + Azurite, single command local stack
│       ├── config.json                 # Event Hub emulator entity config
│       └── test_publish_and_consume.py # Smoke test: Function logic → emulator → read back
│
└── .github/workflows/
    └── validate.yml                    # Lint Bicep, run local smoke test on PR
