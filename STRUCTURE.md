# Repository structure (planning reference)

realtime-iot-pipeline/
├── README.md                          # Main deliverable: architecture, decisions, run instructions
├── STRUCTURE.md                       # This file — internal planning reference, not graded
├── .env.example                       # All required env vars / connection strings, no secrets
├── .gitignore
│
├── infra/                             # Infrastructure as Code — governs WHAT gets deployed
│   ├── bicep/
│   │   ├── main.bicep                 # [STUB] Orchestrates all modules, parameterized per environment
│   │   ├── eventhub.bicep             # [STUB] Namespace, hub, partitions, consumer groups
│   │   ├── storage.bicep              # [STUB] ADLS Gen2 account, bronze/silver/gold containers
│   │   ├── keyvault.bicep             # [IMPLEMENTED] Vault + RBAC role assignment for the UAMI --
│   │   │                                # NOT access policies (this comment was stale; see README Layer 6,
│   │   │                                # RBAC is now the actual API-version default as of 2026-02-01)
│   │   ├── function.bicep             # [STUB] Function App, Consumption plan, app settings
│   │   ├── streamanalytics.bicep      # [STUB] ASA job shell (input/output bindings, SU count)
│   │   ├── cosmosdb.bicep             # [IMPLEMENTED] Serverless account, container, partition key,
│   │   │                                # Synapse Link analytical storage
│   │   ├── synapse.bicep              # [IMPLEMENTED] Synapse workspace, serverless SQL only,
│   │   │                                # Entra-only auth (no dedicated pool, no SQL admin password)
│   │   ├── loganalytics.bicep         # [IMPLEMENTED] Workspace + a documented diagnostic-settings
│   │   │                                # wiring template for other modules to adopt once they exist
│   │   └── identity.bicep             # [IMPLEMENTED] User-assigned managed identity, shared across
│   │                                    # every compute resource in this repo
│   ├── scripts/
│   │   ├── deploy.sh                  # [STUB] az deployment group create wrapper
│   │   └── teardown.sh                # [STUB] Safe resource group deletion
│   └── parameters/
│       ├── dev.parameters.json
│       └── prod.parameters.json
│
├── ingestion/                         # LAYER 1 — governs what gets published to Event Hub
│   └── function_app/
│       ├── TimerTriggerCityPoll/      # implemented — timer trigger, see README section 3
│       │   ├── __init__.py            # Function entrypoint: poll → enrich → publish
│       │   └── function.json          # Timer trigger binding (CRON schedule)
│       ├── shared/
│       │   ├── owm_client.py          # OpenWeatherMap API wrapper, retry logic
│       │   ├── enrichment.py          # Adds metadata: poll_timestamp, source, schema_version
│       │   ├── eventhub_publisher.py  # Batches + sends to Event Hub
│       │   └── key_vault.py           # API key resolution: env var locally, Key Vault via managed identity in Azure
│       │                              # (added during Layer 1 implementation — not in original plan)
│       ├── cities.json                # List of cities polled (the "virtual sensors")
│       ├── host.json
│       ├── requirements.txt
│       └── local.settings.json.example
│
├── stream_processing/                 # LAYER 2 — governs windowing logic, never raw code execution [IMPLEMENTED]
│   ├── asa_queries/
│   │   ├── rolling_averages.asaql     # Hopping window query — real SAQL, OVER city_id substreams
│   │   ├── anomaly_detection.asaql    # Sliding window query — real SAQL, dual trigger flags
│   │   └── raw_passthrough.asaql      # Unwindowed passthrough to ADLS — field-diff verified vs BRONZE_SCHEMA
│   └── local_emulation/
│       ├── windowing_logic.py         # Spark-free aggregation math, 6/6 unit tests passing
│       │                              # (added during implementation — not in original plan)
│       └── pyspark_structured_streaming_equivalent.py  # Logic parity test, runs locally against Event Hub emulator
│                                                          # delegates math to windowing_logic.py
│
├── batch/                             # LAYER 3 — governs daily bronze→silver→gold transformation [IMPLEMENTED]
│   ├── databricks_notebooks/
│   │   ├── 01_bronze_ingest.py        # explicit BRONZE_SCHEMA on read, append+partition by ingestion_date
│   │   ├── 02_silver_clean_dedup.py   # validation gate -> quarantine, dt->timestamp, dedup, mergeSchema write
│   │   ├── 03_gold_aggregate_zorder.py# daily agg + 7-day rolling avg, Delta MERGE upsert, OPTIMIZE ZORDER BY
│   │   └── utils/schema_definitions.py# validate_silver_required_fields() implemented, 3/3 unit tests passing
│   └── adf_pipelines/
│       └── pl_daily_batch_trigger.json  # 3 chained DatabricksNotebook activities, dependsOn Succeeded,
│                                          # baseParameters cross-checked against each notebook's widgets
│
├── hot_path/                          # LAYER 4 — governs anomaly alert storage + serving contract [IMPLEMENTED]
│   └── cosmos_schemas/
│       └── alert_document_schema.json # matches anomaly_detection.asaql exactly, incl. deterministic id field
│                                        # (real Cosmos resources are in infra/bicep/cosmosdb.bicep, not here --
│                                        #  this folder holds the document CONTRACT, not the provisioning)
│
├── cold_path/                         # LAYER 5 — governs historical query surface [IMPLEMENTED]
│   └── synapse_sql/
│       └── create_external_tables_openrowset.sql  # gold = CREATE EXTERNAL TABLE (unpartitioned, safe);
│                                                      # includes documented VIEW-pattern template for any
│                                                      # future partitioned source (bronze/silver, if ever exposed)
│
├── powerbi/
│   └── README.md                      # Describes the 2 PBI artifacts: push dataset + DirectQuery report
│
├── docs/
│   ├── diagrams/architecture.png       # Exported from this conversation's diagram
│   ├── openweathermap_api_reference.md # Full API docs: free endpoints used, params,
│   │                                    # responses, errors, rate budget, vs One Call 4.0
│   ├── decisions/ADR-*.md              # One-pager per "Think about" question, decision-log style
│   └── kql/                            # [IMPLEMENTED] all 3 had real corrections -- see README Layer 6
│       ├── eventhub_consumer_lag.kql   # uses EH's own ConsumerLag activity, not a manual self-join
│       ├── asa_su_utilization.kql      # ResourceUtilization (correct name) + BacklogedInputEvents,
│       │                                # not the nonexistent "SUUtilization" + SU%-alone
│       └── adf_pipeline_failures.kql   # joins ADFActivityRun for error detail -- ADFPipelineRun's
│                                          # own error columns are documented to always be empty
│
├── tests/
│   └── local_dev/
│       ├── docker-compose.yml          # Event Hub emulator + Azurite, single command local stack
│       ├── config.json                 # Event Hub emulator entity config
│       └── test_publish_and_consume.py # Smoke test: Function logic → emulator → read back
│
└── .github/workflows/
    └── validate.yml                    # Lint Bicep, run local smoke test on PR
