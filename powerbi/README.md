# Power BI artifacts

Two separate artifacts per the assignment and README section 3
("Push dataset limitations in Power BI"):

1. **Live alert dashboard** — DirectQuery via **Azure Synapse Link**, NOT
   Cosmos DB's Change Feed directly (there's no native Power BI Change Feed
   connector — this was corrected during Layer 4 implementation; see
   README.md Layer 4 implementation notes for the full explanation). Full
   read/query access against the `anomaly_alerts` container's analytical
   store, no impact on the container's transactional RU budget, no ETL
   pipeline. Built on top of `hot_path/cosmos_schemas/alert_document_schema.json`.
   Prerequisite (`enableAnalyticalStorage` at the account level +
   `analyticalStorageTtl` on the container) is implemented in
   `infra/bicep/cosmosdb.bicep` — note this is a one-way decision per
   Microsoft's docs (Synapse Link can't be disabled once enabled for an
   account), flagged as a comment in that file.
2. **Real-time city metrics report** — push streaming dataset, fed by
   `stream_processing/asa_queries/rolling_averages.asaql`'s `RollingAveragesOutput`.
   Fixed schema, small number of pre-aggregated tiles only (NOT ad hoc
   queryable) — this is a Power BI platform limitation, not a choice;
   design the tile set accordingly (e.g. current temp/AQI per city, rolling
   average sparkline).
3. **Historical trend report** — DirectQuery against Synapse serverless SQL
   external tables (`cold_path/synapse_sql/create_external_tables_openrowset.sql`).

TODO: screenshots / .pbix files once Layer 4 and 5 are deployed.
