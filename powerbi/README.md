# Power BI artifacts (planned)

Two separate artifacts per the assignment and README section 3
("Push dataset limitations in Power BI"):

1. **Live alert dashboard** — reads Cosmos DB via Change Feed. Full read
   access, can slice/filter freely. Built on top of `hot_path/cosmos_schemas/alert_document_schema.json`.
2. **Real-time city metrics report** — push streaming dataset. Fixed schema,
   small number of pre-aggregated tiles only (NOT ad hoc queryable) — this is
   a Power BI platform limitation, not a choice; design the tile set
   accordingly (e.g. current temp/AQI per city, rolling average sparkline).
3. **Historical trend report** — DirectQuery against Synapse serverless SQL
   external tables (`cold_path/synapse_sql/create_external_tables_openrowset.sql`).

TODO: screenshots / .pbix files once Layer 4 and 5 are deployed.
