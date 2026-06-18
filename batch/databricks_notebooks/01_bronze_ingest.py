# PLANNED — Layer 3, bronze ingest (see README section 3).
# Reads raw events landed by the ASA raw-passthrough query (ADLS Gen2),
# writes append-only Parquet, partitioned by ingestion date. No transformation.
#
# TODO: spark.read on the raw cold-path location -> df.write.partitionBy("ingestion_date").parquet(bronze_path)