# PLANNED — Layer 3, gold aggregate + Z-order (see README section 3).
# Aggregates daily stats + rolling averages from silver, writes Delta,
# then runs OPTIMIZE ... ZORDER BY (city_id), registers table in Unity Catalog.
#
# TODO: groupBy + agg -> write.format("delta") -> spark.sql("OPTIMIZE gold_table ZORDER BY (city_id)")
