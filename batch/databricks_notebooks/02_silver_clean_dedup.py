# PLANNED — Layer 3, silver clean + dedup (see README section 3).
# Reads bronze, deduplicates on (city_id, observation_timestamp), cleans
# types, writes Delta format, partitioned by date. mergeSchema enabled with
# a minimum-required-schema check run first (see README "Schema evolution").
#
# TODO: dropDuplicates(["city_id", "observation_timestamp"]) -> cast types -> write.format("delta")
