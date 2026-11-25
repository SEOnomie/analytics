-- Run against a replica after `mix ecto.migrate` with CLICKHOUSE_CLUSTER_NAME set.
-- Returns one row per problem, no rows means OK.
-- Pass the database as query parameter `db` (see Makefile).
--
-- Excluded: `events` and `sessions` are the empty V1 tables. Community Edition
-- keeps them on purpose (source for the NumericIDs data migration), they are
-- created and altered locally on the migrating node and never queried.
SELECT 'not replicated' AS problem, name, engine
FROM system.tables
WHERE database = {db:String}
  AND engine LIKE '%MergeTree'
  AND engine NOT LIKE 'Replicated%'
  AND name NOT IN ('events', 'sessions')
UNION ALL
SELECT 'differs between replicas' AS problem, name, toString(count())
FROM clusterAllReplicas(getMacro('cluster'), system.tables)
WHERE database = {db:String}
  AND engine != 'TinyLog'
  AND name NOT IN ('events', 'sessions')
GROUP BY name
HAVING uniqExact(create_table_query) != 1
    OR count() != (SELECT count() FROM system.clusters WHERE cluster = getMacro('cluster'))
FORMAT TSV
