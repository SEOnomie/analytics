-- Run against a replica after `mix ecto.migrate` with CLICKHOUSE_CLUSTER_NAME set.
-- Returns one row per problem, no rows means OK.
-- Pass the database as query parameter `db` (see Makefile).
SELECT 'not replicated' AS problem, name, engine
FROM system.tables
WHERE database = {db:String}
  AND engine LIKE '%MergeTree'
  AND engine NOT LIKE 'Replicated%'
UNION ALL
SELECT 'differs between replicas' AS problem, name, toString(count())
FROM clusterAllReplicas(getMacro('cluster'), system.tables)
WHERE database = {db:String} AND engine != 'TinyLog'
GROUP BY name
HAVING uniqExact(create_table_query) != 1
    OR count() != (SELECT count() FROM system.clusters WHERE cluster = getMacro('cluster'))
FORMAT TSV
