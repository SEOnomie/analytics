defmodule Plausible.DataMigration.ReplicateTableTest do
  use ExUnit.Case, async: false

  alias Plausible.DataMigration.ReplicateTable
  alias Plausible.IngestRepo

  describe "replicated_engine_full/3" do
    test "keeps engine parameters and remaining clauses" do
      assert ReplicateTable.replicated_engine_full(
               "sessions_v2",
               "VersionedCollapsingMergeTree(sign, events) PARTITION BY toYYYYMM(start) ORDER BY (site_id, session_id) SETTINGS index_granularity = 8192",
               "20261111120000"
             ) ==
               {:ok,
                "ReplicatedVersionedCollapsingMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/sessions_v2_20261111120000', '{replica}', sign, events) PARTITION BY toYYYYMM(start) ORDER BY (site_id, session_id) SETTINGS index_granularity = 8192"}
    end

    test "handles engines without parameters" do
      assert ReplicateTable.replicated_engine_full("t", "MergeTree ORDER BY id", "1") ==
               {:ok,
                "ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/t_1', '{replica}') ORDER BY id"}
    end

    test "rejects already replicated and unsupported engines" do
      assert ReplicateTable.replicated_engine_full(
               "t",
               "ReplicatedMergeTree('/p', '{replica}') ORDER BY id"
             ) == {:error, :already_replicated}

      assert ReplicateTable.replicated_engine_full("t", "Memory") ==
               {:error, {:unsupported_engine, "Memory"}}
    end
  end

  describe "unreplicated_engine_full/1" do
    test "drops path and replica parameters" do
      assert ReplicateTable.unreplicated_engine_full(
               "ReplicatedVersionedCollapsingMergeTree('/clickhouse/c/tables/01/db/t', '{replica}', sign, events) ORDER BY id"
             ) == {:ok, "VersionedCollapsingMergeTree(sign, events) ORDER BY id"}

      assert ReplicateTable.unreplicated_engine_full(
               "ReplicatedMergeTree('/p', '{replica}') ORDER BY id"
             ) == {:ok, "MergeTree() ORDER BY id"}
    end

    test "rejects plain engines" do
      assert ReplicateTable.unreplicated_engine_full("MergeTree ORDER BY id") ==
               {:error, :not_replicated}
    end
  end

  test "placeholder_statement/1 adds IF NOT EXISTS and ON CLUSTER" do
    assert ReplicateTable.placeholder_statement(
             "CREATE TABLE db.t (`id` UInt64) ENGINE = MergeTree ORDER BY id"
           ) ==
             "CREATE TABLE IF NOT EXISTS db.t ON CLUSTER '{cluster}' (`id` UInt64) ENGINE = MergeTree ORDER BY id"
  end

  test "ddl_statements/5 exchanges before the old table is renamed" do
    statements =
      ReplicateTable.ddl_statements(
        "db",
        "t",
        "CREATE TABLE db.t (`id` UInt64) ENGINE = MergeTree ORDER BY id",
        "ReplicatedMergeTree('/p', '{replica}') ORDER BY id",
        "t_backup"
      )

    assert [
             "CREATE DATABASE IF NOT EXISTS `db` ON CLUSTER '{cluster}'",
             "CREATE TABLE IF NOT EXISTS db.t ON CLUSTER" <> _,
             "CREATE TABLE `db`.`t_swap_tmp` ON CLUSTER '{cluster}' AS `db`.`t` ENGINE = ReplicatedMergeTree" <>
               _,
             "EXCHANGE TABLES `db`.`t_swap_tmp` AND `db`.`t` ON CLUSTER '{cluster}'",
             "RENAME TABLE `db`.`t_swap_tmp` TO `db`.`t_backup` ON CLUSTER '{cluster}'"
           ] = statements
  end

  test "drop_leftover/2 only accepts backup tables" do
    assert ReplicateTable.drop_leftover("events_v2") == {:error, :not_a_leftover_table}
  end

  describe "against a replicated cluster" do
    # Needs ClickHouse with two replicas, Keeper and macros {cluster}, {shard}, {replica}.
    @describetag :clickhouse_cluster

    setup do
      original = Application.get_env(:plausible, IngestRepo)

      Application.put_env(
        :plausible,
        IngestRepo,
        Keyword.put(original, :cluster_name, "test_cluster")
      )

      table = "rt_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        Application.put_env(:plausible, IngestRepo, original)

        for t <- [table, "#{table}_backup", "#{table}_replicated_backup", "#{table}_swap_tmp"] do
          IngestRepo.query!("DROP TABLE IF EXISTS #{t} ON CLUSTER '{cluster}' SYNC")
        end
      end)

      {:ok, table: table}
    end

    test "replicates partitioned VersionedCollapsingMergeTree incl. rows written afterwards",
         %{table: table} do
      IngestRepo.query!("""
      CREATE TABLE #{table} (site_id UInt64, start DateTime, sign Int8, events Int32)
      ENGINE = VersionedCollapsingMergeTree(sign, events)
      PARTITION BY toYYYYMM(start) ORDER BY (site_id, start)
      """)

      IngestRepo.query!("""
      INSERT INTO #{table}
      SELECT number % 10, toDateTime('2026-01-01') + number * 3600, 1, 1 FROM numbers(3000)
      """)

      assert {:ok, %{partitions: partitions}} = ReplicateTable.run(table)
      assert partitions > 1

      IngestRepo.query!("INSERT INTO #{table} VALUES (99, now(), 1, 1)")

      assert engine(table) == "ReplicatedVersionedCollapsingMergeTree"
      assert engine("#{table}_backup") == "VersionedCollapsingMergeTree"
      assert rows_per_replica(table) == [3001, 3001]
    end

    test "collapses a session whose rows are written before and after the exchange",
         %{table: table} do
      IngestRepo.query!("""
      CREATE TABLE #{table} (session_id UInt64, start DateTime, sign Int8, events Int32)
      ENGINE = VersionedCollapsingMergeTree(sign, events)
      PARTITION BY toYYYYMM(start) ORDER BY session_id
      """)

      IngestRepo.query!("INSERT INTO #{table} VALUES (1, '2026-01-01 10:00:00', 1, 1)")

      assert {:ok, _} = ReplicateTable.run(table)

      # session update arriving after the exchange: cancel old state, write new one
      IngestRepo.query!("""
      INSERT INTO #{table} VALUES
        (1, '2026-01-01 10:00:00', -1, 1),
        (1, '2026-01-01 10:00:00', 1, 2)
      """)

      %{rows: [[sessions, events]]} =
        IngestRepo.query!("SELECT sum(sign), sum(sign * events) FROM #{table}")

      assert {sessions, events} == {1, 2}

      IngestRepo.query!("OPTIMIZE TABLE #{table} FINAL")
      assert count(table) == 1
    end

    test "replicates unpartitioned SummingMergeTree", %{table: table} do
      IngestRepo.query!("""
      CREATE TABLE #{table} (k String, v UInt64) ENGINE = SummingMergeTree(v) ORDER BY k
      """)

      IngestRepo.query!("INSERT INTO #{table} VALUES ('a', 1), ('b', 2)")

      assert {:ok, %{partitions: 1}} = ReplicateTable.run(table)
      assert engine(table) == "ReplicatedSummingMergeTree"
      assert rows_per_replica(table) == [2, 2]
    end

    test "dry run changes nothing", %{table: table} do
      IngestRepo.query!("CREATE TABLE #{table} (id UInt64) ENGINE = MergeTree ORDER BY id")
      IngestRepo.query!("INSERT INTO #{table} VALUES (1)")

      assert {:ok, statements} = ReplicateTable.run(table, dry_run: true)
      assert Enum.any?(statements, &String.starts_with?(&1, "EXCHANGE TABLES"))
      assert engine(table) == "MergeTree"
    end

    test "refuses already replicated tables and existing backups", %{table: table} do
      IngestRepo.query!("CREATE TABLE #{table} (id UInt64) ENGINE = MergeTree ORDER BY id")
      assert {:ok, _} = ReplicateTable.run(table)
      assert ReplicateTable.run(table) == {:error, :already_replicated}
    end

    test "rollback keeps rows written after the conversion", %{table: table} do
      IngestRepo.query!("""
      CREATE TABLE #{table} (id UInt64, d Date) ENGINE = MergeTree PARTITION BY d ORDER BY id
      """)

      IngestRepo.query!("INSERT INTO #{table} VALUES (1, '2026-01-01'), (2, '2026-01-02')")

      assert {:ok, _} = ReplicateTable.run(table)
      IngestRepo.query!("INSERT INTO #{table} VALUES (3, '2026-01-03')")

      assert {:ok, _} = ReplicateTable.rollback(table)
      assert engine(table) == "MergeTree"
      assert engine("#{table}_replicated_backup") == "ReplicatedMergeTree"
      assert count(table) == 3

      assert {:ok, _} = ReplicateTable.drop_leftover("#{table}_replicated_backup")
      assert {:ok, _} = ReplicateTable.drop_leftover("#{table}_backup")
    end

    test "refuses to run when another replica holds the data", %{table: table} do
      IngestRepo.query!(
        "CREATE TABLE #{table} ON CLUSTER '{cluster}' (id UInt64) ENGINE = MergeTree ORDER BY id"
      )

      IngestRepo.query!(
        "INSERT INTO FUNCTION remote('ch_r2', currentDatabase(), '#{table}') VALUES (1)"
      )

      assert {:error, {:data_on_other_replica, %{local_rows: 0, remote_rows: 1}}} =
               ReplicateTable.run(table)
    end
  end

  defp engine(table) do
    %{rows: [[engine]]} =
      IngestRepo.query!(
        "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = {$0:String}",
        [table]
      )

    engine
  end

  defp count(table) do
    %{rows: [[n]]} = IngestRepo.query!("SELECT count() FROM #{table}")
    n
  end

  defp rows_per_replica(table) do
    %{rows: [[db]]} = IngestRepo.query!("SELECT currentDatabase()")
    # SYSTEM ... ON CLUSTER does not inherit the current database on remote hosts
    IngestRepo.query!("SYSTEM SYNC REPLICA ON CLUSTER '{cluster}' #{db}.#{table}")

    %{rows: rows} =
      IngestRepo.query!("""
      SELECT hostName(), count()
      FROM clusterAllReplicas(getMacro('cluster'), currentDatabase(), #{table})
      GROUP BY hostName()
      ORDER BY hostName()
      """)

    Enum.map(rows, fn [_, n] -> n end)
  end
end
