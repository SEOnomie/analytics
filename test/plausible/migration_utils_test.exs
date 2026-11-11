defmodule Plausible.MigrationUtilsTest do
  use ExUnit.Case, async: false

  alias Plausible.MigrationUtils

  setup do
    original = Application.get_env(:plausible, Plausible.IngestRepo)
    on_exit(fn -> Application.put_env(:plausible, Plausible.IngestRepo, original) end)
    :ok
  end

  defp put_cluster_name(value) do
    config =
      :plausible
      |> Application.get_env(Plausible.IngestRepo)
      |> Keyword.put(:cluster_name, value)

    Application.put_env(:plausible, Plausible.IngestRepo, config)
  end

  describe "cluster_enabled?/0" do
    test "is false when not configured" do
      put_cluster_name(nil)
      refute MigrationUtils.cluster_enabled?()
    end

    test "is false for blank values" do
      put_cluster_name("")
      refute MigrationUtils.cluster_enabled?()

      put_cluster_name("   ")
      refute MigrationUtils.cluster_enabled?()
    end

    test "is true when a cluster name is configured" do
      put_cluster_name("test_cluster")
      assert MigrationUtils.cluster_enabled?()
    end
  end

  describe "on_cluster_statement/1" do
    test "uses the cluster macro when clustering is enabled" do
      put_cluster_name("test_cluster")

      assert MigrationUtils.on_cluster_statement("table_that_does_not_exist") ==
               "ON CLUSTER '{cluster}'"
    end

    test "never interpolates the configured cluster name" do
      put_cluster_name("some_other_name")
      refute MigrationUtils.on_cluster_statement("events_v2") =~ "some_other_name"
    end

    test "is empty for non-replicated tables when clustering is disabled" do
      put_cluster_name(nil)
      assert MigrationUtils.on_cluster_statement("events_v2") == ""
    end
  end

  describe "replicated_engine/3" do
    test "returns the plain engine when clustering is disabled" do
      put_cluster_name(nil)
      assert MigrationUtils.replicated_engine("MergeTree", "ingest_counters") == "MergeTree()"

      assert MigrationUtils.replicated_engine("VersionedCollapsingMergeTree", "sessions_v2",
               args: ["sign", "events"]
             ) == "VersionedCollapsingMergeTree(sign, events)"
    end

    test "returns the replicated engine when clustering is enabled" do
      put_cluster_name("test_cluster")

      assert MigrationUtils.replicated_engine("MergeTree", "ingest_counters") ==
               "ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/ingest_counters', '{replica}')"

      assert MigrationUtils.replicated_engine("VersionedCollapsingMergeTree", "sessions_v2",
               args: ["sign", "events"]
             ) ==
               "ReplicatedVersionedCollapsingMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/sessions_v2', '{replica}', sign, events)"
    end

    test "cluster? option overrides the config switch" do
      put_cluster_name(nil)

      assert MigrationUtils.replicated_engine("SummingMergeTree", "imported_pages",
               cluster?: true
             ) ==
               "ReplicatedSummingMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/imported_pages', '{replica}')"

      put_cluster_name("test_cluster")

      assert MigrationUtils.replicated_engine("MergeTree", "imported_pages", cluster?: false) ==
               "MergeTree()"
    end
  end
end
