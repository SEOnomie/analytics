defmodule Plausible.MigrationUtils do
  @moduledoc """
  Base module for to use in Clickhouse migrations
  """

  use Plausible

  alias Plausible.IngestRepo

  @replication_path "/clickhouse/{cluster}/tables/{shard}/{database}"

  @doc """
  Whether replicated ClickHouse setup was explicitly requested via
  `CLICKHOUSE_CLUSTER_NAME`.

  The configured value is only used as a switch. Generated SQL relies on the
  `{cluster}`, `{shard}` and `{replica}` macros defined on the ClickHouse
  servers, so the cluster name itself never ends up in queries.
  """
  def cluster_enabled?() do
    case IngestRepo.config()[:cluster_name] do
      name when is_binary(name) -> String.trim(name) != ""
      _ -> false
    end
  end

  @doc """
  Whether DDL for `table` should run `ON CLUSTER`: clustering is enabled via
  config or `table` is already replicated.
  """
  def cluster?(table) do
    cluster_enabled?() or IngestRepo.clustered_table?(table)
  end

  @doc """
  Returns `ON CLUSTER '{cluster}'` when clustering is enabled via config or
  when `table` is already replicated, otherwise an empty string.

  The replication check is kept as a safety net for installations that are
  replicated but lack the config switch.
  """
  def on_cluster_statement(table) do
    if cluster?(table) do
      "ON CLUSTER '{cluster}'"
    else
      ""
    end
  end

  @doc """
  Builds the engine expression for `table`.

  With clustering enabled, returns the `Replicated*` variant of `engine` using
  the replication path `#{@replication_path}/<table>` and the `{replica}`
  macro, followed by `args`. Otherwise returns the plain engine.

  Options:

    * `:args` - engine parameters, e.g. `["sign", "events"]` (default: `[]`)
    * `:cluster?` - overrides the `cluster_enabled?/0` decision

  ## Examples

      replicated_engine("MergeTree", "ingest_counters")
      #=> "MergeTree()" or
      #=> "ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/ingest_counters', '{replica}')"

      replicated_engine("VersionedCollapsingMergeTree", "sessions_v2", args: ["sign", "events"])
  """
  def replicated_engine(engine, table, opts \\ []) when is_binary(engine) and is_binary(table) do
    args = Keyword.get(opts, :args, [])

    if Keyword.get_lazy(opts, :cluster?, &cluster_enabled?/0) do
      replication_args = ["'#{@replication_path}/#{table}'", "'{replica}'"]
      "Replicated#{engine}(#{Enum.join(replication_args ++ args, ", ")})"
    else
      "#{engine}(#{Enum.join(args, ", ")})"
    end
  end

  # See https://clickhouse.com/docs/en/sql-reference/dictionaries#clickhouse for context
  def dictionary_connection_params() do
    IngestRepo.config()
    |> Enum.map(fn
      {:database, database} -> "DB '#{database}'"
      {:username, username} -> "USER '#{username}'"
      {:password, password} -> "PASSWORD '#{password}'"
      _ -> nil
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
  end

  def table_settings() do
    IngestRepo.config()
    |> Keyword.get(:table_settings)
    |> Enum.reject(fn {_, v} -> is_nil(v) end)
  end

  def table_settings_expr(type \\ :prefix) do
    expr = Enum.map_join(table_settings(), ", ", fn {k, v} -> "#{k} = #{encode(v)}" end)

    case {table_settings(), type} do
      {[], _} -> ""
      {_, :prefix} -> "SETTINGS #{expr}"
      {_, :suffix} -> ", #{expr}"
    end
  end

  def enterprise_edition?(), do: ee?()

  def community_edition?(), do: ce?()

  defp encode(value) when is_number(value), do: value
  defp encode(value) when is_binary(value), do: "'#{value}'"
end
