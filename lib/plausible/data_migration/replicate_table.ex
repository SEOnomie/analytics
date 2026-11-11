defmodule Plausible.DataMigration.ReplicateTable do
  @moduledoc """
  Converts an existing ClickHouse table between a plain `*MergeTree` engine
  and its `Replicated*MergeTree` variant, keeping table name, columns, keys
  and settings.

  Intended for moving a running single-node installation to a replicated
  cluster (`CLICKHOUSE_CLUSTER_NAME` set). Run per table, e.g. from a remote
  shell:

      Plausible.DataMigration.ReplicateTable.run("events_v2")

  Order of operations (no event loss, no downtime):

    1. `CREATE DATABASE IF NOT EXISTS ... ON CLUSTER`, and an empty placeholder
       of `table` on every replica that does not have it yet, so that all
       following `ON CLUSTER` statements find the same schema everywhere.
    2. Create an empty `<table>_swap_tmp` with the target engine `ON CLUSTER`.
    3. `EXCHANGE TABLES <table>_swap_tmp AND <table> ON CLUSTER`. From here on
       new inserts land in the converted table.
    4. `RENAME` the old table to `<table>_backup` (`<table>_unreplicated_backup`
       for `rollback/2`) `ON CLUSTER`.
    5. `ATTACH PARTITION ID ... FROM` the old table into the converted one on
       the connected node. Parts are hard-linked, replicas fetch them.

  Between 3. and 5. the table only contains new data, queries see incomplete
  history for that short period.

  The old table is kept as fallback and never dropped automatically, see
  `drop_leftover/2`. It initially shares its files with the converted table;
  additional disk usage only builds up as merges rewrite parts.

  `rollback/2` runs the same steps in the opposite direction, so data written
  after the conversion is kept as well.

  Requirements: `Atomic` database engine, ClickHouse macros `{cluster}`,
  `{shard}` and `{replica}`, `CLUSTER` grant for the user. Run it against the
  node that holds the data (the node a single-node installation used so far);
  the run is refused if another replica holds more rows of `table`.

  Options for `run/2` and `rollback/2`:

    * `:dry_run` - only return the planned statements (default `false`)
    * `:repo` - defaults to `Plausible.IngestRepo`
  """

  alias Plausible.MigrationUtils

  @on_cluster "ON CLUSTER '{cluster}'"
  @suffix_format "%Y%m%d%H%M%S"

  @doc """
  Converts `table` to its `Replicated*` engine.
  """
  def run(table, opts \\ []) do
    convert(table, :replicate, opts)
  end

  @doc """
  Converts a replicated `table` back to its plain engine.
  """
  def rollback(table, opts \\ []) do
    convert(table, :unreplicate, opts)
  end

  @doc """
  Drops a table left over by `run/2` or `rollback/2` on all replicas, e.g.
  `drop_leftover("events_v2_backup")`. Only names ending in `_backup` are
  accepted.
  """
  def drop_leftover(name, opts \\ []) do
    repo = Keyword.get(opts, :repo, Plausible.IngestRepo)

    if String.ends_with?(name, "_backup") do
      exec(repo, "DROP TABLE IF EXISTS #{quote_ident(name)} #{@on_cluster} SYNC")
    else
      {:error, :not_a_leftover_table}
    end
  end

  @doc false
  def convert(table, direction, opts) do
    repo = Keyword.get(opts, :repo, Plausible.IngestRepo)
    dry_run? = Keyword.get(opts, :dry_run, false)

    with :ok <- check_cluster_enabled(),
         {:ok, db} <- current_database(repo),
         :ok <- check_atomic(repo, db),
         {:ok, info} <- table_info(repo, db, table),
         {:ok, target_engine_full} <- target_engine(direction, table, info.engine_full),
         backup = backup_name(table, direction),
         :ok <- check_absent(repo, db, swap_name(table)),
         :ok <- check_absent(repo, db, backup),
         :ok <- check_data_location(repo, db, table) do
      ddl = ddl_statements(db, table, info.create_table_query, target_engine_full, backup)

      if dry_run? do
        partitions = partition_ids(repo, db, table)
        {:ok, ddl ++ Enum.map(partitions, &attach_statement(db, table, backup, &1))}
      else
        Enum.each(ddl, &exec!(repo, &1))

        partitions = partition_ids(repo, db, backup)
        Enum.each(partitions, &exec!(repo, attach_statement(db, table, backup, &1)))

        {:ok, %{table: table, backup: backup, partitions: length(partitions)}}
      end
    end
  end

  @doc false
  def ddl_statements(db, table, create_table_query, target_engine_full, backup) do
    swap = swap_name(table)

    [
      "CREATE DATABASE IF NOT EXISTS #{quote_ident(db)} #{@on_cluster}",
      placeholder_statement(create_table_query),
      "CREATE TABLE #{qualified(db, swap)} #{@on_cluster} AS #{qualified(db, table)} " <>
        "ENGINE = #{target_engine_full}",
      "EXCHANGE TABLES #{qualified(db, swap)} AND #{qualified(db, table)} #{@on_cluster}",
      "RENAME TABLE #{qualified(db, swap)} TO #{qualified(db, backup)} #{@on_cluster}"
    ]
  end

  @doc false
  def placeholder_statement(create_table_query) do
    case Regex.run(~r/\ACREATE TABLE (\S+) (.*)\z/s, create_table_query) do
      [_, name, rest] -> "CREATE TABLE IF NOT EXISTS #{name} #{@on_cluster} #{rest}"
      nil -> raise ArgumentError, "unexpected create_table_query: #{create_table_query}"
    end
  end

  @doc """
  Turns the `engine_full` of a plain `*MergeTree` table into the replicated
  variant, keeping engine parameters and the remaining clauses
  (`PARTITION BY`, `ORDER BY`, `SETTINGS`, ...).
  """
  def replicated_engine_full(table, engine_full, suffix \\ timestamp_suffix()) do
    case split_engine(engine_full) do
      {"Replicated" <> _, _, _} ->
        {:error, :already_replicated}

      {engine, args, rest} ->
        if String.ends_with?(engine, "MergeTree") do
          {:ok,
           MigrationUtils.replicated_engine(engine, "#{table}_#{suffix}",
             args: args,
             cluster?: true
           ) <> rest}
        else
          {:error, {:unsupported_engine, engine}}
        end

      nil ->
        {:error, {:unparsable_engine, engine_full}}
    end
  end

  @doc """
  Turns the `engine_full` of a `Replicated*MergeTree` table into the plain
  variant by dropping the replication path and replica name parameters.
  """
  def unreplicated_engine_full(engine_full) do
    case split_engine(engine_full) do
      {"Replicated" <> engine, [_path, _replica | args], rest} ->
        {:ok, "#{engine}(#{Enum.join(args, ", ")})" <> rest}

      {"Replicated" <> _, _, _} ->
        {:error, {:unsupported_engine_params, engine_full}}

      {_, _, _} ->
        {:error, :not_replicated}

      nil ->
        {:error, {:unparsable_engine, engine_full}}
    end
  end

  # Splits "Engine(a, b) PARTITION BY ..." into {"Engine", ["a", "b"], " PARTITION BY ..."}.
  # Engine parameters must not contain parentheses, which holds for MergeTree family
  # parameters (column names, quoted paths).
  defp split_engine(engine_full) do
    case Regex.run(~r/\A(\w+)(?:\(([^()]*)\))?(\s.*)?\z/s, engine_full) do
      [_, engine] -> {engine, [], ""}
      [_, engine, args] -> {engine, split_args(args), ""}
      [_, engine, args, rest] -> {engine, split_args(args), rest}
      nil -> nil
    end
  end

  defp split_args(""), do: []

  defp split_args(args) do
    args |> String.split(",") |> Enum.map(&String.trim/1)
  end

  defp target_engine(:replicate, table, engine_full),
    do: replicated_engine_full(table, engine_full)

  defp target_engine(:unreplicate, _table, engine_full),
    do: unreplicated_engine_full(engine_full)

  defp backup_name(table, :replicate), do: "#{table}_backup"
  defp backup_name(table, :unreplicate), do: "#{table}_replicated_backup"

  defp swap_name(table), do: "#{table}_swap_tmp"

  defp attach_statement(db, table, from, partition_id) do
    "ALTER TABLE #{qualified(db, table)} ATTACH PARTITION ID '#{partition_id}' " <>
      "FROM #{qualified(db, from)}"
  end

  defp check_cluster_enabled() do
    if MigrationUtils.cluster_enabled?(),
      do: :ok,
      else: {:error, :cluster_not_enabled}
  end

  defp current_database(repo) do
    {:ok, %{rows: [[db]]}} = repo.query("SELECT currentDatabase()")
    {:ok, db}
  end

  defp check_atomic(repo, db) do
    case repo.query("SELECT engine FROM system.databases WHERE name = {$0:String}", [db]) do
      {:ok, %{rows: [["Atomic"]]}} -> :ok
      {:ok, %{rows: [[engine]]}} -> {:error, {:unsupported_database_engine, engine}}
    end
  end

  defp table_info(repo, db, table) do
    query = """
    SELECT engine_full, create_table_query
    FROM system.tables
    WHERE database = {$0:String} AND name = {$1:String}
    """

    case repo.query(query, [db, table]) do
      {:ok, %{rows: [[engine_full, create_table_query]]}} ->
        {:ok, %{engine_full: engine_full, create_table_query: create_table_query}}

      {:ok, %{rows: []}} ->
        {:error, {:table_not_found, table}}
    end
  end

  defp check_absent(repo, db, name) do
    case repo.query(
           "SELECT count() FROM system.tables WHERE database = {$0:String} AND name = {$1:String}",
           [db, name]
         ) do
      {:ok, %{rows: [[0]]}} -> :ok
      {:ok, _} -> {:error, {:table_exists, name}}
    end
  end

  # Refuses to run when another replica holds more rows than the connected node,
  # as only the connected node's parts are attached into the converted table.
  defp check_data_location(repo, db, table) do
    {:ok, %{rows: [[local_host]]}} = repo.query("SELECT hostName()")

    query = """
    SELECT hostName(), sum(rows)
    FROM clusterAllReplicas(getMacro('cluster'), system.parts)
    WHERE database = {$0:String} AND table = {$1:String} AND active
    GROUP BY hostName()
    """

    case repo.query(query, [db, table]) do
      {:ok, %{rows: rows}} ->
        {local, remote} = Enum.split_with(rows, fn [host, _] -> host == local_host end)
        local_rows = local |> Enum.map(fn [_, n] -> n end) |> Enum.sum()
        remote_rows = remote |> Enum.map(fn [_, n] -> n end) |> Enum.max(fn -> 0 end)

        if remote_rows > local_rows do
          {:error, {:data_on_other_replica, %{local_rows: local_rows, remote_rows: remote_rows}}}
        else
          :ok
        end

      {:error, error} ->
        {:error, {:data_location_check_failed, error}}
    end
  end

  defp partition_ids(repo, db, table) do
    {:ok, %{rows: rows}} =
      repo.query(
        """
        SELECT DISTINCT partition_id
        FROM system.parts
        WHERE database = {$0:String} AND table = {$1:String} AND active
        ORDER BY partition_id
        """,
        [db, table]
      )

    Enum.map(rows, fn [id] -> id end)
  end

  defp exec(repo, sql) do
    repo.query(sql, [], timeout: :infinity)
  end

  defp exec!(repo, sql) do
    IO.puts("    #{sql}")
    {:ok, _} = exec(repo, sql)
  end

  defp qualified(db, table), do: "#{quote_ident(db)}.#{quote_ident(table)}"

  defp quote_ident(name), do: "`" <> String.replace(name, "`", "\\`") <> "`"

  defp timestamp_suffix() do
    DateTime.utc_now() |> Calendar.strftime(@suffix_format)
  end
end
