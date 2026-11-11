defmodule Plausible.ReleaseMigrationUrlTest do
  # Changes global application env, hence not async
  use ExUnit.Case, async: false

  alias Plausible.Release

  setup do
    original = Application.get_env(:plausible, Plausible.IngestRepo)
    on_exit(fn -> Application.put_env(:plausible, Plausible.IngestRepo, original) end)
    {:ok, original_url: Keyword.fetch!(original, :url)}
  end

  defp put_migration_url(value) do
    config =
      :plausible
      |> Application.get_env(Plausible.IngestRepo)
      |> Keyword.put(:migration_url, value)

    Application.put_env(:plausible, Plausible.IngestRepo, config)
  end

  defp ingest_repo_url do
    :plausible |> Application.get_env(Plausible.IngestRepo) |> Keyword.fetch!(:url)
  end

  test "keeps the regular URL when no migration URL is configured", %{original_url: url} do
    put_migration_url(nil)
    Release.use_clickhouse_migration_url()
    assert ingest_repo_url() == url

    put_migration_url("")
    Release.use_clickhouse_migration_url()
    assert ingest_repo_url() == url
  end

  test "uses the migration URL when configured" do
    put_migration_url("http://replica-1:8123/plausible")

    ExUnit.CaptureIO.capture_io(fn -> Release.use_clickhouse_migration_url() end)

    assert ingest_repo_url() == "http://replica-1:8123/plausible"
  end
end
