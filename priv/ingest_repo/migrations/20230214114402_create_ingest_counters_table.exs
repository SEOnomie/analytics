defmodule Plausible.IngestRepo.Migrations.CreateIngestCountersTable do
  use Ecto.Migration

  def change do
    create_if_not_exists table(:ingest_counters,
                           primary_key: false,
                           engine:
                             Plausible.MigrationUtils.replicated_engine(
                               "SummingMergeTree",
                               "ingest_counters",
                               args: ["value"]
                             ),
                           options:
                             Plausible.MigrationUtils.table_options(
                               order_by:
                                 "(domain, toDate(event_timebucket), metric, toStartOfMinute(event_timebucket))"
                             )
                         ) do
      add(:event_timebucket, :utc_datetime)
      add(:domain, :"LowCardinality(String)")
      add(:site_id, :"Nullable(UInt64)")
      add(:metric, :"LowCardinality(String)")
      add(:value, :UInt64)
    end
  end
end
