---
status: draft
autor: Andreas Kaiser (kaiser@tracking-garden.com)
stand: 2026-11-11
---

# ClickHouse HA fuer Plausible: Bestandsaufnahme und Design

Ziel dieses Dokuments: technischer Plan fuer Branch `feature/clickhouse-ha`
(Fork `SEOnomie/analytics`). Gewuenschtes Ergebnis:

1. Ein expliziter Konfig-Schalter, mit dem eine Plausible-Instanz wahlweise
   gegen eine Single-Instance- oder eine HA-ClickHouse-Umgebung laeuft.
2. Ein Migrationspfad von Single-Instance zu HA fuer bereits laufende
   Installationen mit Live-Daten.
3. Design bleibt generisch (kein Hardcoding von Cluster-Namen oder
   TG-Infra-Spezifika), damit ein spaeterer PR gegen `plausible:master`
   realistisch bleibt.

## 1. Ist-Zustand Infra (`platform/infra/`)

- Altinity-Operator-CHI `plausible-clickhouse`
  (`infra/plausible-tg-nw2/clickhouse/20-clickhouse-installation-plausible.yaml`),
  Cluster-Name **`plausible`**, Layout **1 Shard / 2 Replicas**.
- Keeper: eigenstaendiges ClickHouse-Keeper, 3-Node-Raft-Quorum (2 Replicas +
  1 Tiebreaker `50-ch-keeper-tiebreaker.yaml`, 2/3-Quorum toleriert Ausfall
  eines Node).
- `tp2.*`-Tabellen (unsere Go-Services) laufen bereits produktiv mit
  `ON CLUSTER 'plausible'` + `Replicated*MergeTree(...)` im selben CHI,
  Makro-Pfad `'/clickhouse/tables/{shard}/{database}/{table}', '{replica}'`
  (kein `{cluster}`-Segment im Pfad).
- `plausible.*`-Tabellen sind bewusst **nicht** repliziert (Decision Record
  `infra/docs/clickhouse-plausible-replication-decision.md`, Stand 2026-08-13):
  Plausible verbindet sich fest auf den gepinnten Pod
  `chi-plausible-clickhouse-plausible-0-0.clickhouse:8123`, Replica-1 ist fuer
  Plausible-Tabellen leer.
- Aktuelle Prod-App ist **nicht** unser Fork, sondern das offizielle Image
  `ghcr.io/plausible/community-edition:v3.2.1`
  (`infra/plausible-tg-nw2/plausible/30-values-plausible.yaml`, Helm-Chart,
  `clickhouse.enabled: false` da wir den Altinity-CHI separat betreiben).
  Unser Fork-`master` steht nach dem Sync vom 2026-11-11 auf `0c3903a941`
  (Upstream-Stand 2026-11-10), weit vor `v3.2.1`. Ein Rollout des Forks
  bedeutet also **immer** auch einen
  Versions-Sprung von `v3.2.1` auf einen deutlich neueren Stand
  (u.a. neue OAuth-Tabellen, Team-Deletion-Workflows, Oban-v14-Upgrade).
  Das ist ein eigenes Risiko, unabhaengig von HA, und sollte separat
  verifiziert werden (Migrationen einmal komplett durchlaufen lassen, bevor
  HA obendrauf kommt).
- CH-Version Prod: `25.8.33.6` (letzter 25.8-LTS-Patch). Plausibles eigene
  CI/Makefile pinnen `25.11.5.8-alpine`, Versions-Differenz siehe Risiken
  (Abschnitt 6).

## 2. Ist-Zustand Code (Fork, identisch zu Upstream)

`master` ist ein reiner Spiegel von `plausible/analytics`. Auf
`feature/clickhouse-ha` sind seit 2026-11-11 die Schritte 1 und 2 (Abschnitt 5)
umgesetzt, Details in Abschnitt 3.1 unter "Umsetzungsstand" und in 3.3. Die Bestandsaufnahme in
diesem Abschnitt beschreibt den Upstream-Stand vor dieser Aenderung.

Abgleich 2026-11-11 (Upstream `5716baab58` -> `0c3903a941`, 130 Commits):
keine Aenderungen an `lib/plausible/migration_utils.ex`,
`lib/plausible/ingest_repo.ex`, `lib/plausible/data_migration.ex`,
`priv/ingest_repo/` und den HA-relevanten Data-Migrationen. CI pinnt
weiterhin `25.11.5.8-alpine`. `config/runtime.exs` hat nur neue Query-Timeouts
(`CLICKHOUSE_QUERY_TIMEOUT_MS`, `CLICKHOUSE_MAX_EXECUTION_TIME_SEC`)
bekommen. Die Bestandsaufnahme unten gilt damit unveraendert.
Es existiert bereits eingebaute, aber unvollstaendige und undokumentierte
Cluster-Infrastruktur:

### 2.1 Vorhandene Bausteine

- `Plausible.MigrationUtils.on_cluster_statement/1`
  (`lib/plausible/migration_utils.ex`): liefert `"ON CLUSTER '{cluster}'"`
  oder `""`, abhaengig von `IngestRepo.clustered_table?/1`.
- `Plausible.IngestRepo.clustered_table?/1` und `replica_count/1`
  (`lib/plausible/ingest_repo.ex`): reine Laufzeit-Erkennung via
  `SELECT sum(active_replicas) FROM system.replicas WHERE table = '...'`.
  Fuer eine noch nicht existierende Tabelle liefert das `0` (kein Crash,
  aber auch keine Absicht: eine Neuinstallation kann so nie "weiss", dass
  sie HA werden soll).
- `Plausible.DataMigration` (`lib/plausible/data_migration.ex`): generisches
  Framework fuer manuell angestossene Migrationen (`run_sql/2,3`,
  `run_sql_multi/2,3`, `run_sql_confirm/2,3` mit interaktivem Y/n-Prompt via
  `IO.gets`). Laut Moduldoc "run via remote shell or otherwise (TBD)",
  nicht Teil von `mix ecto.migrate`, nicht automatisiert.
- Zwei konkrete, fertige Daten-Migrationen darauf aufgebaut:
  - `VersionedSessions` (`lib/plausible/data_migration/versioned_sessions.ex`):
    `sessions_v2` -> `ReplicatedVersionedCollapsingMergeTree`, per
    Create-Temp-Table -> Partitions attachen -> `EXCHANGE TABLES`
    (Fallback `rename-table.sql.eex`, falls `EXCHANGE TABLE` nicht
    unterstuetzt wird).
  - `NumericIDs` (`lib/plausible/data_migration/numeric_ids.ex`): baut
    `events_v2` als `ReplicatedMergeTree` neu auf, Nebenprodukt einer
    grossen V1->V2-Schema-Migration (2024), nicht primaer fuer HA gedacht.
- `LocationsSync` und `AcquisitionChannel`: orchestrieren Dictionary
  (`location_data_dict`) bzw. materialisierte Spalte cluster-bewusst von
  Anfang an (idempotent, kein Einmal-Umbau bestehender Daten noetig).

### 2.2 Herkunft dieser Bausteine (zur Einordnung)

Commit `8ba2b934b3` (Feb 2024, PR #3802, Autor Karl-Aksel Puulmann,
Plausible-Core) hatte als eigentliches Ziel einen Engine-Wechsel wegen
Dedup-Korrektheit, nicht HA. Die `cluster?`-Verzweigung kam als Nebencommit
("cluster? extract common code") dazu, vermutlich weil Plausible Cloud
selbst auf einem Cluster laeuft und Cloud/CE denselben Code teilen. Kein
CHANGELOG-Eintrag, keine Doku, keine Test-Abdeckung dafuer. Es ist kein
von Plausible gepflegtes Feature fuer Self-Hoster, sondern brauchbares,
aber unvollstaendiges Rohmaterial.

### 2.3 Luecken

- **Ecto-Migrations-DSL ohne Cluster-Support:** `ingest_counters`
  (`priv/ingest_repo/migrations/20230214114402_create_ingest_counters_table.exs`)
  und alle 10 `imported_*`-Tabellen
  (`priv/ingest_repo/migrations/20211112130238_create_imported_tables.exs`)
  werden ueber `Ecto.Migration.create_if_not_exists table(..., engine: "...")`
  angelegt. Diese DSL kennt kein `ON CLUSTER` und keine Replicated-Engine:
  bei einer Neuinstallation landen diese Tabellen **immer** als Single-Node-
  Engine, unabhaengig vom Cluster-Status.
- **Kein expliziter Konfig-Schalter:** Die Absicht "ich will HA" existiert
  nirgends, nur die implizite Beobachtung "ist diese Tabelle schon
  repliziert". Fuer eine frische Tabelle ist das immer "nein".
- **Inkonsistente Replikations-Checks:** `IngestRepo.clustered_table?/1`
  prueft `active_replicas > 1`; die eigene SQL-Datei
  `priv/data_migrations/NumericIDs/sql/check-replicas.sql.eex` prueft
  stattdessen `SELECT 1 FROM system.replicas WHERE table = 'events'`:
  Tabellenname `events` statt `events_v2` sah zunaechst nach Altlast aus.
  Nachtrag 2026-11-11: `NumericIDs` liest aus der V1-Tabelle `events`, die
  Pruefung auf die Quelltabelle ist also beabsichtigt, kein Bug. In Schritt 2
  nur um den Konfig-Schalter ergaenzt.
- **Keine Migration fuer `ingest_counters` und `imported_*`:** weder Fresh-
  noch Nachtraeglich-Pfad existiert fuer diese 11 Tabellen.
- **Keine Testabdeckung:** `Makefile` (`make clickhouse`) und
  `.github/workflows/elixir.yml` starten ausschliesslich Single-Node-CH,
  kein Keeper, kein zweiter Node. Der komplette `ON CLUSTER`-Pfad ist damit
  in der offiziellen Testsuite ungetestet.

## 3. Zielarchitektur

### 3.1 Konfig-Schalter

Neue ENV-Variable `CLICKHOUSE_CLUSTER_NAME` (Default: leer/nicht gesetzt =
Single-Instance, unveraendertes Verhalten). Wichtig fuer Generizitaet: der
tatsaechliche String-Wert dieser Variable wird **nicht** ins SQL
interpoliert. Er dient nur als Aktivierungs-Flag. Das SQL verwendet weiterhin
das ClickHouse-seitige Server-Makro `'{cluster}'` (wie es der bestehende
Code bereits tut): der echte Cluster-Name bleibt Sache der CH-Server-Konfig
(vom Altinity-Operator oder manuell per `<macros>` gepflegt), nicht des
Elixir-Codes. Damit bleibt der Code cluster-namens-agnostisch und
upstream-tauglich.

`Plausible.MigrationUtils`:

- `cluster_enabled?/0`: liest `CLICKHOUSE_CLUSTER_NAME` aus Config
  (`Application.get_env(:plausible, :clickhouse_cluster_name)` o.ae.,
  gesetzt in `config/runtime.exs` analog zu `CLICKHOUSE_DEFAULT_STORAGE_POLICY`).
- `on_cluster_statement/1` umbauen: primaer `cluster_enabled?/0` pruefen
  (deckt Fresh-Install ab), zusaetzlich (nicht ersetzend)
  `IngestRepo.clustered_table?/1` als Sicherheitsnetz behalten fuer den
  Fall, dass eine Tabelle bereits repliziert ist, die Config-Variable aber
  (aus welchem Grund auch immer) fehlt: beide Pruefungen ODER-verknuepft.
- Neue Funktion (Name TBD, z.B. `replicated_engine/3`) kapselt: gib die
  Replicated-Variante einer Engine mit Standard-Zookeeper-Pfad
  `/clickhouse/{cluster}/tables/{shard}/{database}/{table}` zurueck, wenn
  `cluster_enabled?/0`, sonst die Basis-Engine. Dieses Pfadmuster ist
  bereits das, was Plausibles eigene Templates nutzen (siehe 2.1), wird
  hier nur zentralisiert statt pro Tabelle dupliziert.

**Umsetzungsstand (2026-11-11, Schritt 1):**

- `config/runtime.exs`: `CLICKHOUSE_CLUSTER_NAME` landet als `:cluster_name`
  in der `Plausible.IngestRepo`-Config (neben `table_settings`). Leer oder
  nur Leerzeichen gilt als nicht gesetzt.
- `MigrationUtils.cluster_enabled?/0`: liest `IngestRepo.config()[:cluster_name]`.
- `MigrationUtils.on_cluster_statement/1`: `cluster_enabled?() or
  IngestRepo.clustered_table?(table)`. Bei gesetztem Schalter entfaellt die
  `system.replicas`-Abfrage.
- `MigrationUtils.replicated_engine(engine, table, opts)`: Engine-Name ohne
  Klammern, Optionen `:args` (Engine-Parameter, z. B. `["sign", "events"]`)
  und `:cluster?` (ueberschreibt den Schalter, z. B. fuer das ODER mit
  `clustered_table?/1` einer Referenztabelle). Pfad
  `/clickhouse/{cluster}/tables/{shard}/{database}/<table>` mit
  `'{replica}'`.
- Tests: `test/plausible/migration_utils_test.exs`.
- Ohne gesetzten Schalter ist das Verhalten identisch zu Upstream. Bestehende
  Migrationen und Data-Migrationen sind noch nicht auf `replicated_engine/3`
  umgestellt (Schritte 3 und 4).

**Umsetzungsstand (2026-11-11, Schritt 2):**

- `MigrationUtils.cluster?/1` (Schalter ODER Tabelle schon repliziert).
  Darauf umgestellt: `VersionedSessions`, `LocationsSync`,
  `PopulateEventSessionColumns`. `NumericIDs`: Schalter ODER bestehende
  Pruefung der V1-Quelltabelle `events`.
- `20230320094327_create_v2_schemas.exs`: statt fest `@cluster? false` jetzt
  `cluster_enabled?/0`, Neuinstallationen mit Schalter bekommen
  `events_v2`/`sessions_v2` repliziert.
- Replikationspfad in `LocationsSync`, `AcquisitionChannel`,
  `VersionedSessions` und `NumericIDs/create-sessions-v2` von hart kodiertem
  `plausible_prod` auf `{database}` umgestellt (wirkt nur beim Anlegen).
- `ReplicateTable`-Helfer, siehe 3.3.
- Tests: `test/plausible/data_migration/replicate_table_test.exs`, reine
  Funktionstests laufen in der normalen Suite, Cluster-Tests mit Tag
  `:clickhouse_cluster` (standardmaessig ausgeschlossen, `--include
  clickhouse_cluster` gegen 2 Replicas + Keeper, lokal auf 25.8.33.6
  verifiziert). Das Compose-Setup dafuer kommt in Schritt 5 ins Repo.

### 3.2 Fresh-Install-Pfad fuer `ingest_counters` + `imported_*`

Diese 11 Tabellen muessen von der Ecto-DSL auf raw-SQL-`execute()`-
Migrationen umgestellt werden (wie `sessions_v2`/`events_v2` es nach ihrer
Konvertierung bereits sind), gesteuert ueber `cluster_enabled?/0`. Fuer eine
echte Neuinstallation reicht eine bedingte Migration ohne Backfill (noch
keine Daten vorhanden).

### 3.3 Migrationspfad fuer bestehende Installationen (Single -> HA)

Statt 11 weiterer Kopien von `VersionedSessions`/`NumericIDs`: ein
generischer Helfer `Plausible.DataMigration.ReplicateTable`
(`lib/plausible/data_migration/replicate_table.ex`, umgesetzt 2026-11-11).
Er liest Engine, Parameter und Klauseln aus `system.tables` und braucht
deshalb nur den Tabellennamen:

```elixir
ReplicateTable.run("events_v2")                     # Single -> Replicated
ReplicateTable.run("events_v2", dry_run: true)      # nur Statements zeigen
ReplicateTable.rollback("events_v2")                # Replicated -> Single
ReplicateTable.drop_leftover("events_v2_backup")    # Alt-Tabelle loeschen
```

Ablauf (Entscheidung 2026-11-11: **erst tauschen, dann Altdaten
einhaengen**, damit waehrend der Umstellung kein Event verloren geht und
Plausible nicht gestoppt werden muss):

1. `CREATE DATABASE IF NOT EXISTS ... ON CLUSTER` und leere
   Platzhalter-Tabelle mit identischer Struktur auf allen Replicas, die sie
   noch nicht haben (aus `create_table_query`, `IF NOT EXISTS`). Damit
   finden alle folgenden `ON CLUSTER`-Statements ueberall dasselbe Schema.
2. Leere `<table>_swap_tmp` mit Ziel-Engine `ON CLUSTER` anlegen.
   Replikationspfad `/clickhouse/{cluster}/tables/{shard}/{database}/<table>_<Zeitstempel>`
   (Zeitstempel verhindert Konflikte mit Keeper-Resten nach Rollback).
3. `EXCHANGE TABLES <table>_swap_tmp AND <table> ON CLUSTER` (atomar). Ab
   hier landen neue Inserts in der replizierten Tabelle.
4. Alte Tabelle per `RENAME ... ON CLUSTER` zu `<table>_backup`.
5. Auf dem verbundenen Node jede Partition per
   `ATTACH PARTITION ID ... FROM <table>_backup` einhaengen (Hardlinks,
   Replicas holen die Parts per Replikation). Funktioniert auch fuer
   unpartitionierte Tabellen (Partition `all`).

Zwischen 3. und 5. zeigt die Tabelle nur neue Daten, das Dashboard also fuer
Sekunden bis wenige Minuten unvollstaendige Historie. Eine Sitzung, deren
Zeilen vor und nach dem Tausch geschrieben werden, rechnet sich nach dem
Einhaengen ueber `sign` korrekt zusammen (Test vorhanden).

Schutzmechanismen: nur mit gesetztem Schalter, nur `Atomic`-Datenbanken,
nur `*MergeTree`-Engines, Abbruch wenn `<table>_swap_tmp` oder das Backup
schon existiert, Abbruch wenn eine andere Replica mehr Zeilen hat als der
verbundene Node (sonst wuerden nur die falschen Parts eingehaengt).

`<table>_backup` wird nie automatisch geloescht. Es teilt anfangs seine
Dateien mit der neuen Tabelle (Hardlinks), zusaetzlicher Platz entsteht erst,
wenn Merges die Parts der neuen Tabelle neu schreiben, maximal die Groesse
der Alt-Tabelle. Loeschen per `drop_leftover/2`, sobald kein Rueckweg mehr
gebraucht wird (siehe 4, Rollback).

`rollback/2` nutzt denselben Ablauf in Gegenrichtung (Ziel = Engine ohne
`Replicated`, Alt-Tabelle wird `<table>_replicated_backup`). Da die
replizierte Tabelle alte und neue Daten enthaelt, gehen auch nach der
Umstellung geschriebene Events beim Rueckweg nicht verloren.

`VersionedSessions` und `NumericIDs` werden bewusst **nicht** auf den Helfer
umgestellt (Entscheidung 2026-11-11): `NumericIDs` ist eine einmalige
V1->V2-Umwandlung mit Join auf `domains_lookup`, keine Tabellenkopie,
`VersionedSessions` laeuft in einer historischen Migration. Beide bekommen
nur Schalter-Erkennung und den `{database}`-Pfad.

### 3.4 Test-Infrastruktur

- Neues Compose-Setup (z.B. `docker-compose.ha-test.yml` oder
  Makefile-Target `make clickhouse-cluster`): 2x `clickhouse-server`
  (Version **25.8.33.6-alpine**, identisch zu unserer Prod-Version, nicht
  die 25.11 aus der bestehenden Single-Node-CI, siehe Risiken) + 1x
  `clickhouse-keeper` (ein Node genuegt lokal, kein 3-Node-Quorum noetig),
  mit `<remote_servers>`/`<macros>`/`<zookeeper>`-Config analog zum
  Altinity-CHI-Manifest, Cluster-Name z.B. `test_cluster`.
- Neuer CI-Job in `.github/workflows/elixir.yml` (zusaetzlich zum
  bestehenden Single-Node-Job, nicht ersetzend): setzt
  `CLICKHOUSE_CLUSTER_NAME=test_cluster`, laesst `mix ecto.migrate` und die
  Data-Migrationen (`ReplicateTable`-Aufrufe) automatisiert durchlaufen.
  Die interaktiven `run_sql_confirm`-Prompts muessen dafuer umgangen werden
  (direkt `run_sql`/`run_sql_multi` aufrufen statt der interaktiven
  Wrapper-Funktion).

## 4. Rollout auf TG-Infra (zweiphasig)

Entscheidung (Stand 2026-09-14): Zweiphasiger Rollout mit fc-moto (fcm) als
einzigem aktuell aktivem Plausible-Tenant. Phase A ist eine **wegwerfbare**
Testumgebung, die nichts an fcm's echten Produktivdaten aendert. Phase B ist
der eigentliche Cutover, **in-place** auf fcm's bestehender Produktivinstanz:
keine zweite dauerhafte Instanz, keine Uebernahme von Daten ueber
Instanzgrenzen hinweg (das wuerde eine `site_id`-Neuzuordnung noetig machen,
da ClickHouse kein einfaches `UPDATE` kennt, bewusst vermieden).

### Phase A: Validierung mit echtem Traffic (wegwerfbar)

1. Neue ClickHouse-Datenbank (z.B. `plausible_ha_test`) auf dem
   **bestehenden** Altinity-CHI `plausible` (kein zweiter physischer
   Cluster noetig: die 2 Replicas + 3-Node-Keeper-Quorum sind schon da,
   nur fuer `plausible.*` bisher ungenutzt). Tabellen darin von Anfang an
   als `Replicated*MergeTree` ueber `ON CLUSTER 'plausible'`
   (`CLICKHOUSE_CLUSTER_NAME` gesetzt).
2. Neue, schlanke Plausible-Helm-Release (unser Fork-Image) zeigt auf diese
   Test-DB ueber den **lastverteilten** CH-Service (nicht den gepinnten
   Pod), eigene CNPG-Postgres-Datenbank/Rolle
   (analog `infra/plausible-tg-nw2/cnpg/10-role-plausible.yaml`).
3. Neue interne Caddy-Route (analog
   `plausible.internal.wedapi.eu/api/event`, siehe
   `infra/internal-caddy-proxy/configmap.yaml`) fuer den Test-Ingest:
   **kein** neuer oeffentlicher Hostname noetig, da fcm's sGTM schon heute
   ausschliesslich intern sendet (`45-backendtrafficpolicy-plausible-event-deny.yaml`
   sperrt den oeffentlichen `/api/event`-Pfad ohnehin).
4. fcm's Domain als neue Site in der Testinstanz anlegen.
5. In fcm's sGTM/aGTM: den bestehenden Plausible-Forward als
   konfigurierbare Option im **gemeinsamen** Template ergaenzen (nicht als
   manueller Tenant-Hack, siehe `feedback_manual_sgtm_template_edits_drift`),
   der zusaetzlich an die Test-Route sendet. Consent-Trigger-Bedingungen
   der Kopie muessen identisch zum Original sein.
6. Validierungsfenster laufen lassen, Kennzahlen (Visitors/Pageviews/Events)
   zwischen altem Dashboard und Test-HA-Dashboard vergleichen.
7. Nach erfolgreicher Validierung: Testumgebung komplett abbauen
   (Helm-Release, CH-Test-Datenbank, CNPG-Test-DB/Rolle, Caddy-Route,
   sGTM-Template-Option, Test-Site), diente nur dem Nachweis, dass Config-
   Schalter, Engine-Wahl und `ON CLUSTER`-Pfad unter echtem Traffic
   funktionieren.

### Phase B: Cutover in-place (dieselbe Produktivinstanz)

1. Eigenes Image aus dem Fork bauen und pushen (Konvention aus
   `platform/CLAUDE.md`: 7-stelliger Commit-Hash als Tag, kein `:latest`).
2. `infra/plausible-tg-nw2/plausible/30-values-plausible.yaml`:
   `image.repository`/`image.tag` auf eigenes Image umstellen,
   `CLICKHOUSE_CLUSTER_NAME` als neue Env-Variable ergaenzen.
3. `GRANT CLUSTER ON *.*` fuer den Plausible-DB-User auf **beiden** Nodes
   sicherstellen (aktuell vermutlich nicht gesetzt, da bisher kein
   `ON CLUSTER` fuer Plausible-Tabellen genutzt wurde).
4. Generalisierte Single->HA-Migration (`ReplicateTable`, in Phase A/CI
   bereits erprobt) fuer alle ca. 13 betroffenen Tabellen **auf fcm's
   bestehender Produktivdatenbank** einmalig ausfuehren: dieselbe
   Postgres-Site, derselbe Domain-Eintrag, dieselbe `site_id`, volle
   historische Kontinuitaet ohne Datenuebernahme zwischen Instanzen.
   Vorher `run(table, dry_run: true)` fuer alle Tabellen pruefen.
5. Erst danach `CLICKHOUSE_DATABASE_URL` vom gepinnten Pod
   (`chi-plausible-clickhouse-plausible-0-0.clickhouse:8123`) auf den
   lastverteilten Service umstellen, nicht vorher, sonst laufen Queries
   teils gegen den (dann noch leeren) Replica-1.
6. sGTM-Dual-Send-Option aus Phase A wieder entfernen (nicht mehr noetig,
   es gibt nur noch eine Instanz).
7. Decision Record `infra/docs/clickhouse-plausible-replication-decision.md`
   aktualisieren/schliessen.
8. Neue Tenants nach diesem Zeitpunkt starten direkt mit
   `CLICKHOUSE_CLUSTER_NAME` gesetzt: kein Migrationsschritt noetig.

Bestand Prod (gelesen 2026-11-11): Replica 0 haelt `events_v2`
(221 Mio. Zeilen, 19,3 GiB), `sessions_v2` (18,8 Mio., 3,6 GiB) und Kleinkram
(`location_data`, `ingest_counters`, `acquisition_channel_*`, zusammen ca.
5 MiB), alle `imported_*` sowie `events`/`sessions` (V1) und
`sessions_v2_tmp_versioned` sind leer. Replica 1 hat **alle** Plausible-Tabellen
bereits angelegt (leer, gleiche Engines), das Schema ist also schon
symmetrisch. Freier Platz: Replica 0 140 GiB, Replica 1 164 GiB von je 197 GiB.

### Rollback Phase B

Ziel: Rueckkehr zum Stand vor dem Cutover (offizielles `v3.2.1`,
nicht replizierte Tabellen), ohne Events seit dem Cutover zu verlieren.

1. **Vor dem Cutover:** CNPG-Backup/Snapshot der Plausible-Postgres-DB
   ziehen und pruefen. Der Fork-Stand migriert Postgres (u.a. OAuth,
   Team-Deletion, Oban v14), `v3.2.1` laeuft danach nicht mehr ohne Weiteres
   auf dieser DB. Das ist der eigentliche Engpass des Rueckwegs, nicht HA.
2. `CLICKHOUSE_DATABASE_URL` zurueck auf den gepinnten Pod (Replica 0).
3. Je Tabelle `ReplicateTable.rollback(table)`: Daten inkl. der Events seit
   dem Cutover landen wieder in einer nicht replizierten Tabelle auf
   Replica 0.
4. Postgres aus dem Snapshot von 1. wiederherstellen, Image zurueck auf
   `v3.2.1`, `CLICKHOUSE_CLUSTER_NAME` entfernen. Postgres-seitige
   Aenderungen seit dem Cutover (neue Sites, Goals, Nutzer) gehen dabei
   verloren, Events in ClickHouse nicht.
5. `<table>_backup` und `<table>_replicated_backup` erst loeschen
   (`drop_leftover/2`), wenn klar ist, dass kein weiterer Rueckweg gebraucht
   wird, und ein regulaeres CH-Backup nach dem jeweiligen Stand gelaufen ist.

## 5. Vorschlag Reihenfolge (Umsetzung)

1. `MigrationUtils`-Erweiterung + Konfig-Schalter (Basis fuer alles
   Weitere).
2. Generischer `ReplicateTable`-Helfer, `VersionedSessions`/`NumericIDs`
   darauf umstellen (inkl. Bugfix `check-replicas.sql.eex`).
3. Coverage `ingest_counters` (kleines Datenvolumen, guter erster Test des
   neuen Helfers).
4. Coverage der 10 `imported_*`-Tabellen.
5. Docker-Compose-Testcluster + CI-Job (synthetische, automatisierte
   Absicherung, Vorstufe zu Phase A).
6. Phase A: Testumgebung mit echtem fcm-Traffic (Abschnitt 4), Validierung,
   Abbau.
7. Phase B: Cutover in-place auf fcm's Produktivinstanz (Abschnitt 4).

## 6. Offene Risiken

- **CH-Versions-Differenz:** Prod `25.8.33.6`, Plausibles eigene CI
  `25.11.5.8`. `EXCHANGE TABLES`/`Replicated*`-Verhalten sollte gegen die
  tatsaechliche Prod-Version getestet werden (Testcluster daher bewusst auf
  `25.8.33.6-alpine` gepinnt, nicht auf die neuere CI-Version).
- **`EXCHANGE TABLES`-Verlaesslichkeit:** Plausibles eigener Fallback-Code
  (`rename-table.sql.eex`) deutet auf bekannte Einschraenkungen in manchen
  Docker-Setups hin. Im Altinity-Operator-Betrieb vermutlich unproblematisch,
  aber vor dem Prod-Lauf gezielt verifizieren.
- **Plausible-Versions-Sprung:** Prod laeuft aktuell `v3.2.1` (offizielles
  Image), der Fork steht weit davor. Ein Rollout des Forks bringt also
  immer einen grossen Versions-Sprung mit (OAuth-Tabellen,
  Team-Deletion-Workflows, Oban-v14). Das ist unabhaengig von HA und sollte
  separat getestet werden, bevor HA obendrauf kommt.
- **Kein Sharding vorgesehen:** Layout bleibt 1 Shard/2 Replicas. Echtes
  Sharding waere ein deutlich groesseres, separates Vorhaben (Distributed-
  Engine, Sharding-Key-Wahl) und ist hier bewusst nicht Teil des Scopes.
- **Interaktive Prompts:** `run_sql_confirm`/`confirm/3` sind fuer
  automatisierte CI-Laeufe ungeeignet, muessen fuer Tests umgangen werden.
- **`schema_migrations` in ClickHouse ist `TinyLog`** (nicht repliziert,
  auf beiden Replicas vorhanden, auf Replica 1 leer). Sobald Plausible ueber
  den lastverteilten Service verbindet (Phase B Schritt 5), sieht ein
  Migrationslauf auf Replica 1 eine leere Migrationshistorie. Muss vor
  Schritt 5 geloest werden (z.B. Migrationen fest gegen Replica 0 laufen
  lassen oder `schema_migrations` replizieren), offen.
- **Fresh-Install mit Schalter noch unvollstaendig:** Bis Schritte 3 und 4
  umgesetzt sind, legt die Ecto-DSL `ingest_counters` und `imported_*` nur
  lokal an, spaetere Migrationen mit `ON CLUSTER` darauf scheitern auf den
  anderen Replicas. Den Schalter vorher nicht produktiv setzen.

## 7. Nicht Teil dieses Vorhabens

E-Commerce-Tracking (Revenue-Goals) ist ein separates Thema eines anderen
Teams (siehe `project_plausible_fork_ha_feasibility`-Notiz) und bewusst
nicht Teil dieser Branch.
