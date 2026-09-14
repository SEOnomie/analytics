---
status: draft
autor: Andreas Kaiser (kaiser@tracking-garden.com)
stand: 2026-09-14
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
  Unser Fork-`master` steht nach dem Sync vom 2026-09-14 auf `5716baab58`,
  weit vor `v3.2.1`. Ein Rollout des Forks bedeutet also **immer** auch einen
  Versions-Sprung von `v3.2.1` auf einen deutlich neueren Stand
  (u.a. neue OAuth-Tabellen, Team-Deletion-Workflows, Oban-v14-Upgrade) --
  das ist ein eigenes Risiko, unabhaengig von HA, und sollte separat
  verifiziert werden (Migrationen einmal komplett durchlaufen lassen, bevor
  HA obendrauf kommt).
- CH-Version Prod: `25.8.33.6` (letzter 25.8-LTS-Patch). Plausibles eigene
  CI/Makefile pinnen `25.11.5.8-alpine` -- Versions-Differenz siehe Risiken
  (Abschnitt 6).

## 2. Ist-Zustand Code (Fork, identisch zu Upstream)

Der Fork hat 0 eigene Commits (reiner Spiegel von `plausible/analytics`).
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
  aber auch keine Absicht -- eine Neuinstallation kann so nie "weiss", dass
  sie HA werden soll).
- `Plausible.DataMigration` (`lib/plausible/data_migration.ex`): generisches
  Framework fuer manuell angestossene Migrationen (`run_sql/2,3`,
  `run_sql_multi/2,3`, `run_sql_confirm/2,3` mit interaktivem Y/n-Prompt via
  `IO.gets`). Laut Moduldoc "run via remote shell or otherwise (TBD)" --
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
("cluster? extract common code") dazu -- vermutlich weil Plausible Cloud
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
  angelegt. Diese DSL kennt kein `ON CLUSTER` und keine Replicated-Engine --
  bei einer Neuinstallation landen diese Tabellen **immer** als Single-Node-
  Engine, unabhaengig vom Cluster-Status.
- **Kein expliziter Konfig-Schalter:** Die Absicht "ich will HA" existiert
  nirgends, nur die implizite Beobachtung "ist diese Tabelle schon
  repliziert". Fuer eine frische Tabelle ist das immer "nein".
- **Inkonsistente Replikations-Checks:** `IngestRepo.clustered_table?/1`
  prueft `active_replicas > 1`; die eigene SQL-Datei
  `priv/data_migrations/NumericIDs/sql/check-replicas.sql.eex` prueft
  stattdessen `SELECT 1 FROM system.replicas WHERE table = 'events'` --
  Tabellenname `events` statt `events_v2` ist vermutlich eine Altlast aus
  der V1-Schema-Aera und aktuell wirkungslos/potenzieller Bug.
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
Code bereits tut) -- der echte Cluster-Name bleibt Sache der CH-Server-Konfig
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
  (aus welchem Grund auch immer) fehlt -- beide Pruefungen ODER-verknuepft.
- Neue Funktion (Name TBD, z.B. `replicated_engine/3`) kapselt: gib die
  Replicated-Variante einer Engine mit Standard-Zookeeper-Pfad
  `/clickhouse/{cluster}/tables/{shard}/{database}/{table}` zurueck, wenn
  `cluster_enabled?/0`, sonst die Basis-Engine. Dieses Pfadmuster ist
  bereits das, was Plausibles eigene Templates nutzen (siehe 2.1) -- wird
  hier nur zentralisiert statt pro Tabelle dupliziert.

### 3.2 Fresh-Install-Pfad fuer `ingest_counters` + `imported_*`

Diese 11 Tabellen muessen von der Ecto-DSL auf raw-SQL-`execute()`-
Migrationen umgestellt werden (wie `sessions_v2`/`events_v2` es nach ihrer
Konvertierung bereits sind), gesteuert ueber `cluster_enabled?/0`. Fuer eine
echte Neuinstallation reicht eine bedingte Migration ohne Backfill (noch
keine Daten vorhanden).

### 3.3 Migrationspfad fuer bestehende Installationen (Single -> HA)

Statt 11 weiterer Kopien von `VersionedSessions`/`NumericIDs`: ein
generischer Helfer, z.B. `Plausible.DataMigration.ReplicateTable`,
parametrisiert mit Tabellenname, Ziel-Engine-Ausdruck und
ORDER-BY/PARTITION-BY-Klausel, der das bestehende Muster
(Create-Temp -> Backfill -> `EXCHANGE TABLES`, Fallback Rename) generisch
durchfuehrt. Fuer Tabellen ohne Partitionierung (die meisten `imported_*`,
kleines Volumen) genuegt statt Partition-Attach ein einfaches
`INSERT INTO ... SELECT *`.

Im gleichen Zug: `NumericIDs`-Bug fixen (`check-replicas.sql.eex` von
`'events'` auf `'events_v2'`, oder besser: beide Data-Migrationen auf
`Plausible.IngestRepo.clustered_table?/1` umstellen statt eigener
SQL-Datei -- eliminiert die Inkonsistenz strukturell).

### 3.4 Test-Infrastruktur

- Neues Compose-Setup (z.B. `docker-compose.ha-test.yml` oder
  Makefile-Target `make clickhouse-cluster`): 2x `clickhouse-server`
  (Version **25.8.33.6-alpine**, identisch zu unserer Prod-Version -- nicht
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
der eigentliche Cutover, **in-place** auf fcm's bestehender Produktivinstanz
-- keine zweite dauerhafte Instanz, keine Uebernahme von Daten ueber
Instanzgrenzen hinweg (das wuerde eine `site_id`-Neuzuordnung noetig machen,
da ClickHouse kein einfaches `UPDATE` kennt -- bewusst vermieden).

### Phase A -- Validierung mit echtem Traffic (wegwerfbar)

1. Neue ClickHouse-Datenbank (z.B. `plausible_ha_test`) auf dem
   **bestehenden** Altinity-CHI `plausible` (kein zweiter physischer
   Cluster noetig -- die 2 Replicas + 3-Node-Keeper-Quorum sind schon da,
   nur fuer `plausible.*` bisher ungenutzt). Tabellen darin von Anfang an
   als `Replicated*MergeTree` ueber `ON CLUSTER 'plausible'`
   (`CLICKHOUSE_CLUSTER_NAME` gesetzt).
2. Neue, schlanke Plausible-Helm-Release (unser Fork-Image) zeigt auf diese
   Test-DB ueber den **lastverteilten** CH-Service (nicht den gepinnten
   Pod), eigene CNPG-Postgres-Datenbank/Rolle
   (analog `infra/plausible-tg-nw2/cnpg/10-role-plausible.yaml`).
3. Neue interne Caddy-Route (analog
   `plausible.internal.wedapi.eu/api/event`, siehe
   `infra/internal-caddy-proxy/configmap.yaml`) fuer den Test-Ingest --
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
   sGTM-Template-Option, Test-Site) -- diente nur dem Nachweis, dass Config-
   Schalter, Engine-Wahl und `ON CLUSTER`-Pfad unter echtem Traffic
   funktionieren.

### Phase B -- Cutover in-place (dieselbe Produktivinstanz)

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
   bestehender Produktivdatenbank** einmalig ausfuehren -- dieselbe
   Postgres-Site, derselbe Domain-Eintrag, dieselbe `site_id`, volle
   historische Kontinuitaet ohne Datenuebernahme zwischen Instanzen.
5. Erst danach `CLICKHOUSE_DATABASE_URL` vom gepinnten Pod
   (`chi-plausible-clickhouse-plausible-0-0.clickhouse:8123`) auf den
   lastverteilten Service umstellen -- nicht vorher, sonst laufen Queries
   teils gegen den (dann noch leeren) Replica-1.
6. sGTM-Dual-Send-Option aus Phase A wieder entfernen (nicht mehr noetig,
   es gibt nur noch eine Instanz).
7. Decision Record `infra/docs/clickhouse-plausible-replication-decision.md`
   aktualisieren/schliessen.
8. Neue Tenants nach diesem Zeitpunkt starten direkt mit
   `CLICKHOUSE_CLUSTER_NAME` gesetzt -- kein Migrationsschritt noetig.

## 5. Vorschlag Reihenfolge (Umsetzung)

1. `MigrationUtils`-Erweiterung + Konfig-Schalter (Basis fuer alles
   Weitere).
2. Generischer `ReplicateTable`-Helfer, `VersionedSessions`/`NumericIDs`
   darauf umstellen (inkl. Bugfix `check-replicas.sql.eex`).
3. Coverage `ingest_counters` (kleines Datenvolumen, guter erster Test des
   neuen Helfers).
4. Coverage der 10 `imported_*`-Tabellen.
5. Docker-Compose-Testcluster + CI-Job (synthetische, automatisierte
   Absicherung -- Vorstufe zu Phase A).
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
  Team-Deletion-Workflows, Oban-v14) -- unabhaengig von HA, sollte separat
  getestet werden, bevor HA obendrauf kommt.
- **Kein Sharding vorgesehen:** Layout bleibt 1 Shard/2 Replicas. Echtes
  Sharding waere ein deutlich groesseres, separates Vorhaben (Distributed-
  Engine, Sharding-Key-Wahl) und ist hier bewusst nicht Teil des Scopes.
- **Interaktive Prompts:** `run_sql_confirm`/`confirm/3` sind fuer
  automatisierte CI-Laeufe ungeeignet, muessen fuer Tests umgangen werden.

## 7. Nicht Teil dieses Vorhabens

E-Commerce-Tracking (Revenue-Goals) ist ein separates Thema eines anderen
Teams (siehe `project_plausible_fork_ha_feasibility`-Notiz) und bewusst
nicht Teil dieser Branch.
