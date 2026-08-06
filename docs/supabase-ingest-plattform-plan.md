# Supabase als Ingest-Plattform — Architekturplan

Kein Speicher-Produkt, sondern ein Input-Produkt: gesteuerte Aufnahme + deklarative Aufbereitung + chatbot-optimierte Ablage.

---

## 0. Der zentrale Umdenk-Punkt

Du sagst „die n8n-Logik können wir nachbauen im System". **Nicht nachbauen — invertieren.**

Wenn du die Ausführungslogik in Supabase nachbaust, wartest du zwei Engines: n8n-ultra _und_ deinen eigenen Runner. Du verlierst genau den Vorteil, den dein Fork dir gibt (Custom Nodes, Credential-Handling, Retry-UI, Debuggability).

Der richtige Schnitt:

| Was                                                      | Wo                        | Warum                                                        |
| -------------------------------------------------------- | ------------------------- | ------------------------------------------------------------ |
| Pipeline-**Definition** (was passiert mit welchen Daten) | Postgres, als Daten       | versionierbar, diffbar, pro Tenant, ohne Deployment änderbar |
| Pipeline-**Ausführung**                                  | n8n-ultra + Python-Worker | schon gebaut, schon gehärtet                                 |

**Konsequenz:** Statt 50 n8n-Workflows für 50 Datenquellen baust du **einen generischen Workflow pro `source_type`** (ca. 5–7 Stück), der seine Konfiguration zur Laufzeit aus der DB liest. Neue Kunden-Datenquelle = neue Zeile in `source`, kein neuer Workflow.

Das ist der Unterschied zwischen „ich habe viele Automationen" und „ich habe eine Plattform".

---

## 1. Das eigentliche Produkt: der Ingest Contract

Connectoren sind Commodity — YouTube-API, PDF-Parser, IMAP kann jeder. Der Wert liegt in der Antwort auf: _Was extrahiere ich aus diesem Datentyp, in welche Felder, mit welchem Prompt, in welcher Chunk-Granularität, mit welchen Metadaten?_

Das ist der **Ingest Contract** — und genau das ist das Asset, das nicht wegkommodifiziert wird, weil es aus Feldarbeit entsteht, nicht aus Code. Deine Sammlung „für Versicherungspolicen sieht ein guter Extraktionsvertrag so aus, für Screencast-Tutorials so, für Projektdokumentation so" ist der Data Moat.

**Ein Contract definiert:**

```
ingest_contract
  id, name, version                 -- v3 des "screencast_tutorial"-Vertrags
  source_type                       -- youtube | pdf | email | repo | api_generic
  domain_tag                        -- 'insurance_policy' | 'tutorial' | 'project_doc'
  extraction_schema   jsonb         -- JSON-Schema der Zielfelder
  extraction_prompt   text          -- LLM-Anweisung, gegen das Schema constrained
  chunk_strategy      jsonb         -- {mode, size, overlap, contextual: true}
  metadata_mapping    jsonb         -- welche Felder werden filterbare Spalten
  embedding_profile   text          -- 'bge-m3-1024' | 'qwen3-emb-1024'
  quality_gates       jsonb         -- min. Länge, Pflichtfelder, Confidence-Cutoff
  is_active           bool
```

**Kritisch: `contract_version` wird auf jeden erzeugten Chunk geschrieben.** Wenn du einen Vertrag verbesserst, kannst du gezielt nur die betroffenen Dokumente re-prozessieren (`WHERE contract_version < 3`) statt alles neu zu embedden. Ohne das wird jede Schema-Verbesserung später zu einem Vollre-Index über zehntausende Dokumente — der Punkt, an dem solche Systeme aufhören, sich zu verbessern.

---

## 2. Schichtenmodell

```
┌─ INPUT ─────────────────────────────────────────────┐
│  Upload (PDF/Docx)  ·  Ingest-API  ·  E-Mail-Catch  │
│  YouTube+WARP  ·  Repo/Git  ·  Scheduled Crawler    │
└──────────────────┬──────────────────────────────────┘
                   ▼  jeder Kanal erzeugt nur EINS: ein raw_document + einen Job
┌─ QUEUE ─────────────────────────────────────────────┐
│  pgmq (Postgres-native)  ·  Retry  ·  Idempotenz    │
└──────────────────┬──────────────────────────────────┘
                   ▼
┌─ PROCESSING ────────────────────────────────────────┐
│  n8n-ultra (leichte Jobs) │ Python-Worker (schwer)  │
│  liest Contract aus DB → extrahiert → chunked       │
└──────────────────┬──────────────────────────────────┘
                   ▼
┌─ STORAGE (drei physisch getrennte Ebenen) ──────────┐
│  raw_document → structured_record → chunk           │
└──────────────────┬──────────────────────────────────┘
                   ▼
┌─ RETRIEVAL ─────────────────────────────────────────┐
│  RPC-Fassade (hybrid + rerank) → MCP-Server → Bot   │
└─────────────────────────────────────────────────────┘
```

**Wichtigster Speicherentscheid: `raw_document` wird nie verworfen.** Du wirst deine Verträge zwangsläufig verbessern. Wenn du nur die verarbeitete Form behältst, musst du für jede Verbesserung neu crawlen/neu herunterladen — bei YouTube via WARP ist das der teuerste und fragilste Teil der Kette. Roh-Ablage ist billig, Re-Crawl nicht.

---

## 3. Kernschema

```sql
-- Mandanten: ab Tag 1, nicht später
tenant(id, name, plan, created_at)

-- Was für Datenquellen existieren überhaupt (Registry)
source_type(key, display_name, connector_config_schema jsonb)
  -- 'youtube_channel','youtube_video','pdf_upload','email_inbox',
  -- 'git_repo','api_generic','reddit_export'

-- Eine konkrete konfigurierte Quelle eines Kunden
source(
  id, tenant_id, source_type_key, ingest_contract_id,
  display_name,
  connector_config jsonb,      -- Channel-ID, IMAP-Adresse, Repo-URL, API-Key-Ref
  credential_ref text,         -- Zeiger auf Vault, NIE Klartext
  schedule text,               -- cron oder null (= nur push/manuell)
  status, last_run_at, last_error
)

-- Rohdaten, unangetastet
raw_document(
  id, tenant_id, source_id,
  source_uri text,             -- YouTube-URL, S3-Pfad, Message-ID
  content_hash text,           -- Idempotenz-Key
  storage_ref text,            -- R2/MinIO-Pfad für Binärdaten
  raw_payload jsonb,           -- für Text/JSON direkt hier
  fetched_at, UNIQUE(tenant_id, content_hash)
)

-- Vertragsgetrieben extrahierte Struktur
structured_record(
  id, raw_document_id, ingest_contract_id, contract_version int,
  extracted jsonb,             -- gegen extraction_schema validiert
  quality_score numeric,
  extracted_at
)

-- Was der Bot tatsächlich sieht
chunk(
  id, tenant_id, structured_record_id, contract_version int,
  content text,
  contextual_prefix text,      -- Anthropic-Style, vor dem Embedding
  embedding vector(1024),
  fts tsvector,                -- German config
  meta jsonb,                  -- gefiltert wird hier: stack_tag, doc_type, date …
  span jsonb                   -- {start_ms,end_ms} bei Video / {page} bei PDF
)

-- Jobs
ingest_job(
  id, tenant_id, source_id, raw_document_id,
  stage text,                  -- fetch|extract|chunk|embed
  status, attempts, last_error, payload jsonb,
  idempotency_key text UNIQUE
)
```

Indizes: HNSW auf `chunk.embedding`, GIN auf `chunk.fts` und `chunk.meta`, B-Tree auf `(tenant_id, contract_version)`.

**RLS auf allen Tabellen ab Tag 1.** Nachträglich Multi-Tenancy einziehen ist der teuerste Refactor, den du dir einhandeln kannst — und wenn Kundendaten (Policen!) drin landen, ist es auch ein Compliance-Thema, kein Komfort-Thema.

---

## 4. Die Input-Kanäle

### 4.1 Eine Ingest-API, nicht viele

Du beschreibst „eine API-Anbindung für diese Art von Daten, eine für jene". Das explodiert. Stattdessen:

```
POST /ingest
  { source_id, payload | file_ref, external_id? }
→ 202 { raw_document_id, job_id }
```

Ein Endpoint. Der `source_id` bringt den Contract mit, der Contract entscheidet alles Weitere. Die Variabilität lebt in Daten, nicht in Endpunkten. Für Kunden, die pushen wollen, ist das ein Webhook mit HMAC — Node hast du im Fork bereits.

### 4.2 Upload (PDF/Docx)

Supabase Storage → Storage-Webhook → `raw_document` + Job. Der Kunde sieht nur einen Drop-Bereich; die Contract-Zuordnung passiert über den Bucket-Pfad (`/{tenant}/{source_id}/…`) oder eine Auswahl im UI.

### 4.3 E-Mail-Ingest (dein Reddit-n8n-Korpus)

Catch-all-Adresse pro Source (`src-<uuid>@ingest.deine-domain`). Jede Mail = ein `raw_document`. Dedup über `Message-ID` als `content_hash`. Das ist der billigste Weg, einen jahrelang gewachsenen Mail-Korpus ohne Migration einzuhängen — du leitest die Bestandsmails einmalig dorthin weiter und der laufende Fluss geht ab sofort direkt hin.

### 4.4 Repo/Projektdoku

CocoIndex als externer Indexer, der direkt in `chunk` schreibt (pgvector-nativ, inkrementell). Kein eigener Parser.

---

## 5. YouTube-Transkripte via Cloudflare WARP

### 5.0 Wie es heute tatsächlich läuft (gelesen aus dem bestehenden Workflow)

Dieser Abschnitt beschrieb das Verfahren ursprünglich als „WARP + timedtext". Der laufende Workflow macht etwas deutlich Genaueres, und wer ihn portiert, muss diese Kette kennen — sie ist der eigentliche Wert:

1. **Metadaten** über die offizielle YouTube Data API v3 mit API-Key (`videos`, `playlistItems`, `search`). Ganz normaler Aufruf, kein Proxy, keine Tricks.
2. **Schlüssel besorgen:** die normale Watch-Seite des Videos laden und den `INNERTUBE_API_KEY` per regulärem Ausdruck aus dem Seitenquelltext ziehen. InnerTube ist die interne Schnittstelle, die die YouTube-Apps selbst benutzen. Dieser Aufruf läuft **ohne** Proxy.
3. **Client-Rotation:** ein Aufruf an `youtubei/v1/player`, der sich als YouTube-App ausgibt — nacheinander als ANDROID, WEB, MWEB, IOS und zuletzt TVHTML5. Dieser Aufruf läuft **über WARP**. Antwortet ein Client mit einer Sperre (`LOGIN_REQUIRED`, „Sign in to confirm", oder unabspielbar ohne Untertitel), wird der nächste probiert.
4. **Untertitelspur wählen** — bevorzugte Sprache, sonst die erste verfügbare.
5. **Transkript laden** über die Adresse der gewählten Spur, ebenfalls **über WARP**, und in Volltext plus Zeitsegmente zerlegen.

Drei Konsequenzen für den Plan:

- **Die erste Stufe der Fallback-Kette in 5.2 ist nicht eine Stufe, sondern selbst schon eine Kette.** Die Client-Rotation ist die wirksamste Absicherung im ganzen Verfahren und fehlte bisher im Plan. `transcript_provider` bekommt deshalb ein Feld für die Client-Reihenfolge, damit sie ohne Code-Änderung angepasst werden kann — die Client-Versionen veralten, das ist der Teil, der regelmäßig nachgezogen werden muss.
- **Nur zwei der fünf Schritte brauchen den Proxy.** Alles über die offizielle API läuft direkt. Das halbiert den Verkehr durch WARP und damit das Risiko, den Proxy-Pool zu verbrennen.
- **Der Zweck von WARP ist präzise benennbar:** die Rechenzentrums-IP verstecken. Ein Kommentar im Workflow hält fest, dass der ANDROID-Client von Oracle-Cloud-Adressen blockiert wurde, von einem gewöhnlichen Business-Anschluss aber funktionierte. Das ist die Erklärung dafür, warum der Umweg überhaupt nötig ist — und zugleich der Grund, warum Zero Trust mit fester Ausgangsadresse nicht automatisch besser ist: eine feste Adresse ist auch dauerhaft sperrbar.

### 5.1 Setup-Realität — einfacher als geplant

Für den Egress-IP-Wechsel brauchst du **keinen** Autorisierungs-Link und keinen Cloudflare-Login. Consumer-WARP registriert sich unattended:

```
warp-cli registration new
warp-cli mode proxy          # SOCKS5 auf 127.0.0.1:40000
warp-cli connect
```

Als Container im selben Docker-Netz wie n8n/Worker, dann zeigen die YouTube-Requests per `HTTP_PROXY=socks5://warp:40000` dorthin. Das ist ein Compose-Snippet, kein Onboarding-Flow.

Was das Snippet aber braucht und oben fehlt: der WARP-Client baut auch im Proxy-Modus intern einen WireGuard-Tunnel auf, also `cap_add: [NET_ADMIN]`, `devices: [/dev/net/tun]` und `sysctls: net.ipv4.conf.all.src_valid_mark=1`. Ohne die drei Zeilen startet der Container und `warp-cli connect` scheitert still — der Proxy nimmt Verbindungen an und reicht sie nicht weiter. Dazu ein Volume auf `/var/lib/cloudflare-warp`, sonst registriert sich der Client bei jedem Neustart neu und holt sich eine neue IP mitten im Betrieb.

Der `registration new`-Aufruf gehört in ein Entrypoint-Skript, das prüft ob schon eine Registrierung existiert — sonst ist jeder `docker compose up` eine Neuregistrierung.

**Port-Abgleich mit dem Bestand:** oben steht 40000, das ist der Standardwert des nackten `warp-cli`. Die laufende Installation benutzt aber **1080** (`socks5://warp-…:1080` in beiden Transkript-Knoten), der Standardwert der gängigen WARP-Container-Images. Der Port gehört als Umgebungsvariable in die Konfiguration, nicht fest in den Code — sonst passt die Portierung nicht zum bestehenden Aufbau.

**Der geführte Cloudflare-Link ist nur bei Zero Trust nötig** (Device Enrollment gegen ein Team). Der Trade-off, den du bewusst treffen solltest:

|                 | Consumer-WARP                        | Zero Trust                           |
| --------------- | ------------------------------------ | ------------------------------------ |
| Setup           | unattended, 3 Zeilen                 | Account + Team + Enrollment-Link     |
| IPs             | geteilter Pool, rotierend            | stabiler, dedizierter Egress möglich |
| Blocking-Risiko | höher (Pool wird von vielen genutzt) | niedriger                            |
| Kosten          | 0                                    | Free-Tier bis 50 User, dann bezahlt  |

**Empfehlung:** Consumer als Default (Zero-Friction-Versprechen einlösbar), Zero Trust als optionales Upgrade im Setup-Wizard für Kunden mit Volumen. Nicht Zero Trust als Pflicht — sonst hast du genau die Onboarding-Hürde gebaut, die du vermeiden willst.

### 5.2 Der Punkt, den du architektonisch noch nicht gezogen hast

Du sagst selbst: „wenn das mal nicht mehr funktioniert, mein Gott." Die Konsequenz daraus gehört ins Schema, nicht ins Achselzucken. Sonst ist ein Cloudflare-Blocking-Event ein **Produktausfall** statt einer **Degradation**.

```sql
transcript_provider(
  key,                  -- 'warp_timedtext','ytdlp_cookies','whisper_audio','api_vendor'
  priority int,
  cost_per_min numeric,
  health text,          -- ok | degraded | down
  last_probe_at, consecutive_failures int
)
```

Fallback-Kette, absteigend nach Kosten:

1. **WARP + timedtext** — quasi gratis, aktuell gut
2. **yt-dlp mit Cookie-Jar** — gratis, anderer Fehlermodus
3. **Audio-Download + WhisperX** — GPU-Kosten, funktioniert immer, liefert nebenbei wortgenaue Timestamps (die du für die Video-Pipeline ohnehin brauchst)
4. Kommerzielle API — Notnagel

Dazu ein **Health-Probe-Job** (stündlich, ein bekanntes Referenzvideo) + Circuit Breaker: 3 Fehlschläge in Folge → `health='down'`, Kette rutscht automatisch eine Stufe runter, Alert raus. Der Bot merkt nichts, die Marge sinkt vorübergehend.

Das ist der Unterschied zwischen „läuft solange Cloudflare mitspielt" und einem Produkt, das du Kunden verkaufen kannst.

---

## 6. Rollenverteilung n8n / Worker

| Job                                        | Wo                        | Warum                                    |
| ------------------------------------------ | ------------------------- | ---------------------------------------- |
| Trigger, Scheduling, Webhook-Empfang       | n8n-ultra                 | genau dafür gebaut                       |
| Leichte Extraktion (Text, JSON, API-Calls) | n8n-ultra                 | <60s, kein GPU                           |
| Transkript-Fetch via WARP                  | n8n-ultra                 | I/O-bound, kurz                          |
| Video-Download, VLM, Embedding-Batches     | Python-Worker             | Minuten bis Stunden, GPU, Retry-Semantik |
| Orchestrierung über Stufen hinweg          | pgmq + `ingest_job.stage` | State liegt in Postgres, nicht in n8n    |

**Der Zustand lebt in Postgres, nie in n8n.** Ein abgestürzter n8n-Container darf keinen Ingest-Fortschritt vernichten — Job-Status in der DB, Worker idempotent über `idempotency_key`. Das löst nebenbei das bekannte Problem, dass Queue-Mode-Executions in n8n nicht manuell retrybar sind.

---

## 7. Baureihenfolge

**Stufe 1 — Skelett (das Fundament, das alles andere trägt)**
`tenant` / `source_type` / `source` / `ingest_contract` / `raw_document` / `chunk` + RLS + pgmq + die eine `POST /ingest`-Route. Ein einziger Contract als Referenz (`pdf_generic`). Ziel: eine PDF geht rein, ein durchsuchbarer Chunk kommt raus.

Korrektur dazu in 9.3: pgmq ist in diesem Fork bereits vorhanden und hat eine fertige Studio-Oberfläche. Es ist kein Bauteil dieser Stufe, sondern ein Schalter.

**Stufe 2 — Contract-Engine**
Generischer n8n-Workflow, der Contract liest, Extraktion gegen `extraction_schema` ausführt, validiert, chunked, embedded. Ab hier ist ein neuer Datentyp eine DB-Zeile, kein Workflow.

**Stufe 3 — YouTube + WARP**
WARP-Container, `transcript_provider`-Kette, Health-Probe, `youtube_channel`-Contract. Erst hier, weil es ohne Stufe 1+2 nur wieder eine Insel-Automation wäre.

**Stufe 4 — Retrieval-Fassade**
RPC-Funktionen für hybrid search + Rerank, darüber ein MCP-Server. Ein Interface für Chatbot _und_ Claude Code.

**Stufe 5 — Video-Multimodal**
Python-Worker, `artifact`-Tabelle, `span`-Felder. Setzt Stufe 1–4 voraus.

---

## 8. Entscheidungen, die du jetzt treffen musst (weil später teuer)

1. **`tenant_id` überall ab Tag 1** — auch wenn du erstmal allein bist.
2. **`content_hash` als Dedup-Key** — sonst produziert jeder Re-Run Duplikate im Vektorindex, die die Retrieval-Qualität still verschlechtern.
3. **`contract_version` auf jedem Chunk** — sonst kein selektives Re-Indexing.
4. **Raw-Layer nie verwerfen.**
5. **Videodateien nicht in Supabase Storage** — R2 oder MinIO. Supabase Storage für Dokumente ja, für Stunden von Video nein.
6. **Credentials nie in `connector_config`** — nur `credential_ref` auf Vault. Wenn Kunden ihre eigenen API-Keys hinterlegen, ist das der Unterschied zwischen Produkt und Haftungsrisiko.
7. **Embedding-Profil als Feld, nicht als Konstante** — du wirst das Modell wechseln, und dann willst du zwei Profile parallel fahren können statt einen Big-Bang-Re-Index.

---

## 9. Wie das in diesen Fork kommt — die Lücke, die alles andere blockiert

Der Plan oben beschreibt eine Architektur auf grüner Wiese. Dieses Repo ist aber kein leeres Projekt, sondern ein Fork von `supabase/supabase`, der täglich automatisch von Upstream nachgezogen wird (`.github/workflows/sync-upstream.yml`). Die tragende Regel dieses Forks steht in `docs/STATUS.md`: **der Fußabdruck in Dateien, die Upstream auch anfasst, wird so klein wie möglich gehalten** — derzeit vier Zeilen in vier Dateien. Alles Eigene liegt in Dateien, von denen Upstream nichts weiß, weil dort ein Merge-Konflikt gar nicht erst entstehen kann.

Wer den Ingest-Layer plant, ohne diese Regel einzurechnen, plant ein Feature, das beim ersten Upstream-Sync anfängt zu bluten.

### 9.1 Das Muster existiert schon — HA ist die Blaupause

Die HA-Replikation wurde exakt nach diesem Schnitt gebaut, und der Ingest-Layer sollte ihn kopieren statt einen neuen zu erfinden:

| Bestandteil          | HA (existiert)                 | Ingest (analog)                    |
| -------------------- | ------------------------------ | ---------------------------------- |
| Dienst als Container | `docker/ha/agent.py`           | `docker/ingest/api.py` (+ Worker)  |
| Compose-Overlay      | `docker/docker-compose.ha.yml` | `docker/docker-compose.ingest.yml` |
| CLI                  | `run.sh ha …`                  | `run.sh ingest …`                  |
| Ein Hook in `run.sh` | eine Zeile                     | eine Zeile                         |
| Schutz gegen Sync    | `protected-paths.txt`          | dieselbe Datei erweitern           |
| Tests                | `docker/ha/test_agent.py`      | dito                               |

Ein **Overlay** ist eine zusätzliche Compose-Datei, die neben die originale gelegt wird, statt sie zu ändern — so bleibt `docker/docker-compose.yml` (eine Upstream-Datei) unberührt.

### 9.2 Eigenes Postgres-Schema, nicht `public`

Alle Tabellen aus Abschnitt 3 gehören in ein Schema `ingest`, nicht nach `public`. Zwei Gründe: die Kunden-DB bleibt sauber trennbar, und die HA-Standbys replizieren das Schema ohne Sonderbehandlung mit (physische Replikation kopiert den ganzen Cluster).

Die Migration darf **nicht** nach `supabase/migrations/` — das ist der Ordner der Supabase-eigenen Doku-Datenbank (Seitentexte, Fehlercodes, Meetups), nicht der des self-hosted Stacks.

Für den Stack gilt `docker/volumes/db/*.sql`. Diese Dateien werden allerdings einzeln in `docker-compose.yml` eingehängt (Zeilen 488–502) — also in einer Upstream-Datei. Eine neue Zeile dort wäre ein fünfter Fußabdruck. Stattdessen hängt das Overlay `docker-compose.ingest.yml` den Mount an; Compose führt die `volumes`-Listen eines Dienstes zusammen, statt sie zu ersetzen.

Zweiter Punkt dazu: Init-Skripte laufen nur bei einer **leeren** Datenbank. Für jede bestehende Installation braucht es zusätzlich einen idempotenten Migrationslauf beim Start des Ingest-Containers. Ohne den bekommt der Ingest-Layer nur, wer neu aufsetzt.

### 9.3 pgmq musst du nicht einführen — es ist schon da

Der Plan führt pgmq als neue Komponente ein. Tatsächlich ist es als **Supabase Queues** bereits Teil des Produkts, inklusive fertiger Oberfläche im Studio (`apps/studio/components/interfaces/Integrations/Queues/`, `apps/studio/data/database-queues/`). Job-Warteschlangen sind damit kein Bauteil der Stufe 1, sondern nur ein Aktivieren — und du bekommst Queue-Monitoring, Purge und Nachrichten-Ansicht geschenkt, statt sie zu bauen.

Das verschiebt Stufe 1 spürbar nach vorn.

### 9.4 Die Studio-Oberfläche — was sie kostet und was sie ersetzt

Der Plan hat kein einziges Wort zur Bedienoberfläche, obwohl genau das der Teil ist, der aus „ein paar Tabellen" ein Produkt macht: Quellen anlegen, Contracts bearbeiten, Jobs beobachten, fehlgeschlagene Dokumente ansehen.

Aus der HA-Arbeit ist bekannt, was eine Studio-Seite in diesem Fork wirklich kostet:

- Self-hosted Studio läuft als **fertiges Image** (`docker/docker-compose.yml:17`), ohne Build-Schritt. Jede Änderung unter `apps/studio` erfordert ein eigenes Image plus die Workflow-Pipeline, die es baut (`build-studio-image.yml` existiert bereits).
- `next build` prüft die Typen nicht (`ignoreBuildErrors: true`). Ein Typfehler wird zu einem erfolgreich gebauten Image mit weißer Seite. `pnpm --filter studio typecheck` muss von Hand laufen.
- Jede Seite braucht einen Zwilling unter `routes/**`, sonst verschwindet sie im TanStack-Build.
- Self-hosted Studio hat **keine eigene Anmeldung** (`withAuth` ist wirkungslos wenn `IS_PLATFORM` false ist). Alles, was von dort aus schreibend wirkt, ist damit für jeden erreichbar, der das Dashboard-Passwort kennt.

Der letzte Punkt entscheidet den Schnitt: **Contract-Bearbeitung und Quellen-Anlage gehören nicht ins self-hosted Studio**, weil dort ein Kunde sonst mit Dashboard-Passwort die Extraktions-Prompts und Credential-Verweise fremder Mandanten ändern könnte. Vernünftige Aufteilung:

| Was                                                | Wo                                                       |
| -------------------------------------------------- | -------------------------------------------------------- |
| Lesen: Quellenliste, Job-Status, Fehler, Durchsatz | Studio-Seite (eine Menüzeile, wie HA)                    |
| Schreiben: Contracts, Credentials, Quellen anlegen | eigene Oberfläche des Ingest-Dienstes, mit eigenem Token |

Das ist dieselbe Entscheidung, die bei HA schon getroffen wurde (Ansicht im Studio, Promotion beim Agent) — und sie hat sich dort bewährt.

---

## 10. Zwei Lücken im Schema aus Abschnitt 3

### 10.1 `contract_version` allein reicht nicht — es fehlt die Umschaltung

Der Plan sagt richtig, dass jeder Chunk seine Contract-Version tragen muss, damit man gezielt nachbessern kann. Er sagt aber nicht, **was während der Nachbesserung passiert**. Wenn Vertrag v3 die Dokumente neu zerlegt, existieren für ein und dasselbe Dokument eine Weile lang v2- und v3-Chunks nebeneinander. Der Chatbot durchsucht in dieser Zeit beide und bekommt jede Aussage doppelt — in leicht abweichender Formulierung. Das ist schlimmer als der Zustand vorher, weil die Trefferliste mit Dubletten voll läuft und die wirklich relevanten Stellen verdrängt werden.

Dagegen hilft ein Umschalter pro Dokument:

```sql
chunk(
  …,
  generation int,           -- pro (raw_document, contract) hochgezählt
  is_current bool default false
)
-- Suche liest ausschließlich WHERE is_current
-- Neue Generation wird komplett aufgebaut, dann in EINER Transaktion:
--   alte auf false, neue auf true. Alte danach löschbar.
```

Damit ist ein Re-Processing jederzeit abbrechbar, ohne dass der Bot je einen halben Zustand sieht.

### 10.2 Löschen ist im Plan nicht vorgesehen

Sobald Versicherungspolicen im System liegen, ist „lösch alles zu diesem Kunden/Dokument" keine Komfortfunktion, sondern eine Pflicht mit Frist. Der Plan sagt an einer Stelle „Raw-Layer nie verwerfen" — das ist als Regel gegen unnötiges Neu-Crawlen richtig, kollidiert aber ungebremst mit einer Löschaufforderung.

Beides zusammen geht nur mit einer bewussten Regel:

- Löschen läuft über `raw_document` und kaskadiert nach unten (`structured_record`, `chunk`) — dafür Fremdschlüssel mit `on delete cascade` von Anfang an, sonst bleiben verwaiste Vektoren zurück, die weiterhin gefunden werden.
- Eine Aufbewahrungsfrist pro Mandant oder Quelle (`retention_days`), die einen nächtlichen Lauf steuert.
- Und der Punkt, den man leicht übersieht: der **HA-Standby repliziert byte-genau mit**, gelöschte Zeilen verschwinden dort also automatisch — aber jedes Datei-Backup und jedes Objekt in R2/MinIO muss separat abgeräumt werden. Das ist ein eigener Löschpfad, kein Nebeneffekt.

---

## 11. Woran du misst, ob ein Contract besser geworden ist

Der Plan nennt den Ingest Contract das eigentliche Asset — und beschreibt keinen Weg festzustellen, ob eine neue Version davon besser ist als die alte. Ohne den ist „Vertrag verbessern" reine Bauchentscheidung, und mit jeder Version ist unklar, ob das Retrieval gewonnen oder verloren hat.

Das nötige Werkzeug ist klein: eine Tabelle mit Beispielfragen und der Angabe, welche Textstelle die richtige Antwort enthält.

```sql
eval_case(
  id, tenant_id, ingest_contract_id,
  question text,
  expected_chunk_ids uuid[],     -- oder ein Textausschnitt, der vorkommen muss
  note text
)
eval_run(
  id, ingest_contract_id, contract_version int,
  recall_at_5 numeric, mrr numeric, ran_at
)
```

Fünfzig gute Fälle pro Domäne reichen, um eine Verschlechterung sofort zu sehen. Zwei gebräuchliche Kennzahlen: **Recall@5** heißt „steht die richtige Stelle unter den ersten fünf Treffern", **MRR** misst, wie weit oben sie steht. Beide lassen sich vollautomatisch nach jedem Contract-Wechsel berechnen.

Der Wert davon ist nicht die Zahl selbst, sondern dass sie eine Behauptung überprüfbar macht: „für Versicherungspolicen sieht ein guter Extraktionsvertrag so aus" ist erst dann ein Asset, wenn es einen Beleg dafür gibt.

---

## 12. Der Ausführer ist austauschbar — und der bestehende Workflow beweist es

Eine frühere Fassung dieses Abschnitts behauptete, n8n werde durch Abschnitt 0 zur Pflichtkomponente des Produkts. Das ist falsch, und der bestehende Workflow (`docs/n8n/Youtube Transcript Generator MCP.json`, 53 Knoten) zeigt warum. n8n ist dort Prozess-Orchestrierung: Auslöser, Schleifen, Wartezeiten, HTTP-Aufrufe. Die eigentliche Substanz sind drei Netzwerkaufrufe und rund 300 Zeilen JavaScript in Code-Knoten. Das ist portierbar, nicht gebunden.

Wichtiger ist deshalb, den **Vertrag zwischen Definition und Ausführung** so zu schneiden, dass der Ausführer beliebig ist. Vier Punkte, mehr braucht es nicht:

1. Ein Job wird aus der Warteschlange geholt (`pgmq`), nicht aus einem systemeigenen Trigger.
2. Der Ausführer liest seine gesamte Konfiguration aus `ingest_contract` und `source` — kein Wissen im Workflow selbst.
3. Er schreibt Ergebnis und Status zurück, idempotent über `idempotency_key`.
4. Er meldet Fehler als Daten (`last_error`, `attempts`), nicht als Absturz einer Ausführung.

Wer diese vier Punkte einhält, ist ein gültiger Ausführer — heute n8n, morgen ein Python-Worker, für einen Einzelfall auch ein Bash-Skript. Damit ist die Frage „welches Werkzeug" keine Architekturfrage mehr, sondern eine Betriebsentscheidung. Genau das ist der Sinn davon, den Zustand in Postgres zu halten.

### 12.1 Was der bestehende Workflow über die Aufteilung verrät

Der YouTube-Workflow existiert **zweimal im selben Dokument**: einmal für ein einzelnes Video (`Get Base Url`, `Fetch Transcript`, …) und einmal identisch für eine Playlist (`Get Base Url1`, `Fetch Transcript1`, …). Derselbe Ablauf, zweimal gepflegt, weil sich der Einstieg unterscheidet.

Das ist kein Vorwurf an den Workflow, sondern das Argument für den ganzen Plan in einem Bild: Sobald „Video" und „Playlist" zwei Zeilen in `source` sind statt zweier Zweige im Diagramm, verschwindet die Dopplung. Genau dieser Schritt ist der Übergang von Automation zu Plattform.

Der Workflow schreibt außerdem bereits nach Supabase, in ein eigenes Schema pro Anwendungsfall (`dawni_chatbotknowledge.youtube_videos`, `deepmines_internal.news_from_interviews`) und mit einem Statusfeld (`ingestion_status = 'metadata_complete'`). Das Muster aus Abschnitt 3 wird also faktisch schon gelebt — es ist nur pro Fall neu erfunden statt einmal definiert.

### 12.2 Das eigentliche Produkt: mitgelieferte Anwendungsfälle

Der Punkt, den der Plan bisher nur andeutet: Ein leeres Vertragsformular ist kein Produkt. Ein Kunde soll nicht `extraction_schema`, `chunk_strategy` und `embedding_profile` ausfüllen — er soll sagen können „hier ist eine YouTube-Playlist, mach daraus etwas Durchsuchbares", und der Rest ist schon entschieden.

Konkret heißt das: Der Ingest-Layer wird mit einer **Bibliothek fertiger Verträge** ausgeliefert, versioniert wie Code, importiert wie Stammdaten.

```sql
-- ausgelieferte Vorlage, gehört dem System
ingest_contract_template(
  key,                 -- 'youtube_playlist', 'youtube_channel', 'pdf_generic',
                       -- 'email_inbox', 'git_repo'
  version int,
  display_name text,   -- "YouTube-Playlist → durchsuchbare Wissensbasis"
  description text,    -- was es tut, in einem Satz, für die Oberfläche
  definition jsonb,    -- vollständiger Vertrag: Schema, Prompt, Chunking, Profil
  requires jsonb       -- was der Anwender stellen muss: YouTube-API-Key, WARP-Proxy
)
```

Ein Anwender wählt eine Vorlage, gibt die Playlist-URL an und liefert das unter `requires` Geforderte. Daraus entsteht eine Zeile in `source` und ein daran gekoppelter `ingest_contract` — der bei Bedarf abweichen darf, aber nicht muss.

Zwei Regeln, die diesen Teil tragfähig machen:

- **Vorlagen werden mit dem Stack aktualisiert, Kundenverträge nicht überschrieben.** Wenn Vorlage `youtube_playlist` v4 erscheint, ist das ein Angebot („neue Version verfügbar, jetzt neu verarbeiten?"), keine stille Änderung an laufenden Daten. Sonst ändert ein Update den Inhalt der Wissensbasis eines Kunden ohne dessen Zutun.
- **`requires` ist maschinenlesbar**, damit die Oberfläche daraus ein Formular erzeugen kann. Sonst wird aus jeder neuen Vorlage wieder eine Handarbeit im Frontend — genau die Explosion, die Abschnitt 4.1 für die APIs vermeidet.

Damit ist der Satz „Supabase kann von Haus aus YouTube" nicht Marketing, sondern eine Zeile in einer Vorlagentabelle. Und die Sammlung dieser Vorlagen ist derselbe Vorteil, den Abschnitt 1 beschreibt — nur ausgeliefert statt nur beschrieben.

---

## 13. Was noch nicht beziffert ist

Der Plan trifft Technologie-Entscheidungen, ohne ihre laufenden Kosten zu nennen. Drei davon können den Betrieb dominieren:

- **Wo die Embeddings entstehen.** `embedding_profile` steht als Feld im Schema, aber nirgends steht, worauf `bge-m3` läuft. Auf einem Kundenserver ohne Grafikkarte ist das der Unterschied zwischen Minuten und Tagen für einen Erstimport. Entweder gehört ein gehosteter Embedding-Endpunkt zum Produkt, oder die Hardware-Anforderung gehört in die Installationsvoraussetzungen.
- **Der `contextual_prefix` kostet einen LLM-Aufruf pro Chunk.** Bei 10.000 Dokumenten à 20 Chunks sind das 200.000 Aufrufe. Das ist beherrschbar, aber nur mit Prompt-Caching (das Dokument einmal zwischenspeichern, dann alle Chunks dagegen laufen lassen) — und das ist eine Design-Entscheidung in der Contract-Engine, keine Optimierung für später.
- **Der HNSW-Index wächst mit.** Bei rund einer Million Chunks à 1024 Dimensionen liegt der Index im Bereich mehrerer Gigabyte und will beim Aufbau in den Arbeitsspeicher. Er landet außerdem auf jedem HA-Standby mit. Ab dieser Größenordnung ist die Frage, ob Ingest-Daten und Anwendungsdaten wirklich in derselben Postgres-Instanz liegen sollen, keine Geschmacksfrage mehr.

Ein Satz zu jedem in der Planung reicht — aber ohne ihn ist der erste ernsthafte Import die Stelle, an der die Zahlen zum ersten Mal auftauchen.
