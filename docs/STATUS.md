# Status

Running log for this fork's own work. Newest first.

## 2026-08-05 — Ingest stage 3: YouTube, and the gaps stage 1 left

### Gaps closed first (`sql/003_operations.sql`)

Reviewing stage 1 before building on it turned up four things that were built
but not reachable, or not maintainable:

- **Reprocessing was unreachable.** The generation machinery was complete and
  tested, but `idempotency_key` is `<hash>:<contract>:extract`, so a second job
  for the same document was refused as a duplicate — correct for a re-upload,
  wrong for "run this again with the improved contract". A revision suffix
  separates the two, behind `ingest.request_reprocess()` and
  `run.sh ingest reprocess`. `documents_behind()` plus
  `reprocess-contract` does the selective case: only what is on an older
  version, which is the whole reason `contract_version` is on every chunk.
- **A worker killed mid-job lost its document.** `status = 'running'` is not
  picked up by the claim query, so the row stayed claimed forever.
  `requeue_stalled()` runs on a timer in every worker.
- **The job table grew forever** — the busiest table in the schema and the one
  the claim index depends on. `prune_jobs()` keeps failures far longer than
  successes: a job that worked is history, a job that did not is evidence.
- **Four missing indexes**, including `(tenant_id, source_id, external_id)` —
  asked once per candidate by every scheduled source and a sequential scan
  until now.

### YouTube (`sql/004_youtube.sql`, `fetcher.py`)

A port of `docs/n8n/Youtube Transcript Generator MCP.json`, with the parts that
were duplicated per entry point collapsed into one path and the parts that were
constants moved into the database.

```sh
sh run.sh ingest warp on
sh run.sh ingest youtube <tenant> "https://www.youtube.com/playlist?list=PL..."
```

| Piece                       | Where                                 |
| --------------------------- | ------------------------------------- |
| Provider chain + breaker    | `ingest.transcript_provider`          |
| Client identities           | `transcript_provider.clients` (jsonb) |
| Fetching, rotation, parsing | `docker/ingest/fetcher.py`            |
| discover / fetch stages     | `docker/ingest/worker.py`             |
| WARP proxy                  | `docker/docker-compose.warp.yml`      |
| Unit tests (44)             | `docker/ingest/test_youtube.py`       |
| End-to-end (29)             | `docker/tests/test-ingest-youtube.sh` |

### Key decisions

**The client rotation is the product, so it lives in data.** ANDROID, WEB,
MWEB, IOS, TVHTML5 with their app versions sit in `transcript_provider.clients`.
Those versions go stale, and the moment a route stops working is never a
convenient moment to deploy.

**A refusal and an absence are different failures.** A refused identity moves to
the next client and counts against the route. A video with no captions stops
the chain immediately, counts against nothing, and is not retried — no route
invents captions. Getting this backwards either retires working routes on
ordinary unavailable videos, or walks the whole chain spending proxy requests
to learn nothing.

**Only two of the five requests are proxied.** Data API calls and the watch page
go direct. The proxy is the scarce resource; spending it on requests nobody
objects to burns the pool for nothing.

**`PermanentError` is a distinct failure class.** A video with no captions, a
scanned PDF, an unsupported media type — retrying these five times with growing
backoff spends the retry budget and, for YouTube, proxy requests, to be told
the same thing again. They go to `dead` after one attempt.

**Curl, not urllib.** The standard library cannot speak SOCKS5, and the proxy is
SOCKS5. Same trade already made for `psql` and `pdftotext`. Secrets travel to
curl through `--config -` on stdin, never in argv, because `ps` is readable by
every account on the machine.

**Timed chunks keep their span.** Transcripts are cut on sentence boundaries
and each chunk records `{start_ms, end_ms}`. Recomputing that later by matching
text is guesswork the moment a phrase repeats.

### Bugs found by testing, and fixed

1. **`finish()` overwrote the job's stage** with `'embed'` on success, so every
   completed fetch and discovery looked like an extraction and "how many
   fetches ran" was unanswerable. The stage says what a job _is_; how far it
   got is `status`.
2. **The `updated_at` trigger made the column impossible to write.** An
   unconditional `now()` breaks backdating during a data migration and made the
   retention sweep untestable — every attempt to age a row re-stamped it as
   current. It now only stamps when the statement did not set it itself.
3. **A captionless video was retried five times** with growing backoff, each
   attempt spending a proxy request to be told the same thing. Hence
   `PermanentError`.

### Proof

- Unit tests: **101 passed** (56 stage 1 + 44 YouTube + 1 added while fixing).
- Stage 1 end-to-end: **28/28**.
- YouTube end-to-end: **29/29** (`sh docker/tests/test-ingest-youtube.sh`).
  It runs the real request chain against a stand-in YouTube — curl really
  connects, the rotation really rotates, the caption file is really downloaded
  and parsed. It proves: a playlist link becomes one fetch job per video;
  ANDROID is refused and WEB takes over; the requested language and manual
  captions are honoured; a captionless video fails **without taking the route
  down**; every chunk carries a valid span; the transcript is findable; an
  unchanged transcript is not ingested twice; the breaker degrades at two
  failures and opens at three, leaving another route in the chain, and `--reset`
  restores it; reprocessing leaves exactly one generation current; a job
  abandoned by a dead worker is requeued; retention prunes finished jobs.
- Fork guard: **58/58**.

### Open

- **Stage 2 is still not built.** `extraction_prompt` and `extraction_schema`
  are unused. A contract shapes chunking, language and metadata — not field
  extraction. For a video, `extracted` carries what the fetcher knows (title,
  channel, date, duration), so the shape is right and the LLM step is missing.
- **Never run against real YouTube from here.** The rotation, the refusal
  handling and the parsing are exercised against a stand-in. The stand-in was
  built from the observed behaviour of the working n8n workflow, but the first
  run against the real thing is still the first run against the real thing.
- **WARP is not verified end to end either** — the compose overlay carries the
  capabilities and healthcheck it needs, and `ingest warp status` asks
  Cloudflare whether traffic is really tunnelling, but no container has been
  started here.
- **No scheduled re-runs.** `source.schedule` exists and nothing reads it. A
  channel is fetched when asked, not nightly.
- **Quota is not tracked.** A large channel walks its uploads playlist 50 videos
  per call; nothing counts that against the Data API's daily budget.

## 2026-08-05 — Ingest layer, stage 1

Documents go in through one endpoint and come out as chunks a chatbot can
answer from. What happens to a document is read from its contract — a row in
the database — so a new kind of data is a row, not a new workflow. Plan and
reasoning: `docs/supabase-ingest-plattform-plan.md`, build order:
`docs/PLAN-ingest.md`.

### What was built

| Piece                | File                                  |
| -------------------- | ------------------------------------- |
| Schema, RLS, indexes | `docker/ingest/sql/001_schema.sql`    |
| Shipped templates    | `docker/ingest/sql/002_templates.sql` |
| Database access      | `docker/ingest/db.py`                 |
| HTTP API             | `docker/ingest/api.py`                |
| Worker               | `docker/ingest/worker.py`             |
| Operator commands    | `docker/ingest/admin.py`              |
| Raw payloads         | `docker/ingest/storage.py`            |
| CLI behind `run.sh`  | `docker/ingest/ingest-cli.sh`         |
| Overlay              | `docker/docker-compose.ingest.yml`    |
| Unit tests (56)      | `docker/ingest/test_ingest.py`        |
| End-to-end test (27) | `docker/tests/test-ingest.sh`         |
| Documentation        | `docker/ingest/README.md`             |

### Key decisions

**The job table is the queue.** The plan proposed pgmq alongside `ingest_job`.
That puts the same fact in two places — a message and a row describing the same
work — and they drift silently: a message consumed while its row still says
pending, or the reverse. One table claimed with `FOR UPDATE SKIP LOCKED` is a
single source of truth and is the mechanism a queue extension uses internally
anyway. The Studio queue UI would not have shown these stages either; it lists
pgmq queues.

**No values ever reach SQL as text.** `psql` takes SQL as a string, so every
value crossing that boundary would be an injection. Everything travels as one
JSON document through `COPY ... FROM STDIN` into a temp table, and statements
read from that table. One escaping rule to get right instead of one per call
site, no 128 KB argument limit, and a statement whose text never varies with
its data. The one identifier that has to come from data — the text search
configuration — is cast to `regconfig` at run time rather than pasted in.

**Standard library only, plus `psql` and `pdftotext`.** Same reasoning as
`docker/ha`: no wheel tree to keep patched. The image has no `pip install`.

**Embeddings are optional.** Without `INGEST_EMBEDDING_URL` chunks are still
produced and searchable by text, so the stack is useful on a machine with no
model server. `run.sh ingest reembed` fills the vectors in later.

**Chunks carry a generation and an `is_current` flag.** A better contract
builds its chunks alongside the live ones and the switch happens in one
transaction. Without it, old and new passages answer the same question at once
and every result list fills with near-duplicates — the plan named
`contract_version` but not the switch, which is the half that makes it safe.

### Bugs found by testing, and fixed

1. **Concurrent migrations collided.** The API and the worker start together
   and both migrate. `create extension if not exists vector` is not the guard
   it looks like under that race: both see it missing, both create it, one gets
   a unique violation on `pg_extension` and the container dies. Migrations now
   run under an advisory lock, and the bookkeeping insert is
   `on conflict do nothing`. Found by the end-to-end test on its second run —
   the first run happened to win the race.
2. **`admin.py` and the tests were not in the image.** The Dockerfile was
   written before them, so every `run.sh ingest` command would have failed on a
   fresh install with "no such file".
3. **The API and the worker did not share the raw volume** in the test stack.
   The API writes the original, the worker reads it back; mounting it on one of
   them fails in a way that looks like a broken extractor.
4. **Grants assumed Supabase roles exist.** Installing on a plain Postgres died
   at `grant ... to service_role`. Now guarded by a role-existence check, so the
   layer installs beside any database.

### Proof

- Unit tests: **56 passed** (`cd docker/ingest && python3 -m unittest discover -s .`).
- End-to-end: **27/27 passed** (`sh docker/tests/test-ingest.sh`), on an
  isolated `pgvector/pgvector:pg16` with the Supabase roles created, so the
  grant path runs rather than being skipped. It proves: a real PDF is parsed by
  `pdftotext` and becomes a chunk found by searching for its words; an
  unrelated search returns nothing; the same bytes twice produce one document;
  reprocessing leaves exactly one generation current; a confined role sees its
  own tenant's chunks and **zero** of another tenant's, and zero with no tenant
  set; deleting a document leaves no orphaned chunks; a PDF with no text layer
  fails saying it needs OCR and is retried rather than dropped.
- Fork guard: **51/51**.
- `docker compose --env-file .env.example -f docker-compose.yml -f docker-compose.ingest.yml config` resolves.

### Open

- **Stage 2 (contract engine) is not built.** Extraction currently means "take
  the text"; the `extraction_prompt` and `extraction_schema` columns exist and
  are unused. Until then a contract shapes chunking, language and metadata, but
  not field extraction.
- **YouTube is stage 3.** `docs/n8n/Youtube Transcript Generator MCP.json` is
  the working reference: metadata over the official API, then an InnerTube
  player call rotated across five client identities through the WARP proxy.
  That rotation is the real first line of defence and belongs in
  `transcript_provider`.
- **No Studio page.** The API is the interface. A read-only view would follow
  the standby-servers pattern; anything that writes must not live in
  self-hosted Studio, which has no authentication of its own.
- **A service token, not per-tenant tokens.** The token authenticates the
  operator; the tenant comes from the source or an explicit field. Per-tenant
  credentials belong with a customer-facing surface.
- **`INGEST_EMBEDDING_DIMENSIONS` cannot change the column.** The schema fixes
  `vector(1024)`. A model of another width needs a migration, and the worker
  says so instead of failing at the insert.

## 2026-07-26 — Upstream sync automation, and a Studio page

## 2026-07-26 — Upstream sync automation, and a Studio page

### Keeping upstream updates from eating our work

`.github/workflows/sync-upstream.yml` merges `supabase/supabase` daily. Daily,
not weekly, so each merge stays small enough to resolve.

It never resolves a conflict on its own:

| Outcome          | What happens                                                                     |
| ---------------- | -------------------------------------------------------------------------------- |
| Clean merge      | Verifies our changes survived, then pushes                                       |
| Conflict         | Pushes a branch, opens a PR naming the colliding files, pushes nothing to master |
| Our work missing | Pushes nothing and fails loudly                                                  |

The third case is the point. `.github/fork/verify-intact.sh` reads
`.github/fork/protected-paths.txt` and asserts that every file we own still
exists **and** that every hook we placed inside an upstream file is still
there. Verified by removing the `run.sh` hook and confirming the check fails
(`MISSING MARKER docker/run.sh`), then restoring it: 32/32.

**No blanket "ours" merge strategy**, deliberately. Always preferring our
version of a contested file also discards whatever upstream fixed in it,
including security fixes, and does so silently. The guard makes losing our work
loud instead, which is the part that actually needed solving.

The strategy the guard enforces is: keep the fork's footprint inside upstream
files as small as possible. It is currently **four lines across four files**
(`run.sh`, `.gitattributes`, `.env.example`, `SettingsMenu.utils.tsx`).
Everything else lives in files upstream has never heard of, where a conflict is
impossible.

### Studio page — and what it costs

`/project/default/settings/standby-servers` shows every node, its role, its
replication state and its lag, and calls out the two states worth waking up
for: no primary, and two primaries.

Findings that shaped it:

- **Self-hosted Studio is a prebuilt image** (`docker/docker-compose.yml:17`
  pins `supabase/studio:2026.07.07-sha-a6a04f2`, no `build:`). No change under
  `apps/studio` reaches a running instance without building and publishing a
  custom image. Hence `docker-compose.studio-fork.yml` (an overlay, so the
  upstream image line is never edited) and `.github/workflows/build-studio-image.yml`.
- **Upstream already has a "High Availability" feature** of its own for
  platform projects (`hooks/misc/useHighAvailability.ts`). Ours was renamed to
  **Standby servers** to avoid both the confusion and a future collision if
  upstream adds a settings page under that name.
- **The page is read-only.** Self-hosted Studio has no auth of its own
  (`withAuth` is a no-op when `IS_PLATFORM` is false), so a promote button here
  would quietly change the authority to fail over a database from "knows the HA
  token" to "knows the dashboard password". Promotion stays on the agent page.
- **The page cannot be relied on during an outage**: `standby.compose.yml` runs
  no Studio, so it is served only from the primary host. It is a health view
  for normal operation; the agent's own page on port 8008 answers on every node
  and is the one for incidents.
- **`next build` does not typecheck** (`ignoreBuildErrors: true`), and the only
  typecheck workflow is PR-triggered on runner labels this fork does not have.
  A type error would ship as a successfully built image and a blank page. Run
  `pnpm --filter studio typecheck` by hand.

### Bug found in yesterday's work

`ha-agent` declared `depends_on: db: condition: service_healthy`. After a
reboot with a damaged database the agent would never start — the one component
able to report the outage would itself be missing. Removed on both the primary
overlay and the standby stack.

### Proof

- Studio unit tests: **10 passed** (`npx vitest run components/interfaces/Settings/StandbyServers`).
- Typecheck: **0 errors in the fork's own files.** The run reports 216
  pre-existing `ui-patterns/admonition` resolution errors across upstream files,
  an artefact of installing with `--node-linker=hoisted --ignore-scripts` to work
  around a Windows/OneDrive symlink failure; `Admonition` is exported normally
  (`packages/ui-patterns/src/admonition/index.tsx:1`).
- Fork guard: **32/32**, and verified to fail when a hook is removed.

### Deliberate deviation

No entry was added to `apps/studio/TANSTACK_MIGRATION.md`, though the Studio
conventions ask for one. That file tracks upstream's own migration progress;
adding a fork-only page to it would mean one more line inside an upstream file
— the exact thing the sync strategy minimises — for no functional gain. The
`routes/**` twin itself _was_ added, since without it the page would silently
vanish from a TanStack build.

## 2026-07-25 — HA feature complete on `feat/ha-replication`

### Fork brought up to date

The fork had **no commits of its own** and was 5039 behind upstream, last synced
2025-09-13. Fast-forwarded to upstream `2b27ed0ab1` (2026-07-24) and pushed.
Nothing could be lost — there was nothing local to lose. No fork-sync GitHub
Action exists in this repo; the sync was done from the console.

### What was built

Streaming replication with automatic failover for the self-hosted stack.

| Piece                                                       | File                               |
| ----------------------------------------------------------- | ---------------------------------- |
| Agent: roles, lag, promotion, witness quorum, overview page | `docker/ha/agent.py`               |
| Standby clone                                               | `docker/ha/bootstrap-standby.sh`   |
| Primary preparation                                         | `docker/ha/setup-primary.sh`       |
| Router config generator                                     | `docker/ha/render-haproxy.sh`      |
| CLI behind `run.sh ha`                                      | `docker/ha/ha-cli.sh`              |
| Primary overlay                                             | `docker/docker-compose.ha.yml`     |
| Standby stack (second server)                               | `docker/standby.compose.yml`       |
| Unit tests (37)                                             | `docker/ha/test_agent.py`          |
| End-to-end test                                             | `docker/tests/test-ha-failover.sh` |
| Documentation                                               | `docker/ha/README.md`              |

### Key decisions

**Route through `POSTGRES_HOST`.** Every Supabase service already reaches
Postgres through that one variable. Pointing it at a router makes failover
invisible to all of them, with no per-service change.

**Physical, not logical replication.** A standby has to carry roles, DDL,
extensions and the `_supabase` database. Logical replication reproduces none of
those.

**Auto-failover requires witnesses, and refuses without them.** Two nodes cannot
distinguish a dead primary from a broken link between them. Promoting during a
split leaves two primaries whose data cannot be merged. Default is manual.

**The overview page lives in the agent, not in Studio.** It stays up when the
database it reports on is down, works on every node, and keeps the feature out
of a Studio codebase this fork re-syncs from upstream in bulk. This is the one
place the delivered shape differs from "a page inside Studio" — see Open below.

### Bugs found by testing, and fixed

Each of these would have failed on a first real deployment:

1. **`pg_reload_conf()` is silently refused.** In Supabase `postgres` is not a
   superuser (only `supabase_admin` is), so the freshly added replication rule
   was never loaded. Now the postmaster is signalled directly, and a failure is
   fatal rather than ignored.
2. **`pg_promote()` is refused for the same reason.** The standby detected the
   outage, got witness agreement, decided to promote — and was rejected every
   two seconds. Agents now connect as `supabase_admin`.
3. **`hot_standby=off` in the Supabase image.** A node in recovery then refuses
   every connection, making the standby unusable: no reads, no health check, no
   role query. Standbys now start with `hot_standby=on`.
4. **The readiness check tested the wrong connection type.** Postgres matches
   logical replication connections against ordinary `host all all` rules, so the
   check passed while the clone still failed. It now opens the physical
   connection `pg_basebackup` uses.
5. **HAProxy permanently disabled unresolvable nodes.** A standby added later
   would never be routed to. Fixed with a runtime resolver.
6. **Missing healthchecks broke `run.sh start`**, which uses `up --wait`.
7. **The router needed a `start_period`** — it resolves every node twice at
   startup, so binding can take ~20s.

### Proof

- Unit tests: **37 passed** (`python3 -m unittest discover -s docker/ha`).
- Automatic failover, two nodes plus witness and router on stardawneg64:
  **24/24 checks passed.** Replication lag 0.27s, standby promoted itself
  **6s** after the primary was killed, router followed in 0s, no committed row
  lost.
- Manual mode: **27/27 checks passed.** The standby did _not_ promote itself, a
  promote without a valid token was refused with 401, and the operator's
  promotion through the agent worked.

Tested against `supabase/postgres:15.8.1.048` because that image was already on
the host. The stack default is 17.6.1.136; the mechanism is version-independent,
but the pair must match.

### Open

- **Studio integration.** Delivered as a standalone page served by the agent
  (reasoning above). A page inside Studio remains possible if wanted; it would
  mean carrying Studio patches across upstream syncs.
- **Failover covers the database only.** If a whole server disappears, something
  still has to point clients at the other server's Kong — a DNS or
  load-balancer job outside this stack.
- **Replication is asynchronous by default**, so a failover can lose the last
  transactions that had not reached the standby. Setting
  `synchronous_standby_names` trades write latency for not losing them.
