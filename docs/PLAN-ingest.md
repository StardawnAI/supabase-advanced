# Supabase Advanced — Ingest layer

Goal: a document goes in through one endpoint and comes out as searchable
chunks, with the pipeline described by data in the database rather than by a
workflow somewhere else.

The architecture this implements is `docs/supabase-ingest-plattform-plan.md`.
This file is only the build order and its verification.

**Stage 1 and stage 3 are done and verified.** Stage 2 is next; the checklist
for it is at the bottom.

## Scope of stage 1

In:

- the `ingest` schema — tenants, sources, contracts, raw documents, structured
  records, chunks, jobs — with row level security from the first migration
- one `POST /ingest` endpoint, one job per accepted document
- a worker that turns a PDF or a text payload into chunks with full text search
  vectors, and embeddings when an embedding endpoint is configured
- shipped contract templates, so a source can be created from a preset instead
  of a hand-written contract
- delivery in the fork's own pattern: own container, compose overlay, one line
  in `run.sh`, entries in the fork guard

Out (later stages):

- YouTube and the transcript provider chain (stage 3)
- retrieval RPCs and the MCP server (stage 4)
- video and multimodal artifacts (stage 5)
- a Studio page — the API is the interface for now

## Architecture at a glance

```
   POST /ingest ──▶ raw_document (deduplicated by content_hash)
                    └─▶ ingest_job (stage=extract, status=pending)
                                    │
                         worker claims with FOR UPDATE SKIP LOCKED
                                    ▼
                    structured_record  ──▶  chunk (generation N, is_current=false)
                                                      │
                                        one transaction: N-1 → false, N → true
                                                      ▼
                                            search reads is_current only
```

## Checklist

- [x] 1. `docs/PLAN-ingest.md` (this file)
- [x] 2. `docker/ingest/sql/001_schema.sql` — schema, tables, RLS, indexes
     → verify: applied twice in a row without error (idempotent)
- [x] 3. `docker/ingest/sql/002_templates.sql` — shipped contract templates
     → verify: `pdf_generic` present, re-running does not duplicate
- [x] 4. `docker/ingest/db.py` — psql wrapper, migration runner
     → verify: unit tests for SQL literal quoting and migration ordering
- [x] 5. `docker/ingest/api.py` — `POST /ingest`, `/health`, `/sources`, `/jobs`
     → verify: unit tests for auth, dedup, payload validation
- [x] 6. `docker/ingest/worker.py` — claim, extract, chunk, embed, swap
     → verify: unit tests for chunking, hashing, generation swap
- [x] 7. `docker/ingest/Dockerfile` + `docker/docker-compose.ingest.yml`
     → verify: `docker compose config` resolves
- [x] 8. `docker/ingest/ingest-cli.sh` + one line in `run.sh`
     → verify: `sh -n` clean, `run.sh ingest help` prints
- [x] 9. `docker/ingest/test_ingest.py`
     → **57 passed**
- [x] 10. End-to-end: a PDF goes in, a chunk comes out and is found by search
      → `sh docker/tests/test-ingest.sh` — **28/28 passed**, transcript in
      `docs/STATUS.md`
- [x] 11. `.github/fork/protected-paths.txt` + guard passes → **58/58**
- [x] 12. `docker/ingest/README.md` + `.env.example` section + `docs/STATUS.md`

All twelve are done and verified. What stage 1 deliberately does not include is
listed under "Out" above; the open points are at the end of the stage-1 entry in
`docs/STATUS.md`.

## Decisions taken while building (and why)

**The job table is the queue — pgmq is not used.** The plan proposed pgmq with
`ingest_job` holding the state. That means the same fact lives in two places:
a message in a queue and a row describing the same work. They drift, and the
failure is silent — a message consumed while its row still says pending, or the
reverse. `ingest_job` claimed with `FOR UPDATE SKIP LOCKED` is one source of
truth, needs no extension, and is the same mechanism pgmq itself is built on.
The Studio queue UI would not have shown this progress either; it lists pgmq
queues, not our stages.

**Only the standard library, plus `psql` and `pdftotext`.** Same reasoning as
`docker/ha`: no wheel tree to keep patched. Postgres is reached through `psql`,
PDFs through poppler's `pdftotext`, embeddings through an HTTP call with
`urllib`. The container has no `pip install` step at all.

**Embeddings are optional.** Without `INGEST_EMBEDDING_URL` the worker still
produces chunks and full text search vectors, so the stage-1 goal ("a document
goes in, something searchable comes out") holds on a machine with no model
server. Chunks then carry `embedding IS NULL` and are picked up by
`run.sh ingest reembed` once an endpoint exists.

**Tenancy is enforced, not assumed.** Every table with a `tenant_id` has RLS
enabled, keyed on the `ingest.current_tenant()` setting. The service role
bypasses it by design — the API is what sets the tenant per request, per
transaction. `enable` rather than `force`, so the owning role can still
administer the tables; the end-to-end test verifies a non-owner role sees its
own tenant and zero rows of another.

**Migrations run under an advisory lock.** Both containers migrate on start,
and `create extension if not exists` does not survive that race — both see it
missing, both create it, one dies on a unique violation. Found by the
end-to-end test on its second run; the first run happened to win the race.

**The grants are guarded by role existence.** The layer installs on a plain
Postgres too, where `service_role` does not exist. A missing role should mean
"nothing to grant", not a failed migration.

---

## Stage 3 — YouTube transcripts (done)

Built before stage 2 deliberately: a transcript is text, and text already goes
through the whole pipeline. The contract engine is not a prerequisite for it,
and this is the input that was actually wanted.

- [x] 1. `sql/003_operations.sql` — the gaps stage 1 left: reachable
     reprocessing, job retention, stalled-job recovery, four missing indexes
     → verify: exercised by both end-to-end suites
- [x] 2. `sql/004_youtube.sql` — provider chain, breaker, source types,
     three templates
     → verify: breaker marks a provider degraded from its first failure and
     takes it out of the chain at the third
- [x] 3. `fetcher.py` — InnerTube key extraction, client rotation, refusal
     detection, track choice, caption parsing, link parsing
     → **44 unit tests**
- [x] 4. `worker.py` — `discover` and `fetch` stages, credential resolution,
     timed chunking with spans
     → verify: a playlist becomes one job per video, each chunk carries a span
- [x] 5. `admin.py` / CLI — `ingest youtube <link>`, `providers`, `warp on|off|status`
     → verify: a pasted link of any shape resolves to the right kind
- [x] 6. `docker-compose.warp.yml` — NET_ADMIN, /dev/net/tun, src_valid_mark,
     a persistent registration volume, and a healthcheck that asks Cloudflare
     whether traffic is really tunnelling
     → verify: `docker compose config` resolves
- [x] 7. `docker/tests/test-ingest-youtube.sh` + `youtube-stub.py`
     → **29/29 passed**, running the real request chain against a stand-in
- [x] 8. Docs, `.env.example`, fork guard → **58/58**

### What stage 3 is not

- **It has never run against real YouTube.** The stand-in reproduces the
  behaviour the working n8n workflow observes — ANDROID refused from a
  datacenter address, a captionless video answering normally — but a stand-in
  built from expectations cannot surprise you the way the real thing will.
- **No schedule.** `source.schedule` exists and nothing reads it.
- **No quota accounting** against the Data API's daily budget.
- **yt-dlp and Whisper are not in the chain.** The plan lists them as the third
  and fourth fallbacks. Only routes that exist are registered — a provider row
  for something unimplemented would offer a fallback that then fails.

## Stage 2 — the contract engine (next)

The columns exist and are unused: `extraction_prompt`, `extraction_schema`.
Today a contract shapes chunking, language and metadata; it does not yet pull
fields out of the text. That is the step that turns "a searchable transcript"
into "the policy number, the term and the coverage, as columns".

- [ ] 1. An LLM call constrained to `extraction_schema`, with the result
     validated before it is stored
- [ ] 2. `contextual_prefix` — a situating sentence per chunk, generated once
     per document with prompt caching, not once per chunk from scratch
- [ ] 3. `quality_score` from the contract's `quality_gates`, so a bad
     extraction is visible rather than silently indexed
- [ ] 4. `eval_case` / `eval_run` — the measurement without which "the contract
     got better" is an opinion (see section 11 of the architecture plan)
