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

## Verified against real YouTube (2026-08-06)

The chain ran live on stardawneg64, through WARP, against real videos:
a single video (jNQXAC9IVRw) and the @supabase channel limited to three
videos — handle resolution, uploads playlist, one fetch job per video,
srv3 caption parsing, 149 chunks with `{start_ms,end_ms}` spans, full text
search answering with YouTube deeplinks. Two real defects surfaced and are
the reason live runs exist:

1. **Real YouTube speaks srv3** (`<timedtext format="3"><p t d>`), not the
   legacy `<text start dur>` the stub reproduced. Fixed in `parse_transcript`,
   the real payload is now a regression test.
2. **The search default config mismatches the index** — `search` defaults to
   `simple` while chunks carry the contract's config (`german` for the
   channel templates), so matches exist and are not found. The identical
   mistake sits in the existing `dawni_chatbotknowledge.hybrid_search`
   (`websearch_to_tsquery('english')` against a `'simple'` tsvector). Stage 4
   fixes this structurally: the retrieval facade derives the query config per
   chunk from its contract instead of trusting a request parameter.

Also learned live: the Data API works with an OAuth Bearer token — the n8n
workflow never had an API key, and now the fetcher accepts both. Two paper
cuts for stage 6: `--tenant` demands a UUID where a name would do, and the
template picker chose `youtube_channel_de` for an English channel.

---

# The road ahead — stages 2, 4, 5, 6

What the user asked for, in his words: paste a YouTube link in the frontend
and get a searchable knowledge base; chat with that base from the frontend
with a choice of model; credentials collected once, as simply as possible
(one Google sign-in, WARP without ceremony); everything also drivable through
the API; and video itself — frames or clips per passage — designed properly,
not with dumb fixed-interval cuts.

## Stage 2 — the contract engine

The columns exist and are unused: `extraction_prompt`, `extraction_schema`.
Today a contract shapes chunking, language and metadata; it does not yet pull
fields out of the text — or enrich chunks the way the proven n8n embedding
pipeline does. Stage 2 ports that pipeline's ideas into the worker, driven by
contract fields instead of workflow nodes:

- [ ] 1. An LLM call constrained to `extraction_schema`, with the result
     validated before it is stored
- [ ] 2. `contextual_prefix` — a situating sentence per chunk (the Anthropic
     contextual-retrieval pattern the n8n pipeline already uses), generated
     with the document cached once via prompt caching, not resent per chunk
- [ ] 3. **Chapters** (from the n8n pipeline): an LLM pass over the timed
     chunks yields 5–12 chapters with `{title, summary, start_ms, end_ms}`
     aligned to chunk boundaries; stored as rows, referenced by chunks.
     Chapter summaries are themselves embedded — they answer "which video
     covers X" where chunks answer "where exactly"
- [ ] 4. Embedding profiles as data: the shipped default stays the generic
     HTTP endpoint; add a `voyage` profile (`voyage-3-large`, 1024 dims,
     `input_type` document/query asymmetry — the pipeline's proven setup)
- [ ] 5. `quality_score` from the contract's `quality_gates`, so a bad
     extraction is visible rather than silently indexed
- [ ] 6. `eval_case` / `eval_run` — recall@5 and MRR per contract version,
     because "the contract got better" must be measurable (section 11)

## Stage 4 — retrieval facade and chat

Port of the working retrieval prototype (hybrid RRF → rerank → answer with
[n] citations → YouTube deeplinks at the chunk's start_ms), as RPCs plus one
endpoint, not as a workflow:

- [ ] 1. `ingest.search_hybrid(tenant, query, query_embedding, …)` — RRF over
     vector + FTS, **query config derived per contract** (the fix for the
     mismatch found live), filters on `meta`, `is_current` only
- [ ] 2. Rerank step in the ingest service (Voyage rerank-2 or none), behind
     a provider field — same pattern as embedding profiles
- [ ] 3. `POST /ask`: question → query embedding → hybrid → rerank → LLM
     answer with numbered citations carrying `{video_url, start_ms}` — the
     model choice is a parameter, so the same endpoint serves a text-only
     model or a vision model with keyframes attached (stage 5)
- [ ] 4. An MCP server over the same facade, so Claude Code and the chatbot
     use one interface — "check your own database" from any MCP client
- [ ] 5. Verify: eval_case set for the test corpus, recall@5 measured; the
     stemming-mismatch case from the live run becomes a regression eval

## Stage 5 — video: frames and segments per passage

Design decisions, made now against today's model landscape (researched
2026-08-06, sources in STATUS):

**What gets stored.** Three artifact kinds per video, all in object storage
(R2/MinIO — deliberately not Supabase Storage for hours of video), rows in a
new `ingest.artifact` table `{id, tenant_id, raw_document_id, kind, span,
storage_ref, meta}`:

- `video` — the yt-dlp download itself (`bv*[height<=720]`, video-only,
  ~11–17 MB/min; static screencasts far less). Downloaded once, kept —
  re-crawling is the expensive, fragile half
- `keyframe` — one representative frame per detected scene
- `clip` — optional short segments around chapter boundaries, only when a
  contract asks for them

**How frames are chosen — the user's instinct is the researched practice.**
Fixed intervals ("every 2 seconds") are exactly wrong for screencasts: they
miss slide changes and waste frames on stillness. PySceneDetect
content-aware detection with a LOW threshold (~1–5; the default 27 finds
nothing on screen content), one representative frame per scene, plus a
coarse fixed-interval fallback (1 frame/30–60s) for long static stretches.
Threshold lives in the contract (`chunk_strategy.scene_threshold`) because
it needs tuning per corpus.

**Which models can even see video.** Gemini is the only closed-model family
with true video input (File API, 1 fps + audio; ~$0.02–0.09 per 10-min
question depending on tier; YouTube-URL input currently free in preview but
public-only). OpenAI and Anthropic are image-only. Open-weights Qwen3-VL
takes up to 1h of video and runs self-hosted. Therefore:

- default answer path: **keyframes to any vision model** — 6 frames cost
  ~$0.001–0.007 per question, provider-agnostic
- premium path: **Gemini (or Qwen3-VL) with the actual clip** for "where
  do I click" questions — a per-contract, per-question switch
- multimodal embeddings (voyage-multimodal-3.5, $0.12/1M tok + $0.60/1B px)
  as an optional second profile so frames are _searchable_, not only shown

- [ ] 1. `artifact` table + storage adapter (S3 API, works for R2 and MinIO)
- [ ] 2. `video_fetch` stage: yt-dlp through WARP (needs the PO-token
     provider plugin and client rotation — same cat-and-mouse as captions,
     so it joins the provider chain with its own breaker)
- [ ] 3. `frames` stage: PySceneDetect in the worker image; frames land as
     artifacts with spans; chunk ↔ keyframes join on span overlap
- [ ] 4. `/ask` attaches the overlapping keyframes when the chosen model
     takes images; the citation then carries frame thumbnails
- [ ] 5. Verify live on a real tutorial video: frames at the real slide
     changes, not at fixed offsets; a "where do I click" question answered
     with the right frame attached

## Stage 6 — the frontend, and credentials without ceremony

Split exactly as the architecture plan argues (section 9.4): self-hosted
Studio has no login of its own, so **Studio reads, the ingest service
writes**. The Studio page ships in the fork's own image pipeline (the
standby-servers page established the pattern and the cost).

**Credentials, in order of felt friction:**

- **WARP: zero clicks.** Consumer WARP registers unattended — the overlay
  already does. The "Cloudflare button" the user imagined is not needed;
  Zero Trust enrollment is the optional upgrade, not the default.
- **Google/YouTube: one sign-in.** The live run proved the whole Data API
  works on OAuth Bearer alone. Ship one Google OAuth app (ours), request
  only `youtube.readonly`; the token refresh lives in the ingest service;
  `credential_ref` points at Vault. One click covers video, playlist and
  channel sources. (n8n uses far broader scopes — ours must not.)
- **Everything else (Voyage, Anthropic, OpenAI, …): pasted keys** into the
  ingest service's own token-gated UI, stored in Vault, referenced by
  `credential_ref` — never in `connector_config`, never readable back in
  full through the API.

- [ ] 1. Vault-backed credential store in the ingest service
     (`ingest credential add/list/revoke` + minimal web form on :8010, same
     token gate as the API)
- [ ] 2. Google OAuth flow in the ingest service (start URL + callback),
     storing the refresh token in Vault; sources reference it
- [ ] 3. Studio page "Knowledge bases" (read-only, via a Next API proxy like
     /api/ha): sources with status, job throughput, dead jobs with errors,
     per-source chunk counts — and a paste-a-link box that POSTs to the
     ingest service with its token, entered per session in the browser
     (write authority stays with the service token, as with HA promote)
- [ ] 4. Chat page against `/ask`: model picker (text-only vs. vision vs.
     video-capable), source filter, answers with timestamped video links
     and keyframe thumbnails
- [ ] 5. Paper cuts from the live run: `--tenant` accepts names,
     template language chosen from the video's caption language rather
     than defaulting to `_de`

## Execution order

Stage 4 before 2 (retrieval is what makes the existing 149 chunks usable;
chapters and prefixes improve a working search), then 6.1–6.3 (credentials +
read-only page — this is what "simple product" hinges on), then 2, then 5,
then 6.4. Each stage ends with a live run on stardawneg64, not only stubs —
the srv3 lesson generalises.
