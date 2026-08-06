# Ingest layer

Documents go in through one endpoint and come out as chunks a chatbot can
answer from. What happens to a document — how it is extracted, how it is cut
into passages, in what language it is indexed — is read from its **contract**,
a row in the database. A new kind of data is therefore a row, not a new
workflow and not a new endpoint.

The architecture and the reasoning behind it are in
`docs/supabase-ingest-plattform-plan.md`; the build order and its verification
are in `docs/PLAN-ingest.md`.

## Quick start

```sh
cd docker
sh run.sh ingest init          # token, overlay, a tenant and a first source
sh run.sh ingest push <source-id> report.pdf
sh run.sh ingest search <tenant-id> "whatever the report says"
```

`init` prints the tenant and source ids. It is safe to run twice.

## What it is made of

| Piece                | File                | Job                                        |
| -------------------- | ------------------- | ------------------------------------------ |
| HTTP API             | `api.py`            | accepts documents, answers searches        |
| Worker               | `worker.py`         | extracts, chunks, embeds, switches over    |
| Operator commands    | `admin.py`          | everything behind `run.sh ingest`          |
| Database access      | `db.py`             | psql, migrations, and the escaping rules   |
| Raw payloads         | `storage.py`        | originals on disk, addressed by hash       |
| Schema               | `sql/001_schema.sql` | tables, row level security, indexes       |
| Shipped templates    | `sql/002_templates.sql` | the presets `init` creates from        |

Two containers run the same image: `ingest-api` serves, `ingest-worker`
processes. Scale the worker for throughput —

```sh
docker compose up -d --scale ingest-worker=4
```

— which is safe because a job is claimed with `FOR UPDATE SKIP LOCKED`, so two
workers never take the same one.

## The pipeline

```
POST /ingest ──▶ raw_document          deduplicated by sha256 of the payload
                 └─▶ ingest_job        one row, claimed by one worker
                             │
                             ▼
                 structured_record     the extracted text and fields
                             │
                             ▼
                 chunk                 generation N, is_current = false
                             │
                  one transaction: N-1 → false, N → true
                             ▼
                 search reads is_current only
```

The last step is what makes reprocessing safe. A better contract builds its
chunks alongside the live ones and only becomes visible when the whole set is
complete. Without it, old and new passages answer the same question at the same
time and every result list fills up with near-duplicates.

## Endpoints

| Method | Path         | Purpose                            |
| ------ | ------------ | ---------------------------------- |
| GET    | `/health`    | status and counts, no auth         |
| POST   | `/ingest`    | accept a document                  |
| POST   | `/search`    | full text search over current chunks |
| GET    | `/sources`   | a tenant's configured sources      |
| GET    | `/templates` | the shipped presets                |
| GET    | `/jobs`      | recent jobs and their state        |

Everything except `/health` needs `Authorization: Bearer $INGEST_API_TOKEN`.

```sh
curl -sS -X POST http://localhost:8010/ingest \
  -H "Authorization: Bearer $INGEST_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"source_id":"<uuid>","text":"some notes","source_uri":"note-1"}'
```

A document may arrive as `text`, as a JSON `payload`, or as `content_base64`
with a `media_type`. Binaries are written to disk and referenced; text and JSON
are kept in the row.

## YouTube

```sh
sh run.sh ingest warp on          # route transcript requests through WARP
sh run.sh start
sh run.sh ingest youtube <tenant-id> "https://www.youtube.com/playlist?list=PL..."
```

Paste a link — a video, a playlist, a channel, an `@handle`, or a bare id. It
works out which it is, picks the matching template, creates the source and
starts. A playlist link wins over the video inside it, because that is what
clicking a video within a playlist gives you and it is what people mean.

Set `INGEST_YOUTUBE_API_KEY` first: titles, durations and playlist listings
come from the official API. The transcript itself does not use it.

### How a transcript is actually fetched

1. Metadata from the Data API with the key. Ordinary request, never proxied.
2. The public watch page is loaded to lift the InnerTube key out of the HTML.
   Also not proxied — it is a page any visitor loads, and proxy requests are
   the scarce resource.
3. The player is asked for the caption tracks **as a YouTube app** — ANDROID,
   then WEB, then MWEB, then IOS, then the TV client. Through the proxy. A
   refused identity is skipped and the next is tried.
4. The track is chosen by language, preferring manually written captions over
   automatic ones, and downloaded through the proxy.

Step 3 is the part that matters and the reason this is not "download the
subtitles". The identities live in `transcript_provider.clients` because their
version numbers go stale, and updating them must not need a release:

```sql
update ingest.transcript_provider
   set clients = '[...]'::jsonb
 where key = 'warp_innertube';
```

### When it stops working

```sh
sh run.sh ingest providers          # the chain and its health
sh run.sh ingest providers --reset  # clear the breaker, put every route back
```

Three consecutive failures take a route out of the chain; the next route takes
over. Not one failure — a single video can be unavailable for reasons that say
nothing about the route. A video with **no captions at all** is not counted
against the route and is not retried: no route invents captions.

That is the difference between a blocked route being a degradation and being an
outage. The direct route stays available for exactly this reason: it works from
ordinary connections and fails differently from the proxied one.

### The proxy

`sh run.sh ingest warp on` adds `docker-compose.warp.yml` and points the
workers at it. Consumer WARP, which registers itself with no account and no
enrollment link. Check it is really tunnelling, not merely listening:

```sh
sh run.sh ingest warp status        # asks Cloudflare, expects warp=on
```

Zero Trust buys a stable dedicated egress address — which is also a permanently
blockable one. Start with consumer; move only if the shared pool turns out to
be blocked for you.

### Chunks of spoken text carry their time

Transcripts are cut on sentence boundaries and every chunk keeps the span it
covers, so an answer can point back into the video at the second it was said:

```json
{ "start_ms": 122500, "end_ms": 139000 }
```

Recomputing that afterwards by matching text would be guesswork the moment a
phrase repeats, which in a tutorial is constantly.

## Reprocessing an improved contract

```sh
sh run.sh ingest reprocess <document-id>
sh run.sh ingest reprocess-contract <contract-id>   # only what is out of date
```

The second one is what `contract_version` on every chunk exists for: after
improving a contract, only the documents still on an older version are rebuilt.
Without it, every improvement costs a full reindex, which is the point at which
systems like this stop being improved.

## Embeddings are optional

Without `INGEST_EMBEDDING_URL` the worker still produces chunks and full text
search vectors, so the stack is useful on a machine with no model server.
Those chunks carry no vector. Once an endpoint exists:

```sh
sh run.sh ingest reembed <tenant-id>
```

The endpoint must be OpenAI-compatible: `POST` with `{model, input}`, answering
`{data: [{embedding: [...]}]}`. Its width has to match the `vector(1024)`
column — a model of another width needs a schema change, not just a different
number in `.env`.

## Things worth knowing before relying on it

**The same bytes are ingested once.** Deduplication is on the sha256 of the
payload, per tenant. Re-sending yesterday's export nightly queues nothing and
reports `"created": false`. To genuinely reprocess a document, create a new
generation rather than pushing it again.

**Credentials cannot be put in `connector_config`.** The database rejects a
source whose config carries a key named like a secret. Use `credential_ref` and
keep the secret in Vault.

**Deleting a raw document deletes everything derived from it**, through the
foreign keys. Files in storage are not reached by that cascade and are removed
by `run.sh ingest prune`, which applies each tenant's `retention_days`.

**Row level security is on from the first migration.** The service connects as
a role that sees every tenant and sets `ingest.tenant_id` per request; every
other role, including anything arriving through PostgREST, is confined to one
tenant. The `ingest` schema is not exposed through PostgREST by default.

**A scanned PDF fails on purpose.** There is no text layer to extract, and
storing an empty document that silently never matches a search is worse than a
job that says "this needs OCR".

## Tests

```sh
sh run.sh ingest test
# or: cd docker/ingest && python3 -m unittest discover -s . -v
```

No database and no network needed. What they cover is what can be wrong
without either: the escaping that keeps customer content out of SQL text, the
chunking that decides retrieval quality, and the request handling that decides
what reaches the database at all.
