#!/usr/bin/env python3
"""
Supabase Advanced — ingest layer, worker.

Takes accepted documents and turns them into chunks a retrieval system can
answer from. What it does to a document is not written here — it is read from
the document's contract, so a new kind of data is a row in `ingest_contract`
rather than a new code path.

Run with:  python3 worker.py

The loop is deliberately plain: claim one job, do the whole pipeline for it,
record the outcome. Concurrency comes from running several containers, which
is safe because a job is claimed with `FOR UPDATE SKIP LOCKED` — two workers
cannot pick up the same one.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import fetcher
import storage
from db import Db, DatabaseError

LOG = logging.getLogger("ingest.worker")

SQL_DIR = Path(__file__).resolve().parent / "sql"


class ExtractionError(RuntimeError):
    """The document could not be turned into text."""


class PermanentError(RuntimeError):
    """This will fail identically every time — do not retry it.

    A video with no captions, a PDF that is only scanned images, a document
    that vanished. Retrying these five times with growing backoff spends the
    retry budget, fills the log, and in the YouTube case spends proxy requests,
    all to learn the same thing again.
    """


# --------------------------------------------------------------------------
# Text extraction
# --------------------------------------------------------------------------


def extract_text(media_type: str, data: bytes) -> str:
    """Turns a raw payload into plain text.

    PDFs go through poppler's `pdftotext` rather than a Python library, for the
    same reason Postgres is reached through `psql`: it is one apt package with
    no dependency tree, and it is the same tool whose output can be reproduced
    by hand when a document extracts badly.
    """
    if media_type == "application/pdf":
        return _pdftotext(data)
    if media_type.startswith("text/") or media_type in (
        "application/json",
        "application/xml",
        "",
    ):
        return data.decode("utf-8", errors="replace")
    raise PermanentError(f"no extractor for media type {media_type!r}")


def _pdftotext(data: bytes) -> str:
    cmd = [
        "pdftotext",
        # Keeps reading order sane on multi-column layouts, which is most
        # reports and nearly every scientific paper.
        "-layout",
        "-enc", "UTF-8",
        "-", "-",
    ]
    try:
        proc = subprocess.run(
            cmd, input=data, capture_output=True, timeout=120
        )
    except FileNotFoundError as exc:
        raise ExtractionError("pdftotext not installed") from exc
    except subprocess.TimeoutExpired as exc:
        raise ExtractionError("pdftotext timed out after 120s") from exc
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", errors="replace").strip()
        raise ExtractionError(detail or f"pdftotext exited {proc.returncode}")
    text = proc.stdout.decode("utf-8", errors="replace")
    if not text.strip():
        # A PDF of scanned pages parses fine and yields nothing. Saying so is
        # more useful than storing an empty document that silently never
        # matches a search. Permanent, because the same file will extract to
        # the same nothing on every retry — it needs OCR, not patience.
        raise PermanentError(
            "no text layer found — the PDF is probably scanned images and "
            "needs OCR before it can be ingested"
        )
    return text


# --------------------------------------------------------------------------
# Chunking
# --------------------------------------------------------------------------
#
# Pure functions on purpose: this is the part whose behaviour decides retrieval
# quality, so it has to be testable without a database.

_PARAGRAPH_SPLIT = re.compile(r"\n\s*\n+")
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")
_WHITESPACE = re.compile(r"[ \t]+")


def normalise(text: str) -> str:
    """Collapses the whitespace damage that PDF extraction leaves behind."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # Words split across a line break by hyphenation.
    text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)
    text = _WHITESPACE.sub(" ", text)
    # Three or more blank lines carry no more meaning than one blank line.
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def chunk_text(text: str, strategy: dict) -> list[str]:
    """Splits text according to a contract's chunk strategy."""
    mode = strategy.get("mode", "paragraph")
    size = max(int(strategy.get("size", 1200)), 1)
    overlap = max(int(strategy.get("overlap", 0)), 0)
    # An overlap at or above the window size would re-emit what it just
    # emitted and never advance. Clamping is better than rejecting: the
    # contract still works, it just stops overlapping quite so eagerly.
    if overlap >= size:
        overlap = size // 4

    text = normalise(text)
    if not text:
        return []

    if mode == "fixed":
        return _chunk_fixed(text, size, overlap)
    if mode == "sentence":
        return _group(_SENTENCE_SPLIT.split(text), size, overlap, " ")
    return _group(_PARAGRAPH_SPLIT.split(text), size, overlap, "\n\n")


def _chunk_fixed(text: str, size: int, overlap: int) -> list[str]:
    step = size - overlap
    out = []
    for start in range(0, len(text), step):
        piece = text[start:start + size].strip()
        if piece:
            out.append(piece)
        if start + size >= len(text):
            break
    return out


def _group(pieces: list[str], size: int, overlap: int, joiner: str) -> list[str]:
    """Packs natural units into chunks without cutting one in half.

    A unit longer than the window on its own (a page-long paragraph, a table)
    is passed to the fixed splitter rather than emitted oversized, because a
    chunk far larger than the rest skews every similarity score it takes part
    in.
    """
    units = [p.strip() for p in pieces if p.strip()]
    out: list[str] = []
    current: list[str] = []
    length = 0

    for unit in units:
        if len(unit) > size:
            if current:
                out.append(joiner.join(current))
                current, length = [], 0
            out.extend(_chunk_fixed(unit, size, overlap))
            continue
        if length and length + len(joiner) + len(unit) > size:
            out.append(joiner.join(current))
            current, length = _carry_over(current, overlap, joiner)
        current.append(unit)
        length += (len(joiner) if length else 0) + len(unit)

    if current:
        out.append(joiner.join(current))
    return out


def chunk_timed(segments: list[dict], strategy: dict) -> list[dict]:
    """Groups timed caption segments into chunks that keep their time span.

    Spoken text has no paragraphs, so the units here are caption segments and
    the boundaries are sentence ends where there are any. Carrying the span
    through is what lets an answer point back into the video at the second it
    was said — recomputing it afterwards by matching text would be guesswork
    the moment a phrase repeats.
    """
    size = max(int(strategy.get("size", 900)), 1)
    overlap = max(int(strategy.get("overlap", 0)), 0)
    if overlap >= size:
        overlap = size // 4

    out: list[dict] = []
    current: list[dict] = []
    length = 0

    def flush() -> None:
        if not current:
            return
        out.append({
            "content": " ".join(s["text"] for s in current),
            "span": {
                "start_ms": current[0]["start_ms"],
                "end_ms": current[-1]["end_ms"],
            },
        })

    for segment in segments:
        text = (segment.get("text") or "").strip()
        if not text:
            continue
        if length and length + 1 + len(text) > size:
            flush()
            current, length = _carry_over_timed(current, overlap)
        current.append({**segment, "text": text})
        length += (1 if length else 0) + len(text)

    flush()
    return out


def _carry_over_timed(current: list[dict], overlap: int) -> tuple[list[dict], int]:
    if overlap <= 0:
        return [], 0
    kept: list[dict] = []
    total = 0
    for segment in reversed(current):
        if total + len(segment["text"]) > overlap and kept:
            break
        kept.insert(0, segment)
        total += len(segment["text"]) + 1
    return kept, max(total - 1, 0)


def _carry_over(
    current: list[str], overlap: int, joiner: str
) -> tuple[list[str], int]:
    """Keeps the tail of a finished chunk as the head of the next one.

    Overlap exists so a passage split across a boundary is still findable from
    either side.
    """
    if overlap <= 0:
        return [], 0
    kept: list[str] = []
    total = 0
    for unit in reversed(current):
        if total + len(unit) > overlap and kept:
            break
        kept.insert(0, unit)
        total += len(unit) + len(joiner)
    return kept, max(total - len(joiner), 0)


# --------------------------------------------------------------------------
# Embeddings
# --------------------------------------------------------------------------


class Embedder:
    """Calls an OpenAI-compatible embedding endpoint.

    Optional by design. Without one configured the worker still produces
    chunks and full text search vectors, so the stack is useful on a machine
    with no model server; the vectors can be filled in later with
    `run.sh ingest reembed`.
    """

    def __init__(self) -> None:
        env = os.environ.get
        self.url = env("INGEST_EMBEDDING_URL", "").strip()
        self.model = env("INGEST_EMBEDDING_MODEL", "bge-m3")
        self.token = env("INGEST_EMBEDDING_TOKEN", "")
        self.dimensions = int(env("INGEST_EMBEDDING_DIMENSIONS", "1024"))
        self.batch = int(env("INGEST_EMBEDDING_BATCH", "16"))
        self.timeout = int(env("INGEST_EMBEDDING_TIMEOUT", "120"))

    @property
    def enabled(self) -> bool:
        return bool(self.url)

    def embed(self, texts: list[str]) -> list[list[float]]:
        out: list[list[float]] = []
        for start in range(0, len(texts), self.batch):
            out.extend(self._embed_batch(texts[start:start + self.batch]))
        return out

    def _embed_batch(self, texts: list[str]) -> list[list[float]]:
        body = json.dumps({"model": self.model, "input": texts}).encode("utf-8")
        req = urllib.request.Request(self.url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                parsed = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:300]
            raise RuntimeError(f"embedding endpoint said {exc.code}: {detail}")
        except (urllib.error.URLError, socket.timeout, OSError) as exc:
            raise RuntimeError(f"embedding endpoint unreachable: {exc}")

        vectors = [item["embedding"] for item in parsed.get("data", [])]
        if len(vectors) != len(texts):
            raise RuntimeError(
                f"asked for {len(texts)} embeddings, got {len(vectors)}"
            )
        for vec in vectors:
            if len(vec) != self.dimensions:
                # Storing a vector of the wrong width would fail at the column
                # anyway; failing here names the actual cause.
                raise RuntimeError(
                    f"model returned {len(vec)} dimensions, but the chunk "
                    f"column is vector({self.dimensions}). Set "
                    f"INGEST_EMBEDDING_DIMENSIONS to match the model, or "
                    f"pick a model that matches the schema."
                )
        return vectors


def to_pgvector(values: list[float]) -> str:
    """pgvector's text input format."""
    return "[" + ",".join(repr(float(v)) for v in values) + "]"


# --------------------------------------------------------------------------
# The pipeline
# --------------------------------------------------------------------------

CLAIM_SQL = """
with claimed as (
    select id
      from ingest.ingest_job
     where status = 'pending'
       and run_after <= now()
     order by run_after, created_at
     for update skip locked
     limit 1
)
update ingest.ingest_job j
   set status     = 'running',
       attempts   = j.attempts + 1,
       locked_at  = now(),
       locked_by  = (select data->>'worker' from _payload),
       updated_at = now()
  from claimed c
 where j.id = c.id
returning (select json_agg(row_to_json(x)) from (
    select j.id, j.tenant_id, j.raw_document_id, j.source_id, j.stage,
           j.attempts, j.max_attempts, j.payload
) x)
"""

# What a fetch or discover job needs to know about its source: the connector
# configuration, and where to find the credential — never the credential.
SOURCE_SQL = """
select coalesce(json_agg(row_to_json(x)), '[]'::json) from (
    select s.id, s.tenant_id, s.source_type_key, s.connector_config,
           s.credential_ref, s.display_name,
           c.id as contract_id, c.chunk_strategy
      from ingest.source s
      join ingest.ingest_contract c on c.id = s.ingest_contract_id
     where s.id = ((select data->>'source_id' from _payload))::uuid
) x
"""

# Stores a fetched transcript as a raw document and queues its extraction.
# One statement, so a transcript can never exist without the job that turns it
# into chunks.
STORE_FETCHED_SQL = """
with existing as (
    select id, false as created
      from ingest.raw_document
     where tenant_id = ((select data->>'tenant' from _payload))::uuid
       and content_hash = (select data->>'hash' from _payload)
),
inserted as (
    insert into ingest.raw_document
        (tenant_id, source_id, external_id, source_uri, content_hash,
         raw_payload, media_type, byte_size)
    select (data->>'tenant')::uuid, (data->>'source')::uuid,
           data->>'external_id', data->>'source_uri', data->>'hash',
           data->'payload', 'application/json',
           length(data->'payload'->>'text')
      from _payload
     where not exists (select 1 from existing)
    returning id, true as created
),
document as (
    select * from inserted union all select * from existing
),
job as (
    insert into ingest.ingest_job
        (tenant_id, source_id, raw_document_id, stage, idempotency_key)
    select (p.data->>'tenant')::uuid, (p.data->>'source')::uuid, d.id,
           'extract',
           (p.data->>'hash') || ':' || (p.data->>'contract') || ':extract'
      from document d, _payload p
    on conflict (idempotency_key) do nothing
    returning id
)
select json_build_array(json_build_object(
    'raw_document_id', (select id from document),
    'created',         (select created from document),
    'job_id',          (select id from job)
))
"""

# One fetch job per discovered video. `on conflict do nothing` means a playlist
# that grew by three videos queues three jobs, not the whole playlist again.
QUEUE_FETCH_SQL = """
insert into ingest.ingest_job
    (tenant_id, source_id, stage, payload, idempotency_key)
select (p.data->>'tenant')::uuid,
       (p.data->>'source')::uuid,
       'fetch',
       jsonb_build_object('video_id', v.value),
       (p.data->>'source') || ':fetch:' || v.value
  from _payload p,
       jsonb_array_elements_text(p.data->'videos') v
on conflict (idempotency_key) do nothing
"""

LOAD_SQL = """
select coalesce(json_agg(row_to_json(x)), '[]'::json) from (
    select d.id            as raw_document_id,
           d.tenant_id,
           d.storage_ref,
           d.raw_payload,
           d.media_type,
           d.source_uri,
           c.id            as contract_id,
           c.version       as contract_version,
           c.chunk_strategy,
           c.metadata_mapping,
           c.embedding_profile,
           c.fts_config,
           c.quality_gates,
           c.domain_tag
      from ingest.raw_document d
      join ingest.source        s on s.id = d.source_id
      join ingest.ingest_contract c on c.id = s.ingest_contract_id
     where d.id = ((select data->>'raw_document_id' from _payload))::uuid
) x
"""


class Worker:
    def __init__(self) -> None:
        self.db = Db()
        self.embedder = Embedder()
        self.name = os.environ.get("INGEST_WORKER_NAME") or socket.gethostname()
        self.idle_sleep = float(os.environ.get("INGEST_POLL_SECONDS", "3"))
        # How long a claimed job may go untouched before another worker takes
        # it back. Must exceed the longest a single job can legitimately run,
        # or a slow document would be processed twice in parallel.
        self.stall_minutes = int(os.environ.get("INGEST_STALL_MINUTES", "30"))
        self.sweep_interval = float(os.environ.get("INGEST_SWEEP_SECONDS", "300"))
        # The proxy the proxied providers use. Empty means those providers
        # cannot run, and the chain falls through to the direct ones.
        self.proxy = os.environ.get("INGEST_PROXY_URL", "").strip()
        self.running = True

    # ------------------------------------------------------------------
    # Loop
    # ------------------------------------------------------------------

    def run_forever(self) -> None:
        LOG.info("worker %s started", self.name)
        next_sweep = 0.0
        while self.running:
            try:
                # A worker killed mid-job leaves its row claimed, and a claimed
                # row is never picked up again — the document would silently
                # never finish. Any worker returns those to the queue.
                if time.monotonic() >= next_sweep:
                    self.requeue_stalled()
                    next_sweep = time.monotonic() + self.sweep_interval
                did_work = self.run_once()
            except DatabaseError as exc:
                LOG.error("database unavailable: %s", exc)
                time.sleep(min(self.idle_sleep * 5, 30))
                continue
            if not did_work:
                time.sleep(self.idle_sleep)

    def requeue_stalled(self) -> None:
        revived = self.db.query(
            "select json_build_array(ingest.requeue_stalled("
            "  ((select data->>'minutes' from _payload))::int))",
            {"minutes": self.stall_minutes},
        )
        count = revived[0] if isinstance(revived, list) else 0
        if count:
            LOG.warning("requeued %s job(s) abandoned by a vanished worker", count)

    def run_once(self) -> bool:
        """Claims and processes one job. Returns False when there was none."""
        claimed = self.db.query(CLAIM_SQL, {"worker": self.name})
        if not claimed:
            return False
        job = claimed[0]
        LOG.info(
            "job %s claimed, stage %s (attempt %s)",
            job["id"], job["stage"], job["attempts"],
        )
        try:
            if job["stage"] == "discover":
                summary = self.discover(job)
            elif job["stage"] == "fetch":
                summary = self.fetch(job)
            else:
                summary = self.process(job)
        except Exception as exc:  # noqa: BLE001 — the outcome is recorded, not swallowed
            self.fail(job, exc)
            return True
        self.finish(job, summary)
        return True

    # ------------------------------------------------------------------
    # Discovering work
    # ------------------------------------------------------------------

    def discover(self, job: dict) -> dict:
        """Turns a playlist or a channel into one fetch job per video.

        Deliberately a separate stage: listing a playlist fails for entirely
        different reasons than fetching a transcript (quota, a private
        playlist), and folding the two together would make one video's
        problem look like the whole source failing.
        """
        source = self.load_source(job["source_id"])
        config = source.get("connector_config") or {}
        api_key = self.credential(source)
        limit = int(config.get("max_videos") or 500)

        kind = source["source_type_key"]
        if kind == "youtube_playlist":
            videos = fetcher.list_playlist(api_key, config["playlist_id"], limit)
        elif kind == "youtube_channel":
            videos = fetcher.list_channel(api_key, config["channel_id"], limit)
        else:
            raise RuntimeError(f"source type {kind!r} has nothing to discover")

        if not videos:
            return {"videos": 0, "note": "nothing found to fetch"}

        self.db.execute(
            QUEUE_FETCH_SQL,
            {
                "tenant": source["tenant_id"],
                "source": source["id"],
                "videos": videos,
            },
            timeout=120,
        )
        LOG.info("discovered %s video(s) for source %s", len(videos), source["id"])
        return {"videos": len(videos)}

    def fetch(self, job: dict) -> dict:
        """Fetches one transcript, walking the provider chain.

        The chain is the difference between "a blocked route is an outage" and
        "a blocked route costs a little more". Each provider is tried in turn;
        a provider that fails three times in a row is taken out of the chain
        until someone puts it back.
        """
        source = self.load_source(job["source_id"])
        config = source.get("connector_config") or {}
        video_id = (job.get("payload") or {}).get("video_id") or config.get("video_id")
        if not video_id:
            raise RuntimeError("fetch job carries no video id")

        language = config.get("language")
        api_key = self.credential(source)

        chain = self.db.query(
            "select coalesce(json_agg(row_to_json(x)), '[]'::json) from ("
            "  select key, use_proxy, clients from ingest.transcript_chain()) x"
        )
        if not chain:
            raise RuntimeError(
                "every transcript provider is disabled or marked down — "
                "check `run.sh ingest providers`"
            )

        transcript = None
        attempts = []
        for provider in chain:
            try:
                transcript = fetcher.fetch_via(
                    provider, video_id, language, self.proxy
                )
            except fetcher.NoCaptionsError as exc:
                # The video answered and has none. No other route invents
                # captions, and marking a provider down for it would take a
                # working route out of service. Nor is it worth retrying: it
                # would cost proxy requests to be told the same thing again.
                raise PermanentError(str(exc)) from exc
            except fetcher.TranscriptError as exc:
                attempts.append(f"{provider['key']}: {exc}")
                health = self.record_provider(provider["key"], False, str(exc))
                LOG.warning(
                    "provider %s failed for %s (now %s)",
                    provider["key"], video_id, health,
                )
                continue
            self.record_provider(provider["key"], True)
            break

        if transcript is None:
            raise RuntimeError(
                f"no provider could fetch {video_id}: " + "; ".join(attempts)
            )

        metadata = fetcher.video_metadata(api_key, video_id)
        payload = {
            "text": transcript["text"],
            "segments": transcript["segments"],
            "metadata": metadata,
            "language": transcript["language"],
            "automatic_captions": transcript["automatic"],
            "fetched_via": transcript["client"],
        }
        # Hashing the transcript, not the metadata: a view count that ticked up
        # overnight must not look like a changed document.
        content_hash = hashlib.sha256(
            transcript["text"].encode("utf-8")
        ).hexdigest()

        stored = self.db.query(
            STORE_FETCHED_SQL,
            {
                "tenant": source["tenant_id"],
                "source": source["id"],
                "contract": source["contract_id"],
                "external_id": video_id,
                "source_uri": f"https://www.youtube.com/watch?v={video_id}",
                "hash": content_hash,
                "payload": payload,
            },
            timeout=180,
        )
        result = stored[0]
        LOG.info(
            "fetched %s via %s (%s segments)%s",
            video_id, transcript["client"], len(transcript["segments"]),
            "" if result["created"] else " — unchanged, not re-ingested",
        )
        return {
            "video_id": video_id,
            "provider_client": transcript["client"],
            "segments": len(transcript["segments"]),
            "created": result["created"],
        }

    def load_source(self, source_id: str | None) -> dict:
        if not source_id:
            raise RuntimeError("job has no source")
        rows = self.db.query(SOURCE_SQL, {"source_id": source_id})
        if not rows:
            raise RuntimeError(f"source {source_id} no longer exists")
        return rows[0]

    @staticmethod
    def credential(source: dict) -> str:
        """Resolves a source's credential reference to an actual secret.

        The reference is stored, never the secret — that is the difference
        between a product and a liability once customers put their own keys in.
        Stage 1 resolves `env:NAME` against the process environment; a Vault
        backend would be another prefix here and no change anywhere else.
        """
        ref = (source.get("credential_ref") or "").strip()
        if not ref:
            value = os.environ.get("INGEST_YOUTUBE_API_KEY", "")
            if not value:
                raise RuntimeError(
                    "this source needs a YouTube Data API key. Set "
                    "INGEST_YOUTUBE_API_KEY, or point the source's "
                    "credential_ref at another variable with 'env:NAME'."
                )
            return value
        if ref.startswith("env:"):
            name = ref[4:]
            value = os.environ.get(name, "")
            if not value:
                raise RuntimeError(
                    f"credential_ref points at {name}, which is not set in "
                    "the worker's environment"
                )
            return value
        raise RuntimeError(
            f"unsupported credential reference {ref!r} — expected 'env:NAME'"
        )

    def record_provider(self, key: str, ok: bool, detail: str | None = None) -> str:
        health = self.db.query(
            "select json_build_array(ingest.record_provider_result("
            "  (select data->>'k' from _payload),"
            "  ((select data->>'ok' from _payload))::boolean,"
            "  (select data->>'detail' from _payload)))",
            {"k": key, "ok": ok, "detail": detail},
        )
        return health[0] if isinstance(health, list) else "unknown"

    # ------------------------------------------------------------------
    # One document
    # ------------------------------------------------------------------

    def process(self, job: dict) -> dict:
        rows = self.db.query(
            LOAD_SQL, {"raw_document_id": job["raw_document_id"]}
        )
        if not rows:
            raise RuntimeError(
                f"raw document {job['raw_document_id']} vanished before "
                "processing — it was probably deleted while queued"
            )
        doc = rows[0]

        text = self.load_text(doc)
        gates = doc.get("quality_gates") or {}
        min_chars = int(gates.get("min_chars", 0))
        if len(text.strip()) < min_chars:
            raise RuntimeError(
                f"extracted {len(text.strip())} characters, but the contract "
                f"requires at least {min_chars}"
            )

        strategy = doc.get("chunk_strategy") or {}
        payload = doc.get("raw_payload") or {}
        segments = payload.get("segments") if isinstance(payload, dict) else None
        if segments:
            # Timed material: keep the span so an answer can point back into
            # the recording rather than only at the document.
            chunks = chunk_timed(segments, strategy)
        else:
            chunks = [{"content": c, "span": None}
                      for c in chunk_text(text, strategy)]
        if not chunks:
            raise RuntimeError("extraction produced no chunks")

        generation = self.next_generation(doc)
        record_id = self.write_record(doc, text, generation)
        vectors = self.embed_chunks(doc, [c["content"] for c in chunks])
        self.write_chunks(doc, record_id, generation, chunks, vectors)

        LOG.info(
            "job %s: %s chunks, generation %s%s",
            job["id"], len(chunks), generation,
            "" if vectors else " (no embeddings)",
        )
        return {
            "chunks": len(chunks),
            "generation": generation,
            "embedded": bool(vectors),
        }

    def load_text(self, doc: dict) -> str:
        """Gets the document's text, from wherever the payload was put."""
        if doc.get("storage_ref"):
            data = storage.read(doc["storage_ref"])
            return extract_text(doc.get("media_type") or "", data)
        payload = doc.get("raw_payload")
        if payload is None:
            raise ExtractionError("document has neither a payload nor a stored file")
        if isinstance(payload, str):
            return payload
        if isinstance(payload, dict) and isinstance(payload.get("text"), str):
            return payload["text"]
        # Anything else is indexed as its JSON form, which at least makes the
        # values searchable rather than silently dropping the document.
        return json.dumps(payload, ensure_ascii=False, indent=2)

    def next_generation(self, doc: dict) -> int:
        result = self.db.query(
            "select coalesce(max(generation), 0) + 1 as g "
            "  from ingest.structured_record"
            " where raw_document_id = ((select data->>'d' from _payload))::uuid"
            "   and ingest_contract_id = ((select data->>'c' from _payload))::uuid"
            " limit 1",
            {"d": doc["raw_document_id"], "c": doc["contract_id"]},
        )
        # `query` decodes JSON; a bare number comes back as a number.
        if isinstance(result, list) and result:
            return int(result[0]["g"])
        return int(result) if result else 1

    def write_record(self, doc: dict, text: str, generation: int) -> str:
        rows = self.db.query(
            """
            insert into ingest.structured_record
                (tenant_id, raw_document_id, ingest_contract_id,
                 contract_version, generation, extracted, text_content)
            select (data->>'tenant')::uuid,
                   (data->>'doc')::uuid,
                   (data->>'contract')::uuid,
                   (data->>'version')::int,
                   (data->>'generation')::int,
                   data->'extracted',
                   data->>'text'
              from _payload
            returning json_build_array(json_build_object('id', id))
            """,
            {
                "tenant": doc["tenant_id"],
                "doc": doc["raw_document_id"],
                "contract": doc["contract_id"],
                "version": doc["contract_version"],
                "generation": generation,
                "extracted": self.extracted_fields(doc, text),
                "text": text,
            },
            timeout=120,
        )
        return rows[0][0]["id"] if isinstance(rows[0], list) else rows[0]["id"]

    @staticmethod
    def extracted_fields(doc: dict, text: str) -> dict:
        """The structured side of a record.

        Stage 1 carries through what the fetcher already knows — for a video
        that is title, channel, publication date and duration. Running the
        contract's `extraction_prompt` against `extraction_schema` to pull
        fields out of the text itself is stage 2; the column is filled with
        what is known rather than left empty so the shape is already right.
        """
        fields = {
            "char_count": len(text),
            "source_uri": doc.get("source_uri"),
        }
        payload = doc.get("raw_payload")
        if isinstance(payload, dict):
            metadata = payload.get("metadata")
            if isinstance(metadata, dict):
                fields.update({k: v for k, v in metadata.items() if v is not None})
            if payload.get("language"):
                fields["language"] = payload["language"]
            if payload.get("automatic_captions") is not None:
                fields["automatic_captions"] = payload["automatic_captions"]
        return fields

    def embed_chunks(self, doc: dict, chunks: list[str]) -> list[str]:
        profile = doc.get("embedding_profile") or "none"
        if profile == "none" or not self.embedder.enabled:
            if profile != "none" and not self.embedder.enabled:
                LOG.warning(
                    "contract asks for embedding profile %r but no "
                    "INGEST_EMBEDDING_URL is set — storing chunks without "
                    "vectors, run `ingest reembed` once one is configured",
                    profile,
                )
            return []
        return [to_pgvector(v) for v in self.embedder.embed(chunks)]

    def write_chunks(
        self,
        doc: dict,
        record_id: str,
        generation: int,
        chunks: list[str],
        vectors: list[str],
    ) -> None:
        """Writes the new generation and switches to it in one transaction.

        The chunks land with `is_current = false`, so a search running at this
        moment keeps seeing the previous generation in full. The switch at the
        end is what makes reprocessing invisible to whoever is querying: there
        is no instant where both generations answer, and no instant where
        neither does.
        """
        profile = doc.get("embedding_profile") or "none"
        meta = dict(doc.get("metadata_mapping") or {})
        if doc.get("domain_tag"):
            meta.setdefault("domain_tag", doc["domain_tag"])

        rows = []
        for seq, chunk in enumerate(chunks):
            span = chunk.get("span")
            rows.append([
                doc["tenant_id"],
                record_id,
                doc["raw_document_id"],
                doc["contract_id"],
                str(doc["contract_version"]),
                str(generation),
                str(seq),
                chunk["content"],
                vectors[seq] if vectors else None,
                profile if vectors else None,
                json.dumps(meta, ensure_ascii=False),
                json.dumps(span) if span else None,
            ])

        copy_statement = (
            "copy ingest.chunk (tenant_id, structured_record_id, "
            "raw_document_id, ingest_contract_id, contract_version, "
            "generation, seq, content, embedding, embedding_profile, meta, "
            "span) from stdin"
        )

        # The text search vector is computed in the database, not in Python:
        # to_tsvector is by definition the function the index agrees with.
        #
        # The configuration name comes from the contract and is cast to
        # `regconfig` rather than pasted into the statement. The cast happens
        # at run time against the configurations the server actually has, so an
        # unknown name is a clean error and not an opening.
        follow_up = """
        update ingest.chunk
           set fts = to_tsvector(
                   (select data->>'fts' from _payload)::regconfig, content)
         where structured_record_id = ((select data->>'record' from _payload))::uuid
           and generation = ((select data->>'generation' from _payload))::int;

        update ingest.chunk
           set is_current =
                   (generation = ((select data->>'generation' from _payload))::int)
         where raw_document_id = ((select data->>'doc' from _payload))::uuid
           and ingest_contract_id = ((select data->>'contract' from _payload))::uuid;
        """
        self.db.copy_rows(
            copy_statement,
            rows,
            follow_up=follow_up,
            payload={
                "fts": (doc.get("fts_config") or "simple"),
                "record": record_id,
                "generation": generation,
                "doc": doc["raw_document_id"],
                "contract": doc["contract_id"],
            },
            timeout=300,
        )

    # ------------------------------------------------------------------
    # Outcomes
    # ------------------------------------------------------------------

    def finish(self, job: dict, summary: dict) -> None:
        # `stage` is deliberately left alone. It says what this job *is* — a
        # fetch, a discovery, an extraction — and how far it got is `status`.
        # Overwriting it on success made every finished job look like an
        # extraction, so "how many fetches ran" became unanswerable.
        self.db.execute(
            """
            update ingest.ingest_job
               set status = 'done', last_error = null,
                   payload = payload || (select data->'summary' from _payload)
             where id = ((select data->>'id' from _payload))::uuid
            """,
            {"id": job["id"], "summary": summary},
        )

    def fail(self, job: dict, exc: Exception) -> None:
        """Records a failure and decides whether it is worth another try.

        Backoff is exponential and capped. A job that has used up its attempts
        becomes 'dead' rather than 'failed', so the two questions "what is
        currently struggling" and "what has given up" stay separable.
        """
        attempts = int(job["attempts"])
        limit = int(job["max_attempts"])
        message = f"{type(exc).__name__}: {exc}"
        LOG.error("job %s failed (%s/%s): %s", job["id"], attempts, limit, message)

        if isinstance(exc, PermanentError):
            # Nothing about a second attempt would differ. Recording it as
            # dead immediately keeps the retry budget for problems that can
            # actually resolve themselves.
            status, delay = "dead", 0
        elif attempts >= limit:
            status, delay = "dead", 0
        else:
            status, delay = "pending", min(2 ** attempts * 10, 900)

        self.db.execute(
            """
            update ingest.ingest_job
               set status     = data.status,
                   last_error = data.err,
                   run_after  = now() + make_interval(secs => data.delay),
                   locked_at  = null,
                   locked_by  = null,
                   updated_at = now()
              from (select p.data->>'status' as status,
                           p.data->>'err'    as err,
                           (p.data->>'delay')::int as delay,
                           (p.data->>'id')::uuid   as id
                      from _payload p) data
             where ingest_job.id = data.id
            """,
            {
                "id": job["id"],
                "status": status,
                "err": message[:4000],
                "delay": delay,
            },
        )


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("INGEST_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    worker = Worker()

    def stop(signum, _frame):
        LOG.info("signal %s received, finishing current job", signum)
        worker.running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    worker.db.wait_until_ready()
    worker.db.migrate(SQL_DIR)
    worker.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
