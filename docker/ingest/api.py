#!/usr/bin/env python3
"""
Supabase Advanced — ingest layer, HTTP API.

One endpoint accepts documents. Which pipeline runs, what gets extracted and
how it is cut into passages is decided by the source's contract, not by the
route — so a new kind of data needs a row in the database, not a new endpoint.

Endpoints
  GET  /health              200 always, JSON status — no authentication
  POST /ingest              accept a document, queue it        (auth)
  GET  /sources             configured sources of a tenant     (auth)
  GET  /templates           contract templates shipped         (auth)
  GET  /jobs                recent jobs and their state        (auth)
  POST /search              full text search over chunks       (auth)

Authenticated endpoints expect `Authorization: Bearer $INGEST_API_TOKEN`.

The token authenticates the caller as the service operator, not as a tenant:
which tenant a request acts on comes from the `source_id` when ingesting, and
from an explicit `tenant` field otherwise. Per-tenant credentials are a
separate concern from getting documents in, and belong with the customer-facing
surface rather than here.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import storage
from db import Db, DatabaseError, is_uuid

LOG = logging.getLogger("ingest.api")

SQL_DIR = Path(__file__).resolve().parent / "sql"

# A single document is capped so one caller cannot exhaust the disk or park a
# gigabyte in memory. Well above any ordinary PDF; raise it deliberately.
MAX_BODY_BYTES = int(os.environ.get("INGEST_MAX_BODY_MB", "64")) * 1024 * 1024


class ApiError(Exception):
    """An error with an HTTP status to report it under."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


# --------------------------------------------------------------------------
# Statements
# --------------------------------------------------------------------------

RESOLVE_SOURCE_SQL = """
select coalesce(json_agg(row_to_json(x)), '[]'::json) from (
    select s.id, s.tenant_id, s.status, s.display_name,
           c.id as contract_id, c.version as contract_version
      from ingest.source s
      join ingest.ingest_contract c on c.id = s.ingest_contract_id
     where s.id = ((select data->>'source_id' from _payload))::uuid
) x
"""

# One statement does the whole acceptance: deduplicate, insert if new, queue a
# job. Doing it in one transaction is what makes a repeated submission a no-op
# instead of a race between two callers checking and then both inserting.
ACCEPT_SQL = """
with existing as (
    select id, false as created
      from ingest.raw_document
     where tenant_id = ((select data->>'tenant' from _payload))::uuid
       and content_hash = (select data->>'hash' from _payload)
),
inserted as (
    insert into ingest.raw_document
        (tenant_id, source_id, external_id, source_uri, content_hash,
         storage_ref, raw_payload, media_type, byte_size)
    select (data->>'tenant')::uuid, (data->>'source')::uuid,
           data->>'external_id', data->>'source_uri', data->>'hash',
           data->>'storage_ref', data->'raw_payload', data->>'media_type',
           (data->>'byte_size')::bigint
      from _payload
     where not exists (select 1 from existing)
    returning id, true as created
),
document as (
    select * from inserted union all select * from existing
),
job as (
    insert into ingest.ingest_job
        (tenant_id, source_id, raw_document_id, stage, payload,
         idempotency_key)
    select (p.data->>'tenant')::uuid, (p.data->>'source')::uuid, d.id,
           'extract', jsonb_build_object('accepted_at', now()),
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

SEARCH_SQL = """
with q as (
    select websearch_to_tsquery(
               coalesce((select data->>'config' from _payload), 'simple')::regconfig,
               (select data->>'query' from _payload)
           ) as query
)
select coalesce(json_agg(row_to_json(x)), '[]'::json) from (
    select ch.id,
           ch.content,
           ch.meta,
           ch.span,
           ch.seq,
           ch.contract_version,
           d.source_uri,
           round(ts_rank_cd(ch.fts, q.query)::numeric, 6) as rank
      from ingest.chunk ch
      join ingest.raw_document d on d.id = ch.raw_document_id
      cross join q
     where ch.tenant_id = ((select data->>'tenant' from _payload))::uuid
       and ch.is_current
       and ch.fts @@ q.query
     order by ts_rank_cd(ch.fts, q.query) desc, ch.seq
     limit greatest(least(coalesce((select (data->>'limit')::int from _payload), 10), 100), 1)
) x
"""


# --------------------------------------------------------------------------
# Service
# --------------------------------------------------------------------------


class Service:
    """Everything the handler needs, so the handler stays about HTTP."""

    def __init__(self) -> None:
        self.db = Db()
        self.token = os.environ.get("INGEST_API_TOKEN", "")

    def authorised(self, header: str | None) -> bool:
        if not self.token:
            # Refusing everything is the safe answer to "no token configured".
            # Serving everything would turn a missing variable into an open
            # write endpoint.
            return False
        if not header or not header.startswith("Bearer "):
            return False
        return hmac.compare_digest(header[7:].strip(), self.token)

    # ------------------------------------------------------------------

    def ingest(self, body: dict) -> dict:
        source_id = str(body.get("source_id") or "").strip()
        if not is_uuid(source_id):
            raise ApiError(400, "source_id must be a UUID")

        rows = self.db.query(RESOLVE_SOURCE_SQL, {"source_id": source_id})
        if not rows:
            raise ApiError(404, f"no source with id {source_id}")
        source = rows[0]
        if source["status"] != "active":
            raise ApiError(
                409,
                f"source {source['display_name']!r} is {source['status']}, "
                "so it is not accepting documents",
            )

        data, media_type, raw_payload = self._payload_of(body)
        content_hash = hashlib.sha256(data).hexdigest()

        storage_ref = None
        if raw_payload is None:
            storage_ref = storage.store(
                source["tenant_id"], content_hash, data
            )

        result = self.db.query(
            ACCEPT_SQL,
            {
                "tenant": source["tenant_id"],
                "source": source_id,
                "contract": source["contract_id"],
                "hash": content_hash,
                "external_id": body.get("external_id"),
                "source_uri": body.get("source_uri"),
                "storage_ref": storage_ref,
                "raw_payload": raw_payload,
                "media_type": media_type,
                "byte_size": len(data),
            },
            timeout=120,
        )
        accepted = result[0]
        return {
            "raw_document_id": accepted["raw_document_id"],
            "job_id": accepted["job_id"],
            # False means these exact bytes were already ingested for this
            # tenant. Reported rather than hidden: a caller re-sending
            # everything nightly should be able to see that nothing was new.
            "created": accepted["created"],
            "content_hash": content_hash,
        }

    @staticmethod
    def _payload_of(body: dict) -> tuple[bytes, str, str | None]:
        """Reads the document out of the request in either accepted form.

        Text goes into the database as it is; binaries are base64 in the
        request and end up as a file, because keeping megabytes per row makes
        every backup and every replica carry content nothing queries.
        """
        if "content_base64" in body:
            try:
                data = base64.b64decode(body["content_base64"], validate=True)
            except (binascii.Error, ValueError) as exc:
                raise ApiError(400, f"content_base64 is not valid base64: {exc}")
            media_type = body.get("media_type") or "application/octet-stream"
            return data, media_type, None

        if "text" in body:
            text = body["text"]
            if not isinstance(text, str):
                raise ApiError(400, "text must be a string")
            if not text.strip():
                raise ApiError(400, "text is empty")
            return text.encode("utf-8"), "text/plain", text

        if "payload" in body:
            encoded = json.dumps(body["payload"], ensure_ascii=False, sort_keys=True)
            return encoded.encode("utf-8"), "application/json", body["payload"]

        raise ApiError(
            400, "send one of: text, payload, or content_base64"
        )

    # ------------------------------------------------------------------

    def sources(self, tenant: str) -> list:
        return self.db.query(
            """
            select coalesce(json_agg(row_to_json(x)), '[]'::json) from (
                select s.id, s.display_name, s.source_type_key, s.status,
                       s.last_run_at, s.last_error,
                       c.name as contract, c.version as contract_version,
                       c.template_key, c.template_version
                  from ingest.source s
                  join ingest.ingest_contract c on c.id = s.ingest_contract_id
                 where s.tenant_id = ((select data->>'tenant' from _payload))::uuid
                 order by s.created_at
            ) x
            """,
            {"tenant": tenant},
        )

    def templates(self) -> list:
        return self.db.query(
            """
            select coalesce(json_agg(row_to_json(x)), '[]'::json) from (
                select distinct on (key)
                       key, version, display_name, description,
                       source_type_key, requires
                  from ingest.ingest_contract_template
                 order by key, version desc
            ) x
            """
        )

    def jobs(self, tenant: str, status: str | None, limit: int) -> list:
        return self.db.query(
            """
            select coalesce(json_agg(row_to_json(x)), '[]'::json) from (
                select j.id, j.stage, j.status, j.attempts, j.max_attempts,
                       j.last_error, j.created_at, j.updated_at, j.run_after,
                       d.source_uri
                  from ingest.ingest_job j
                  left join ingest.raw_document d on d.id = j.raw_document_id
                 where j.tenant_id = ((select data->>'tenant' from _payload))::uuid
                   and (((select data->>'status' from _payload)) is null
                        or j.status = (select data->>'status' from _payload))
                 order by j.created_at desc
                 limit greatest(least(((select data->>'limit' from _payload))::int, 200), 1)
            ) x
            """,
            {"tenant": tenant, "status": status, "limit": limit},
        )

    def search(self, body: dict) -> list:
        tenant = str(body.get("tenant") or "").strip()
        if not is_uuid(tenant):
            raise ApiError(400, "tenant must be a UUID")
        query = str(body.get("query") or "").strip()
        if not query:
            raise ApiError(400, "query is empty")
        return self.db.query(
            SEARCH_SQL,
            {
                "tenant": tenant,
                "query": query,
                # Must match the configuration the chunks were indexed with,
                # otherwise stemming differs and German words stop matching.
                "config": body.get("config") or "simple",
                "limit": int(body.get("limit") or 10),
            },
        )

    def health(self) -> dict:
        try:
            counts = self.db.query(
                """
                select json_build_array(json_build_object(
                    'documents', (select count(*) from ingest.raw_document),
                    'chunks',    (select count(*) from ingest.chunk where is_current),
                    'pending',   (select count(*) from ingest.ingest_job where status = 'pending'),
                    'running',   (select count(*) from ingest.ingest_job where status = 'running'),
                    'dead',      (select count(*) from ingest.ingest_job where status = 'dead')
                ))
                """,
                timeout=10,
            )
            return {"status": "ok", "database": "up", **counts[0]}
        except DatabaseError as exc:
            return {"status": "degraded", "database": "down", "error": str(exc)}


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------


class Handler(BaseHTTPRequestHandler):
    service: Service = None  # type: ignore[assignment]
    server_version = "supabase-ingest/1.0"

    # ------------------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802 — required by BaseHTTPRequestHandler
        route = urlparse(self.path)
        params = parse_qs(route.query)
        try:
            if route.path == "/health":
                self._json(200, self.service.health())
                return
            self._require_auth()
            if route.path == "/templates":
                self._json(200, {"templates": self.service.templates()})
            elif route.path == "/sources":
                tenant = self._tenant_param(params)
                self._json(200, {"sources": self.service.sources(tenant)})
            elif route.path == "/jobs":
                tenant = self._tenant_param(params)
                status = (params.get("status") or [None])[0]
                if status is not None and status not in (
                    "pending", "running", "done", "failed", "dead"
                ):
                    raise ApiError(400, f"unknown status {status!r}")
                limit = int((params.get("limit") or ["50"])[0] or 50)
                self._json(200, {"jobs": self.service.jobs(tenant, status, limit)})
            else:
                raise ApiError(404, f"no route {route.path}")
        except ApiError as exc:
            self._json(exc.status, {"error": exc.message})
        except (DatabaseError, ValueError) as exc:
            LOG.exception("GET %s failed", route.path)
            self._json(500, {"error": str(exc)})

    def do_POST(self) -> None:  # noqa: N802
        route = urlparse(self.path)
        try:
            self._require_auth()
            body = self._body()
            if route.path == "/ingest":
                self._json(202, self.service.ingest(body))
            elif route.path == "/search":
                self._json(200, {"results": self.service.search(body)})
            else:
                raise ApiError(404, f"no route {route.path}")
        except ApiError as exc:
            self._json(exc.status, {"error": exc.message})
        except ValueError as exc:
            # A malformed value in the body (e.g. "limit": "abc") is the
            # caller's mistake, not a server failure.
            self._json(400, {"error": str(exc)})
        except DatabaseError as exc:
            LOG.exception("POST %s failed", route.path)
            self._json(500, {"error": str(exc)})

    # ------------------------------------------------------------------

    def _require_auth(self) -> None:
        if not self.service.authorised(self.headers.get("Authorization")):
            raise ApiError(401, "missing or invalid bearer token")

    @staticmethod
    def _tenant_param(params: dict) -> str:
        tenant = (params.get("tenant") or [""])[0]
        if not is_uuid(tenant):
            raise ApiError(400, "tenant query parameter must be a UUID")
        return tenant

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            raise ApiError(400, "empty request body")
        if length > MAX_BODY_BYTES:
            raise ApiError(
                413,
                f"body is {length} bytes, the limit is {MAX_BODY_BYTES}. "
                "Raise INGEST_MAX_BODY_MB if this is expected.",
            )
        raw = self.rfile.read(length)
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApiError(400, f"body is not valid JSON: {exc}")
        if not isinstance(parsed, dict):
            raise ApiError(400, "body must be a JSON object")
        return parsed

    def _json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:
        # Default goes to stderr unstructured; route it through logging so it
        # lands in `docker compose logs` with everything else.
        LOG.info("%s - %s", self.address_string(), fmt % args)


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("INGEST_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    service = Service()
    if not service.token:
        LOG.error(
            "INGEST_API_TOKEN is not set. Every authenticated endpoint will "
            "refuse. Run `sh run.sh ingest init` to generate one."
        )

    service.db.wait_until_ready()
    applied = service.db.migrate(SQL_DIR)
    if applied:
        LOG.info("applied migrations: %s", ", ".join(applied))

    port = int(os.environ.get("INGEST_PORT", "8010"))
    Handler.service = service
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    LOG.info("ingest api listening on :%s", port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
