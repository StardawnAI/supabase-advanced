#!/usr/bin/env python3
"""
Supabase Advanced — ingest layer, operator commands.

Runs inside the ingest container and is reached through `sh run.sh ingest …`.
The shell wrapper stays thin on purpose: everything that touches the database
is written here, against the same modules the service itself uses, so an
operator command cannot drift from what the service does.

  init          create a tenant and a first source from a shipped template
  templates     list the shipped templates
  sources       list a tenant's sources
  jobs          list recent jobs
  push          ingest a document read from stdin
  search        full text search over a tenant's current chunks
  reembed       fill in vectors for chunks stored without them
  prune         delete documents past their tenant's retention
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetcher  # noqa: E402
import storage  # noqa: E402
from api import Service  # noqa: E402
from db import Db  # noqa: E402
from worker import Embedder, to_pgvector  # noqa: E402


def out(value) -> None:
    print(json.dumps(value, indent=2, default=str))


# --------------------------------------------------------------------------
# init
# --------------------------------------------------------------------------


def cmd_init(args, db: Db, service: Service) -> int:
    """Creates a tenant, a contract from a template, and a source.

    Written as one statement per object rather than one big script so a
    partially completed init can be re-run: the tenant is matched by name and
    reused, and so is the contract.
    """
    rows = db.query(
        """
        select coalesce(json_agg(row_to_json(x)), '[]'::json) from (
            select key, version, definition, source_type_key, display_name
              from ingest.ingest_contract_template
             where key = (select data->>'template' from _payload)
             order by version desc
             limit 1
        ) x
        """,
        {"template": args.template},
    )
    if not rows:
        print(
            f"No template named {args.template!r}. Available:", file=sys.stderr
        )
        for tpl in service.templates():
            print(f"  {tpl['key']:16} {tpl['display_name']}", file=sys.stderr)
        return 1
    template = rows[0]
    definition = template["definition"]

    created = db.query(
        """
        with t as (
            insert into ingest.tenant (name)
            select data->>'tenant' from _payload
            on conflict (name) do update set name = excluded.name
            returning id
        ),
        c as (
            insert into ingest.ingest_contract
                (tenant_id, name, version, source_type_key, domain_tag,
                 template_key, template_version, extraction_schema,
                 chunk_strategy, metadata_mapping, embedding_profile,
                 fts_config, quality_gates)
            select t.id,
                   p.data->>'contract_name',
                   1,
                   p.data->>'source_type',
                   p.data->'definition'->>'domain_tag',
                   p.data->>'template',
                   (p.data->>'template_version')::int,
                   p.data->'definition'->'extraction_schema',
                   p.data->'definition'->'chunk_strategy',
                   p.data->'definition'->'metadata_mapping',
                   p.data->'definition'->>'embedding_profile',
                   p.data->'definition'->>'fts_config',
                   p.data->'definition'->'quality_gates'
              from t, _payload p
            on conflict (tenant_id, name, version) do update
                set updated_at = now()
            returning id, tenant_id
        ),
        s as (
            insert into ingest.source
                (tenant_id, source_type_key, ingest_contract_id, display_name)
            select c.tenant_id, p.data->>'source_type', c.id,
                   p.data->>'source_name'
              from c, _payload p
            returning id
        )
        select json_build_array(json_build_object(
            'tenant_id',   (select tenant_id from c),
            'contract_id', (select id from c),
            'source_id',   (select id from s)
        ))
        """,
        {
            "tenant": args.tenant,
            "contract_name": f"{template['key']}-v{template['version']}",
            "source_name": args.source_name,
            "source_type": template["source_type_key"],
            "template": template["key"],
            "template_version": template["version"],
            "definition": definition,
        },
    )
    result = created[0]
    result["template"] = f"{template['key']} v{template['version']}"
    out(result)
    return 0


# --------------------------------------------------------------------------
# Listing
# --------------------------------------------------------------------------


def cmd_templates(args, db: Db, service: Service) -> int:
    out(service.templates())
    return 0


def cmd_tenants(args, db: Db, service: Service) -> int:
    out(db.query(
        "select coalesce(json_agg(row_to_json(x)), '[]'::json) from ("
        "  select id, name, plan, retention_days, created_at"
        "    from ingest.tenant order by created_at) x"
    ))
    return 0


def cmd_sources(args, db: Db, service: Service) -> int:
    out(service.sources(args.tenant))
    return 0


def cmd_jobs(args, db: Db, service: Service) -> int:
    out(service.jobs(args.tenant, args.status, args.limit))
    return 0


# --------------------------------------------------------------------------
# Documents
# --------------------------------------------------------------------------


def cmd_push(args, db: Db, service: Service) -> int:
    """Ingests a document read from stdin.

    Goes through the same acceptance path as the HTTP endpoint, including
    deduplication, so pushing a file twice queues one job and says so.
    """
    data = sys.stdin.buffer.read()
    if not data:
        print("nothing on stdin", file=sys.stderr)
        return 1

    if args.media_type == "text/plain":
        body = {
            "source_id": args.source,
            "text": data.decode("utf-8", errors="replace"),
        }
    else:
        import base64

        body = {
            "source_id": args.source,
            "content_base64": base64.b64encode(data).decode("ascii"),
            "media_type": args.media_type,
        }
    if args.uri:
        body["source_uri"] = args.uri
    out(service.ingest(body))
    return 0


def cmd_search(args, db: Db, service: Service) -> int:
    results = service.search({
        "tenant": args.tenant,
        "query": args.query,
        "config": args.config,
        "limit": args.limit,
    })
    if args.json:
        out(results)
        return 0
    if not results:
        print("no matches")
        return 0
    for hit in results:
        snippet = " ".join(hit["content"].split())[:220]
        print(f"[{hit['rank']}] {hit.get('source_uri') or '—'} #{hit['seq']}")
        print(f"    {snippet}")
    return 0


# --------------------------------------------------------------------------
# Maintenance
# --------------------------------------------------------------------------


def cmd_youtube(args, db: Db, service: Service) -> int:
    """Point it at a video, a playlist or a channel and it does the rest.

    Works out what was pasted, picks the matching template, creates the source
    and queues the first run. This is the whole point of the template library:
    the operator supplies a link, not a schema.
    """
    kind, identifier = fetcher.parse_target(args.target)

    if kind == "handle":
        api_key = os.environ.get("INGEST_YOUTUBE_API_KEY", "")
        if not api_key:
            print(
                "resolving an @handle needs INGEST_YOUTUBE_API_KEY. Use the "
                "channel's UC… id instead, or set the key.",
                file=sys.stderr,
            )
            return 1
        identifier = fetcher.resolve_handle(api_key, identifier)
        kind = "channel"

    template, config_key, stage = {
        "video": ("youtube_video", "video_id", "fetch"),
        "playlist": ("youtube_playlist", "playlist_id", "discover"),
        "channel": (args.template or "youtube_channel_de", "channel_id", "discover"),
    }[kind]
    if args.template:
        template = args.template

    config = {config_key: identifier}
    if args.language:
        config["language"] = args.language
    if args.max_videos:
        config["max_videos"] = args.max_videos

    created = db.query(
        """
        with tpl as (
            select * from ingest.ingest_contract_template
             where key = (select data->>'template' from _payload)
             order by version desc limit 1
        ),
        c as (
            insert into ingest.ingest_contract
                (tenant_id, name, version, source_type_key, domain_tag,
                 template_key, template_version, extraction_schema,
                 chunk_strategy, metadata_mapping, embedding_profile,
                 fts_config, quality_gates)
            select (p.data->>'tenant')::uuid,
                   t.key || '-v' || t.version, 1, t.source_type_key,
                   t.definition->>'domain_tag', t.key, t.version,
                   t.definition->'extraction_schema',
                   t.definition->'chunk_strategy',
                   t.definition->'metadata_mapping',
                   t.definition->>'embedding_profile',
                   t.definition->>'fts_config',
                   t.definition->'quality_gates'
              from tpl t, _payload p
            on conflict (tenant_id, name, version) do update
                set updated_at = now()
            returning id, tenant_id, source_type_key
        ),
        s as (
            insert into ingest.source
                (tenant_id, source_type_key, ingest_contract_id, display_name,
                 connector_config, credential_ref)
            select c.tenant_id, c.source_type_key, c.id,
                   p.data->>'name', p.data->'config', p.data->>'credential'
              from c, _payload p
            returning id, tenant_id
        ),
        j as (
            insert into ingest.ingest_job
                (tenant_id, source_id, stage, payload, idempotency_key)
            select s.tenant_id, s.id, p.data->>'stage',
                   jsonb_build_object('video_id',
                       case when p.data->>'stage' = 'fetch'
                            then p.data->'config'->>'video_id' end),
                   s.id || ':' || (p.data->>'stage') || ':initial'
              from s, _payload p
            returning id
        )
        select json_build_array(json_build_object(
            'source_id',   (select id from s),
            'contract_id', (select id from c),
            'job_id',      (select id from j)
        ))
        """,
        {
            "tenant": args.tenant,
            "template": template,
            "name": args.name or f"{kind}:{identifier}",
            "config": config,
            "credential": args.credential_ref,
            "stage": stage,
        },
    )
    result = created[0]
    result.update({"kind": kind, "id": identifier, "template": template})
    out(result)
    return 0


def cmd_providers(args, db: Db, service: Service) -> int:
    """Shows the transcript provider chain and its health.

    Worth looking at the moment transcripts start failing: a provider marked
    down is the system telling you the route is blocked, not that the videos
    are gone.
    """
    if args.reset:
        db.execute(
            "update ingest.transcript_provider "
            "   set health = 'ok', consecutive_failures = 0, last_error = null"
        )
    out(db.query(
        """
        select coalesce(json_agg(row_to_json(x)), '[]'::json) from (
            select key, display_name, priority, use_proxy, health,
                   consecutive_failures, last_ok_at, last_probe_at,
                   left(last_error, 200) as last_error,
                   jsonb_array_length(clients) as client_count, is_enabled
              from ingest.transcript_provider
             order by priority
        ) x
        """
    ))
    return 0


def cmd_reprocess(args, db: Db, service: Service) -> int:
    """Queues documents to be built again under their source's contract.

    With `--behind` only the documents whose chunks were produced by an older
    version of the contract are queued. That is the point of recording
    `contract_version` on every chunk: improving a contract should cost a
    reprocessing of what is out of date, not of everything.
    """
    if args.document:
        queued = db.query(
            "select json_build_array(json_build_object('job_id', "
            "  ingest.request_reprocess("
            "    ((select data->>'d' from _payload))::uuid)))",
            {"d": args.document},
        )
        job = queued[0]["job_id"]
        out({"queued": 1 if job else 0, "job_id": job})
        return 0

    if not args.contract:
        print("give either --document or --contract", file=sys.stderr)
        return 1

    behind = db.query(
        """
        select coalesce(json_agg(row_to_json(x)), '[]'::json) from (
            select raw_document_id, at_version, current_version
              from ingest.documents_behind(
                       ((select data->>'c' from _payload))::uuid)
        ) x
        """,
        {"c": args.contract},
    )
    if args.behind and not behind:
        out({"queued": 0, "note": "every document is already on the current version"})
        return 0

    targets = behind if args.behind else db.query(
        """
        select coalesce(json_agg(row_to_json(x)), '[]'::json) from (
            select distinct d.id as raw_document_id
              from ingest.raw_document d
              join ingest.source s on s.id = d.source_id
             where s.ingest_contract_id = ((select data->>'c' from _payload))::uuid
        ) x
        """,
        {"c": args.contract},
    )

    queued = 0
    for target in targets:
        result = db.query(
            "select json_build_array(json_build_object('job_id', "
            "  ingest.request_reprocess("
            "    ((select data->>'d' from _payload))::uuid)))",
            {"d": target["raw_document_id"]},
        )
        if result[0]["job_id"]:
            queued += 1
    out({"candidates": len(targets), "queued": queued})
    return 0


def cmd_reembed(args, db: Db, service: Service) -> int:
    """Fills in vectors for chunks that were stored without them.

    The case this exists for: the stack ran before a model server was
    available. Those chunks are complete and searchable by text; this adds the
    vector without reprocessing the document.
    """
    embedder = Embedder()
    if not embedder.enabled:
        print(
            "INGEST_EMBEDDING_URL is not set, so there is nothing to embed "
            "with. Set it in .env and restart the ingest services.",
            file=sys.stderr,
        )
        return 1

    total = 0
    while True:
        pending = db.query(
            """
            select coalesce(json_agg(row_to_json(x)), '[]'::json) from (
                select id, content
                  from ingest.chunk
                 where embedding is null
                   and is_current
                   and tenant_id = ((select data->>'tenant' from _payload))::uuid
                 order by created_at
                 limit ((select data->>'batch' from _payload))::int
            ) x
            """,
            {"tenant": args.tenant, "batch": args.batch},
        )
        if not pending:
            break
        vectors = embedder.embed([c["content"] for c in pending])
        db.copy_rows(
            "copy _vectors (id, embedding) from stdin",
            [[c["id"], to_pgvector(v)] for c, v in zip(pending, vectors)],
            follow_up="""
                update ingest.chunk ch
                   set embedding = v.embedding,
                       embedding_profile = (select data->>'profile' from _payload)
                  from _vectors v
                 where v.id = ch.id
            """,
            payload={"profile": embedder.model},
        )
        total += len(pending)
        print(f"embedded {total} chunks", file=sys.stderr)
        if len(pending) < args.batch:
            break
    out({"embedded": total})
    return 0


def cmd_prune(args, db: Db, service: Service) -> int:
    """Deletes raw documents past their tenant's retention period.

    Chunks and structured records go with them through the cascade. Files in
    storage are a separate path — they are not reached by a database cascade —
    so they are removed here explicitly.
    """
    doomed = db.query(
        """
        select coalesce(json_agg(row_to_json(x)), '[]'::json) from (
            select d.id, d.storage_ref
              from ingest.raw_document d
              join ingest.tenant t on t.id = d.tenant_id
             where t.retention_days is not null
               and d.fetched_at < now() - make_interval(days => t.retention_days)
        ) x
        """
    )
    if not doomed:
        out({"deleted": 0})
        return 0
    if args.dry_run:
        out({"would_delete": len(doomed)})
        return 0

    jobs_pruned = db.query(
        "select json_build_array(ingest.prune_jobs("
        "  ((select data->>'done' from _payload))::int,"
        "  ((select data->>'dead' from _payload))::int))",
        {"done": args.done_after_days, "dead": args.dead_after_days},
    )

    db.copy_rows(
        "copy _doomed (id) from stdin",
        [[d["id"]] for d in doomed],
        follow_up=(
            "delete from ingest.raw_document "
            "where id in (select id::uuid from _doomed)"
        ),
    )
    removed = 0
    for document in doomed:
        ref = document.get("storage_ref")
        if not ref or not ref.startswith(storage.SCHEME):
            continue
        try:
            Path(ref[len(storage.SCHEME):]).unlink(missing_ok=True)
            removed += 1
        except OSError as exc:
            print(f"could not remove {ref}: {exc}", file=sys.stderr)
    out({
        "deleted": len(doomed),
        "files_removed": removed,
        "jobs_pruned": jobs_pruned[0] if jobs_pruned else 0,
    })
    return 0


# --------------------------------------------------------------------------
# Wiring
# --------------------------------------------------------------------------

# The temporary tables the maintenance commands COPY into. Declared next to the
# commands that use them so the shape is visible at the call site.
TEMP_TABLES = {
    "reembed": "create temp table _vectors (id uuid, embedding vector) on commit drop;",
    "prune": "create temp table _doomed (id text) on commit drop;",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ingest", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init", help="create a tenant and a first source")
    p.add_argument("--tenant", default="default", help="tenant name")
    p.add_argument("--template", default="pdf_generic", help="template key")
    p.add_argument("--source-name", default="Uploads", dest="source_name")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("templates", help="list shipped contract templates")
    p.set_defaults(func=cmd_templates)

    p = sub.add_parser("tenants", help="list tenants")
    p.set_defaults(func=cmd_tenants)

    p = sub.add_parser("sources", help="list a tenant's sources")
    p.add_argument("--tenant", required=True)
    p.set_defaults(func=cmd_sources)

    p = sub.add_parser("jobs", help="list recent jobs")
    p.add_argument("--tenant", required=True)
    p.add_argument("--status", choices=[
        "pending", "running", "done", "failed", "dead",
    ])
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_jobs)

    p = sub.add_parser("push", help="ingest a document from stdin")
    p.add_argument("--source", required=True, help="source id")
    p.add_argument("--media-type", default="application/pdf", dest="media_type")
    p.add_argument("--uri", help="where this document came from")
    p.set_defaults(func=cmd_push)

    p = sub.add_parser("search", help="search a tenant's current chunks")
    p.add_argument("--tenant", required=True)
    p.add_argument("query")
    p.add_argument("--config", default="simple",
                   help="text search configuration the chunks were indexed with")
    p.add_argument("--limit", type=int, default=10)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_search)

    p = sub.add_parser("youtube", help="add a video, playlist or channel")
    p.add_argument("--tenant", required=True)
    p.add_argument("target", help="a link or an id")
    p.add_argument("--template", help="override the template that is picked")
    p.add_argument("--language", help="preferred caption language, e.g. de")
    p.add_argument("--max-videos", type=int, dest="max_videos")
    p.add_argument("--name", help="display name for the source")
    p.add_argument("--credential-ref", dest="credential_ref",
                   help="where the API key lives, e.g. env:MY_YT_KEY")
    p.set_defaults(func=cmd_youtube)

    p = sub.add_parser("providers", help="transcript provider chain and health")
    p.add_argument("--reset", action="store_true",
                   help="clear the breaker and put every provider back in the chain")
    p.set_defaults(func=cmd_providers)

    p = sub.add_parser("reprocess", help="build documents again under their contract")
    p.add_argument("--document", help="a single raw document id")
    p.add_argument("--contract", help="every document of this contract")
    p.add_argument("--behind", action="store_true",
                   help="with --contract: only what is on an older version")
    p.set_defaults(func=cmd_reprocess)

    p = sub.add_parser("reembed", help="fill in missing vectors")
    p.add_argument("--tenant", required=True)
    p.add_argument("--batch", type=int, default=64)
    p.set_defaults(func=cmd_reembed)

    p = sub.add_parser("prune", help="apply retention")
    p.add_argument("--dry-run", action="store_true", dest="dry_run")
    p.add_argument("--done-after-days", type=int, default=7, dest="done_after_days",
                   help="drop finished jobs older than this")
    p.add_argument("--dead-after-days", type=int, default=90, dest="dead_after_days",
                   help="drop failed jobs older than this — evidence, so kept longer")
    p.set_defaults(func=cmd_prune)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    database = Db()
    service = Service()

    # Commands that stream rows in need their scratch table to exist inside the
    # same transaction. `copy_rows` opens that transaction, so the statement is
    # prepended to the COPY target rather than run on its own.
    prelude = TEMP_TABLES.get(args.command)
    if prelude:
        original = database.copy_rows

        def with_prelude(copy_statement, rows, **kwargs):
            return original(prelude + "\n" + copy_statement, rows, **kwargs)

        database.copy_rows = with_prelude  # type: ignore[method-assign]

    return args.func(args, database, service)


if __name__ == "__main__":
    sys.exit(main())
