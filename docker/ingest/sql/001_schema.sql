-- Supabase Advanced — ingest layer, base schema.
--
-- Everything lives in its own `ingest` schema so it stays separable from the
-- customer's own tables, and so a physical standby replicates it without any
-- special handling.
--
-- Written to be safe to run twice: every object is created conditionally. The
-- migration runner also records what it applied, but re-running by hand must
-- not be a way to lose data.

set search_path = ingest, public, extensions;

create schema if not exists ingest;

-- pgvector. Supabase ships it; the column type below is the only thing that
-- needs it, and a missing extension should fail loudly here rather than at the
-- first embedding write.
create extension if not exists vector;

-- --------------------------------------------------------------------------
-- Tenancy
-- --------------------------------------------------------------------------

create table if not exists ingest.tenant (
    id            uuid primary key default gen_random_uuid(),
    name          text not null unique,
    plan          text not null default 'default',
    -- null means keep forever. A number is enforced by `run.sh ingest prune`,
    -- which deletes raw documents older than this and lets the cascade below
    -- take their records and chunks with them.
    retention_days integer,
    created_at    timestamptz not null default now()
);

-- The tenant a session is acting for. RLS policies below compare against it.
-- Set per request by the API with `set_config('ingest.tenant_id', ..., true)`,
-- which is transaction-scoped, so it cannot leak into a pooled connection's
-- next user.
create or replace function ingest.current_tenant() returns uuid
    language sql stable
as $$
    select nullif(current_setting('ingest.tenant_id', true), '')::uuid;
$$;

-- --------------------------------------------------------------------------
-- What kinds of source exist at all
-- --------------------------------------------------------------------------

create table if not exists ingest.source_type (
    key                     text primary key,
    display_name            text not null,
    -- JSON Schema for the connector_config a source of this type must supply.
    connector_config_schema jsonb not null default '{}'::jsonb
);

-- --------------------------------------------------------------------------
-- Contract templates — shipped with the stack, not written by the customer
-- --------------------------------------------------------------------------
--
-- A template is the answer to "what should happen to this kind of data",
-- prepared in advance. Creating a source from a template is what makes
-- "here is a folder of PDFs, make it searchable" a single step.
--
-- Templates are versioned and never edited in place: a new version is a new
-- row. Contracts created from an older version keep working, and upgrading is
-- an explicit action, never a side effect of updating the stack.

create table if not exists ingest.ingest_contract_template (
    key             text not null,
    version         integer not null,
    display_name    text not null,
    description     text not null default '',
    source_type_key text not null references ingest.source_type (key),
    -- The full contract, in the shape ingest_contract's columns expect.
    definition      jsonb not null,
    -- What the operator has to supply, machine-readable so a form can be
    -- generated from it instead of hand-built per template.
    --   [{"key":"...","label":"...","type":"string|secret","required":true}]
    requires        jsonb not null default '[]'::jsonb,
    created_at      timestamptz not null default now(),
    primary key (key, version)
);

-- --------------------------------------------------------------------------
-- Contracts — one tenant's configured extraction rules
-- --------------------------------------------------------------------------

create table if not exists ingest.ingest_contract (
    id                uuid primary key default gen_random_uuid(),
    tenant_id         uuid not null references ingest.tenant (id) on delete cascade,
    name              text not null,
    version           integer not null default 1,
    source_type_key   text not null references ingest.source_type (key),
    domain_tag        text,
    -- Where this contract came from, so "a newer template exists" is answerable.
    template_key      text,
    template_version  integer,
    extraction_schema jsonb not null default '{}'::jsonb,
    extraction_prompt text,
    -- {"mode":"paragraph|fixed|sentence","size":1200,"overlap":150,
    --  "contextual":false}
    chunk_strategy    jsonb not null default
                      '{"mode":"paragraph","size":1200,"overlap":150}'::jsonb,
    metadata_mapping  jsonb not null default '{}'::jsonb,
    -- 'none' means chunks are stored without vectors and found by full text
    -- search only. That keeps stage 1 usable without a model server.
    embedding_profile text not null default 'none',
    -- Postgres text search configuration: 'german', 'english', 'simple'.
    fts_config        text not null default 'simple',
    quality_gates     jsonb not null default '{}'::jsonb,
    is_active         boolean not null default true,
    created_at        timestamptz not null default now(),
    updated_at        timestamptz not null default now(),
    unique (tenant_id, name, version)
);

-- --------------------------------------------------------------------------
-- Sources — one configured input of one tenant
-- --------------------------------------------------------------------------

create table if not exists ingest.source (
    id                 uuid primary key default gen_random_uuid(),
    tenant_id          uuid not null references ingest.tenant (id) on delete cascade,
    source_type_key    text not null references ingest.source_type (key),
    -- restrict, not cascade: deleting a contract that documents were extracted
    -- with would silently orphan the reason those chunks look the way they do.
    ingest_contract_id uuid not null references ingest.ingest_contract (id) on delete restrict,
    display_name       text not null,
    connector_config   jsonb not null default '{}'::jsonb,
    -- A pointer into Vault, never the secret itself. See the check below.
    credential_ref     text,
    schedule           text,
    status             text not null default 'active'
                       check (status in ('active', 'paused', 'error')),
    last_run_at        timestamptz,
    last_error         text,
    created_at         timestamptz not null default now(),

    -- Makes "credentials never live in connector_config" a rule the database
    -- enforces rather than a line in a document. Without it the first
    -- integration under time pressure puts an API key in here and nobody
    -- notices until the table is exported.
    constraint source_no_inline_credentials check (
        not (connector_config ?| array[
            'api_key', 'apiKey', 'password', 'token', 'secret',
            'access_token', 'client_secret', 'private_key'
        ])
    )
);

-- --------------------------------------------------------------------------
-- Raw documents — kept, never rewritten
-- --------------------------------------------------------------------------

create table if not exists ingest.raw_document (
    id           uuid primary key default gen_random_uuid(),
    tenant_id    uuid not null references ingest.tenant (id) on delete cascade,
    source_id    uuid not null references ingest.source (id) on delete cascade,
    -- The id this document has in the system it came from (video id, message
    -- id, path). Not unique on its own — a re-upload of changed content is a
    -- new document with the same external_id.
    external_id  text,
    source_uri   text,
    -- sha256 of the payload. The deduplication key: the same bytes ingested
    -- twice must not produce a second set of vectors, because duplicates in
    -- the index degrade retrieval quietly.
    content_hash text not null,
    -- Object storage path for binaries. Text and JSON go in raw_payload.
    storage_ref  text,
    raw_payload  jsonb,
    media_type   text,
    byte_size    bigint,
    fetched_at   timestamptz not null default now(),
    unique (tenant_id, content_hash)
);

-- --------------------------------------------------------------------------
-- Structured records — what the contract extracted
-- --------------------------------------------------------------------------

create table if not exists ingest.structured_record (
    id                 uuid primary key default gen_random_uuid(),
    tenant_id          uuid not null references ingest.tenant (id) on delete cascade,
    raw_document_id    uuid not null references ingest.raw_document (id) on delete cascade,
    ingest_contract_id uuid not null references ingest.ingest_contract (id) on delete cascade,
    contract_version   integer not null,
    generation         integer not null default 1,
    extracted          jsonb not null default '{}'::jsonb,
    -- The plain text the chunks are cut from. Kept separately from `extracted`
    -- because it is large and only the worker reads it.
    text_content       text,
    quality_score      numeric,
    extracted_at       timestamptz not null default now(),
    unique (raw_document_id, ingest_contract_id, generation)
);

-- --------------------------------------------------------------------------
-- Chunks — what the bot actually sees
-- --------------------------------------------------------------------------
--
-- `generation` and `is_current` are what make reprocessing safe. A new
-- generation is built alongside the live one and only becomes visible when it
-- is complete, in a single transaction. Without that, a contract improvement
-- means old and new chunks answer the same question at the same time and every
-- result list fills up with near-duplicates.

create table if not exists ingest.chunk (
    id                   uuid primary key default gen_random_uuid(),
    tenant_id            uuid not null references ingest.tenant (id) on delete cascade,
    structured_record_id uuid not null references ingest.structured_record (id) on delete cascade,
    raw_document_id      uuid not null references ingest.raw_document (id) on delete cascade,
    ingest_contract_id   uuid not null references ingest.ingest_contract (id) on delete cascade,
    contract_version     integer not null,
    generation           integer not null default 1,
    is_current           boolean not null default false,
    seq                  integer not null,
    content              text not null,
    -- Anthropic-style contextual retrieval: a short situating sentence that is
    -- embedded together with the content but is not part of it.
    contextual_prefix    text,
    embedding            vector(1024),
    embedding_profile    text,
    fts                  tsvector,
    meta                 jsonb not null default '{}'::jsonb,
    -- {"page":4} for documents, {"start_ms":..,"end_ms":..} for media.
    span                 jsonb,
    created_at           timestamptz not null default now(),
    unique (structured_record_id, generation, seq)
);

-- --------------------------------------------------------------------------
-- Jobs — the queue and the state, in one place
-- --------------------------------------------------------------------------
--
-- Claimed with FOR UPDATE SKIP LOCKED, which is what a queue extension does
-- internally anyway. Keeping the work item and its state in the same row means
-- they cannot disagree, which is the failure mode of a separate queue: a
-- message consumed while its row still says pending, or the reverse.

create table if not exists ingest.ingest_job (
    id              uuid primary key default gen_random_uuid(),
    tenant_id       uuid not null references ingest.tenant (id) on delete cascade,
    source_id       uuid references ingest.source (id) on delete cascade,
    raw_document_id uuid references ingest.raw_document (id) on delete cascade,
    stage           text not null default 'extract'
                    check (stage in ('fetch', 'extract', 'chunk', 'embed')),
    status          text not null default 'pending'
                    check (status in ('pending', 'running', 'done', 'failed', 'dead')),
    attempts        integer not null default 0,
    max_attempts    integer not null default 5,
    -- Retry backoff: a failed job is rescheduled rather than retried at once.
    run_after       timestamptz not null default now(),
    locked_at       timestamptz,
    locked_by       text,
    last_error      text,
    payload         jsonb not null default '{}'::jsonb,
    -- Makes a repeated submission of the same work a no-op instead of a second
    -- pipeline run.
    idempotency_key text not null unique,
    created_at      timestamptz not null default now(),
    updated_at      timestamptz not null default now()
);

-- --------------------------------------------------------------------------
-- Indexes
-- --------------------------------------------------------------------------

-- Vector search. Partial, because chunks without an embedding (no model server
-- configured yet) would otherwise bloat the index for nothing, and because
-- only current chunks are ever searched.
create index if not exists chunk_embedding_hnsw
    on ingest.chunk using hnsw (embedding vector_cosine_ops)
    where embedding is not null and is_current;

create index if not exists chunk_fts_gin
    on ingest.chunk using gin (fts)
    where is_current;

create index if not exists chunk_meta_gin
    on ingest.chunk using gin (meta jsonb_path_ops);

create index if not exists chunk_tenant_current
    on ingest.chunk (tenant_id, is_current);

-- Drives selective reprocessing: "everything still on an older contract".
create index if not exists chunk_tenant_contract_version
    on ingest.chunk (tenant_id, ingest_contract_id, contract_version);

create index if not exists chunk_record
    on ingest.chunk (structured_record_id, generation);

create index if not exists raw_document_source
    on ingest.raw_document (source_id, fetched_at desc);

create index if not exists structured_record_raw
    on ingest.structured_record (raw_document_id);

-- The claim query. Partial so it stays small no matter how much history the
-- table accumulates.
create index if not exists ingest_job_claimable
    on ingest.ingest_job (run_after, created_at)
    where status = 'pending';

create index if not exists ingest_job_tenant_status
    on ingest.ingest_job (tenant_id, status, created_at desc);

-- --------------------------------------------------------------------------
-- Row level security
-- --------------------------------------------------------------------------
--
-- Enabled from the first migration, not retrofitted: adding tenant isolation
-- to a table that already holds several tenants' data means proving after the
-- fact that nothing leaked in the meantime.
--
-- `enable` without `force`, deliberately. The table owner and roles with
-- BYPASSRLS (the service role the ingest service connects as) are meant to see
-- everything — the service sets `ingest.tenant_id` per request instead. Every
-- other role, including anything reaching the database through PostgREST, is
-- confined to its tenant.

do $$
declare
    t text;
begin
    foreach t in array array[
        'ingest_contract', 'source', 'raw_document',
        'structured_record', 'chunk', 'ingest_job'
    ]
    loop
        execute format('alter table ingest.%I enable row level security', t);
        execute format('drop policy if exists tenant_isolation on ingest.%I', t);
        execute format(
            'create policy tenant_isolation on ingest.%I
                 using (tenant_id = ingest.current_tenant())
                 with check (tenant_id = ingest.current_tenant())', t);
    end loop;
end
$$;

-- The tenant table itself: a session may only see its own row.
alter table ingest.tenant enable row level security;
drop policy if exists tenant_self on ingest.tenant;
create policy tenant_self on ingest.tenant
    using (id = ingest.current_tenant());

-- --------------------------------------------------------------------------
-- Grants
-- --------------------------------------------------------------------------
--
-- The ingest schema is not part of the customer's public API. Nothing is
-- exposed through PostgREST unless someone deliberately grants it later.

revoke all on schema ingest from public;

-- Guarded by role existence, because the ingest layer has to install on a
-- plain Postgres too — a test instance, or someone running it beside their own
-- database rather than inside the Supabase stack. A missing `service_role`
-- should mean "nothing to grant", not a failed migration.
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        execute 'revoke all on all tables in schema ingest from anon';
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        execute 'revoke all on all tables in schema ingest from authenticated';
    end if;
    if exists (select 1 from pg_roles where rolname = 'service_role') then
        execute 'grant usage on schema ingest to service_role';
        execute 'grant all on all tables in schema ingest to service_role';
        execute 'grant execute on all functions in schema ingest to service_role';
        execute 'alter default privileges in schema ingest '
                'grant all on tables to service_role';
    end if;
end
$$;
