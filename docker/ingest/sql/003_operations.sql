-- Supabase Advanced — ingest layer, gaps found reviewing the stage 1 schema.
--
-- None of these change what the tables mean. They are the things a schema
-- needs once it is operated rather than demonstrated: a way to ask for
-- reprocessing, a way for the job table to stop growing, and indexes for the
-- questions the code actually asks.

set search_path = ingest, public, extensions;

-- --------------------------------------------------------------------------
-- Reprocessing
-- --------------------------------------------------------------------------
--
-- The generation machinery was complete but unreachable: `idempotency_key` is
-- built as "<hash>:<contract>:extract", so a second job for the same document
-- and contract is refused as a duplicate — which is right for a re-upload and
-- wrong for "run this again with the improved contract". A revision number in
-- the key separates the two.

create sequence if not exists ingest.reprocess_revision;

-- Queues a document for reprocessing under the contract its source currently
-- points at. Returns the job id, or null when one is already queued.
--
-- Written as a function rather than as SQL in the application so that the
-- reprocessing path is identical whether it is triggered by the CLI, by a
-- scheduled sweep, or by hand in the SQL editor.
create or replace function ingest.request_reprocess(document_id uuid)
returns uuid
language plpgsql
as $$
declare
    job_id uuid;
begin
    insert into ingest.ingest_job
        (tenant_id, source_id, raw_document_id, stage, payload, idempotency_key)
    select d.tenant_id,
           d.source_id,
           d.id,
           'extract',
           jsonb_build_object('reason', 'reprocess'),
           d.content_hash || ':' || s.ingest_contract_id || ':extract:r'
               || nextval('ingest.reprocess_revision')
      from ingest.raw_document d
      join ingest.source s on s.id = d.source_id
     where d.id = document_id
    on conflict (idempotency_key) do nothing
    returning id into job_id;

    return job_id;
end;
$$;

-- Everything a contract has left behind at an older version. This is the
-- selective reindexing the whole `contract_version` design exists for: after
-- improving a contract, reprocess only what is actually out of date.
create or replace function ingest.documents_behind(contract uuid)
returns table (raw_document_id uuid, at_version integer, current_version integer)
language sql stable
as $$
    select distinct r.raw_document_id, r.contract_version, c.version
      from ingest.structured_record r
      join ingest.ingest_contract c on c.id = r.ingest_contract_id
     where r.ingest_contract_id = contract
       and r.contract_version < c.version;
$$;

-- --------------------------------------------------------------------------
-- Job retention
-- --------------------------------------------------------------------------
--
-- Without this the job table grows forever. It is the busiest table in the
-- schema and the one whose index the claim query depends on, so unbounded
-- growth is a slow, quiet degradation of exactly the wrong thing.
--
-- Failed and dead jobs are kept far longer than finished ones: a job that
-- worked is history, a job that did not is evidence.

create or replace function ingest.prune_jobs(
    done_after_days integer default 7,
    dead_after_days integer default 90
)
returns integer
language sql
as $$
    with gone as (
        delete from ingest.ingest_job
         where (status = 'done'
                and updated_at < now() - make_interval(days => done_after_days))
            or (status in ('dead', 'failed')
                and updated_at < now() - make_interval(days => dead_after_days))
        returning 1
    )
    select count(*)::integer from gone;
$$;

-- --------------------------------------------------------------------------
-- updated_at
-- --------------------------------------------------------------------------
--
-- The column existed and was maintained by whichever statement remembered to.
-- Retention above depends on it being right, so it is now the database's job.

create or replace function ingest.touch_updated_at()
returns trigger
language plpgsql
as $$
begin
    -- Only stamp it when the statement did not set it itself. An
    -- unconditional `now()` makes the column impossible to write, which breaks
    -- backdating during a data migration and, less obviously, makes the
    -- retention sweep above untestable: every attempt to age a row for the
    -- test simply re-stamps it as current.
    if new.updated_at is not distinct from old.updated_at then
        new.updated_at = now();
    end if;
    return new;
end;
$$;

do $$
declare
    t text;
begin
    foreach t in array array['ingest_job', 'ingest_contract']
    loop
        execute format('drop trigger if exists touch_updated_at on ingest.%I', t);
        execute format(
            'create trigger touch_updated_at before update on ingest.%I
                 for each row execute function ingest.touch_updated_at()', t);
    end loop;
end
$$;

-- --------------------------------------------------------------------------
-- Indexes for questions the code asks
-- --------------------------------------------------------------------------

-- "Do I already have this video / message / file?" — asked once per candidate
-- document by every scheduled source, and a sequential scan until now.
create index if not exists raw_document_external
    on ingest.raw_document (tenant_id, source_id, external_id)
    where external_id is not null;

-- Drives `documents_behind`, i.e. "what is still on an older contract".
create index if not exists structured_record_contract_version
    on ingest.structured_record (ingest_contract_id, contract_version);

-- Retention sweeps scan by status and age.
create index if not exists ingest_job_retention
    on ingest.ingest_job (status, updated_at);

-- A source's contract is followed on every accepted document.
create index if not exists source_contract
    on ingest.source (ingest_contract_id);

-- --------------------------------------------------------------------------
-- A job stuck in 'running'
-- --------------------------------------------------------------------------
--
-- A worker killed mid-job leaves its row claimed forever: `status = 'running'`
-- is not picked up by the claim query, so the document silently never
-- finishes. Returning it to the queue after a grace period is the difference
-- between a crashed container costing a restart and costing a document.

create or replace function ingest.requeue_stalled(older_than_minutes integer default 30)
returns integer
language sql
as $$
    with revived as (
        update ingest.ingest_job
           set status = case when attempts >= max_attempts then 'dead' else 'pending' end,
               locked_at = null,
               locked_by = null,
               last_error = coalesce(last_error, '')
                            || ' [requeued: worker vanished while running]'
         where status = 'running'
           and locked_at < now() - make_interval(mins => older_than_minutes)
        returning 1
    )
    select count(*)::integer from revived;
$$;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'service_role') then
        execute 'grant execute on all functions in schema ingest to service_role';
    end if;
end
$$;
