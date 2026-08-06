-- Supabase Advanced — ingest layer, YouTube transcripts.
--
-- Adds the source types, the provider chain that keeps a blocked route from
-- being an outage, and the templates that make "here is a playlist" one step.

set search_path = ingest, public, extensions;

-- --------------------------------------------------------------------------
-- A job may now discover work rather than only do it
-- --------------------------------------------------------------------------
--
-- A playlist is not a document. It is a job that produces jobs, and it fails
-- differently from fetching one video, so it gets its own stage rather than
-- being folded into 'fetch'.

alter table ingest.ingest_job drop constraint if exists ingest_job_stage_check;
alter table ingest.ingest_job add constraint ingest_job_stage_check
    check (stage in ('discover', 'fetch', 'extract', 'chunk', 'embed'));

-- A fetch job has no document yet — it exists to create one.
alter table ingest.ingest_job alter column raw_document_id drop not null;

-- --------------------------------------------------------------------------
-- Transcript providers
-- --------------------------------------------------------------------------
--
-- The point of this table: a blocked route must be a degradation, not an
-- outage. Each provider is a different way of getting the same transcript,
-- with a different failure mode. The chain is walked in priority order,
-- skipping anything the breaker has marked down.
--
-- Provider health is data, not code, so a route can be disabled or reordered
-- while the system runs — which is what you want at the moment a route stops
-- working, because that is never a convenient moment to deploy.

create table if not exists ingest.transcript_provider (
    key                  text primary key,
    display_name         text not null,
    priority             integer not null,
    -- Rough running cost, so the chain can be ordered by what it costs rather
    -- than by what it is called.
    cost_per_min         numeric not null default 0,
    -- Whether this route sends its requests through the proxy. The reason the
    -- proxy exists at all: YouTube refuses the innertube player call from
    -- datacenter addresses far more often than from ordinary ones.
    use_proxy            boolean not null default false,
    -- Client identities to impersonate, in order, and their app versions.
    -- Here rather than in code because the versions go stale and updating
    -- them must not require a release.
    clients              jsonb not null default '[]'::jsonb,
    health               text not null default 'ok'
                         check (health in ('ok', 'degraded', 'down')),
    consecutive_failures integer not null default 0,
    last_ok_at           timestamptz,
    last_error           text,
    last_probe_at        timestamptz,
    is_enabled           boolean not null default true
);

-- Client definitions, ported from the workflow that has been fetching these
-- transcripts in production. The order matters and was found by trial:
-- ANDROID has the best caption availability, TVHTML5 is the last resort.
--
-- The note worth keeping: ANDROID is refused from Oracle Cloud addresses and
-- works from ordinary business connections — which is the whole reason the
-- proxied route is first.
insert into ingest.transcript_provider
    (key, display_name, priority, cost_per_min, use_proxy, clients)
values
    ('warp_innertube', 'InnerTube via proxy', 1, 0, true, '[
        {"clientName": "ANDROID", "clientVersion": "20.10.38",
         "androidSdkVersion": 30,
         "userAgent": "com.google.android.youtube/20.10.38 (Linux; U; Android 11) gzip"},
        {"clientName": "WEB", "clientVersion": "2.20240101.00.00",
         "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"},
        {"clientName": "MWEB", "clientVersion": "2.20250312.07.00",
         "userAgent": "Mozilla/5.0 (Linux; Android 11) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"},
        {"clientName": "IOS", "clientVersion": "19.29.1",
         "userAgent": "com.google.ios.youtube/19.29.1 (iPhone16,2; U; CPU iOS 17_5_1 like Mac OS X)"},
        {"clientName": "TVHTML5_SIMPLY_EMBEDDED_PLAYER", "clientVersion": "2.0",
         "userAgent": "Mozilla/5.0 (PlayStation; PlayStation 4/12.00) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15"}
    ]'::jsonb),
    ('direct_innertube', 'InnerTube without proxy', 2, 0, false, '[
        {"clientName": "ANDROID", "clientVersion": "20.10.38",
         "androidSdkVersion": 30,
         "userAgent": "com.google.android.youtube/20.10.38 (Linux; U; Android 11) gzip"},
        {"clientName": "WEB", "clientVersion": "2.20240101.00.00",
         "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"}
    ]'::jsonb)
on conflict (key) do update
    set display_name = excluded.display_name,
        priority     = excluded.priority,
        use_proxy    = excluded.use_proxy,
        clients      = excluded.clients;

-- Walking the chain: enabled, not marked down, cheapest first.
create or replace function ingest.transcript_chain()
returns table (key text, use_proxy boolean, clients jsonb)
language sql stable
as $$
    select key, use_proxy, clients
      from ingest.transcript_provider
     where is_enabled
       and health <> 'down'
     order by priority, cost_per_min;
$$;

-- Three consecutive failures take a provider out of the chain. Not one:
-- a single video can be unavailable for reasons that say nothing about the
-- route. Three in a row is the route.
create or replace function ingest.record_provider_result(
    provider_key text,
    succeeded boolean,
    detail text default null
)
returns text
language plpgsql
as $$
declare
    new_health text;
begin
    if succeeded then
        update ingest.transcript_provider
           set consecutive_failures = 0,
               health = 'ok',
               last_ok_at = now(),
               last_error = null,
               last_probe_at = now()
         where key = provider_key
        returning health into new_health;
    else
        update ingest.transcript_provider
           set consecutive_failures = consecutive_failures + 1,
               health = case
                   when consecutive_failures + 1 >= 3 then 'down'
                   else 'degraded'
               end,
               last_error = left(detail, 2000),
               last_probe_at = now()
         where key = provider_key
        returning health into new_health;
    end if;
    return new_health;
end;
$$;

-- --------------------------------------------------------------------------
-- Source types
-- --------------------------------------------------------------------------

insert into ingest.source_type (key, display_name, connector_config_schema)
values
    ('youtube_video', 'YouTube video', '{
        "type": "object",
        "required": ["video_id"],
        "properties": {
            "video_id": {"type": "string"},
            "language": {"type": "string", "description": "preferred caption language"}
        }
    }'::jsonb),
    ('youtube_playlist', 'YouTube playlist', '{
        "type": "object",
        "required": ["playlist_id"],
        "properties": {
            "playlist_id": {"type": "string"},
            "language": {"type": "string"},
            "max_videos": {"type": "integer"}
        }
    }'::jsonb),
    ('youtube_channel', 'YouTube channel', '{
        "type": "object",
        "required": ["channel_id"],
        "properties": {
            "channel_id": {"type": "string"},
            "language": {"type": "string"},
            "max_videos": {"type": "integer"}
        }
    }'::jsonb)
on conflict (key) do update
    set display_name            = excluded.display_name,
        connector_config_schema = excluded.connector_config_schema;

-- --------------------------------------------------------------------------
-- Templates
-- --------------------------------------------------------------------------
--
-- Transcripts are cut by time rather than by paragraph: spoken text has no
-- paragraphs, and a chunk that maps to a stretch of the video can be cited
-- back with a timestamp.

insert into ingest.ingest_contract_template
    (key, version, display_name, description, source_type_key, definition, requires)
values
    (
        'youtube_video', 1,
        'YouTube video → searchable transcript',
        'Fetches the transcript of one video and makes it searchable, with a '
        || 'timestamp on every passage so an answer can point back into the video.',
        'youtube_video',
        '{
            "domain_tag": "video_transcript",
            "extraction_schema": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "channel_title": {"type": "string"},
                    "published_at": {"type": "string"},
                    "duration_sec": {"type": "integer"}
                }
            },
            "chunk_strategy": {"mode": "sentence", "size": 900, "overlap": 120},
            "metadata_mapping": {"doc_type": "video_transcript"},
            "embedding_profile": "none",
            "fts_config": "simple",
            "quality_gates": {"min_chars": 60}
        }'::jsonb,
        '[{"key": "youtube_api_key", "label": "YouTube Data API key",
           "type": "secret", "required": true,
           "help": "Needed for titles and durations. The transcript itself does not use it."}]'::jsonb
    ),
    (
        'youtube_playlist', 1,
        'YouTube playlist → searchable knowledge base',
        'Walks a playlist, fetches every transcript, and makes the whole thing '
        || 'searchable as one body of material.',
        'youtube_playlist',
        '{
            "domain_tag": "video_transcript",
            "extraction_schema": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "channel_title": {"type": "string"},
                    "published_at": {"type": "string"},
                    "duration_sec": {"type": "integer"}
                }
            },
            "chunk_strategy": {"mode": "sentence", "size": 900, "overlap": 120},
            "metadata_mapping": {"doc_type": "video_transcript"},
            "embedding_profile": "none",
            "fts_config": "simple",
            "quality_gates": {"min_chars": 60}
        }'::jsonb,
        '[{"key": "youtube_api_key", "label": "YouTube Data API key",
           "type": "secret", "required": true,
           "help": "Needed to list the playlist and read video metadata."}]'::jsonb
    ),
    (
        'youtube_channel_de', 1,
        'YouTube channel (German) → searchable knowledge base',
        'Every video of a channel, indexed with German word stemming so a '
        || 'search for "Versicherung" also finds "Versicherungen".',
        'youtube_channel',
        '{
            "domain_tag": "video_transcript",
            "extraction_schema": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "channel_title": {"type": "string"},
                    "published_at": {"type": "string"},
                    "duration_sec": {"type": "integer"}
                }
            },
            "chunk_strategy": {"mode": "sentence", "size": 900, "overlap": 120},
            "metadata_mapping": {"doc_type": "video_transcript", "language": "de"},
            "embedding_profile": "none",
            "fts_config": "german",
            "quality_gates": {"min_chars": 60}
        }'::jsonb,
        '[{"key": "youtube_api_key", "label": "YouTube Data API key",
           "type": "secret", "required": true,
           "help": "Needed to list the channel and read video metadata."}]'::jsonb
    )
on conflict (key, version) do update
    set display_name = excluded.display_name,
        description  = excluded.description,
        definition   = excluded.definition,
        requires     = excluded.requires;
