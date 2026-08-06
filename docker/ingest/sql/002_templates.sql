-- Supabase Advanced — source types and shipped contract templates.
--
-- These are the presets that make "here are my documents, make them
-- searchable" one step instead of a schema-design exercise. They ship with the
-- stack and are updated with it.
--
-- Rule: a template is never edited in place. A change is a new version row, so
-- contracts created from an older version keep behaving the way they did.
-- Upgrading an existing source to a newer template is an explicit action.
--
-- Only source types that have a working extractor are registered here. A type
-- listed but not implemented is worse than a missing one — it offers something
-- the pipeline then fails at.

set search_path = ingest, public, extensions;

insert into ingest.source_type (key, display_name, connector_config_schema)
values
    ('pdf_upload', 'PDF upload', '{
        "type": "object",
        "properties": {
            "bucket": {"type": "string", "description": "Storage bucket to watch, optional"},
            "prefix": {"type": "string", "description": "Path prefix inside the bucket"}
        }
    }'::jsonb),
    ('text_generic', 'Text or JSON push', '{
        "type": "object",
        "properties": {
            "text_field": {"type": "string", "description": "JSON field holding the text, defaults to the whole payload"}
        }
    }'::jsonb)
on conflict (key) do update
    set display_name            = excluded.display_name,
        connector_config_schema = excluded.connector_config_schema;

-- --------------------------------------------------------------------------
-- Templates
-- --------------------------------------------------------------------------

insert into ingest.ingest_contract_template
    (key, version, display_name, description, source_type_key, definition, requires)
values
    (
        'pdf_generic', 1,
        'PDF → searchable knowledge base',
        'Splits a PDF into paragraph-sized passages and makes them searchable. '
        || 'Language-neutral: works for any language but does not stem words.',
        'pdf_upload',
        '{
            "domain_tag": "document",
            "extraction_schema": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "page_count": {"type": "integer"}
                }
            },
            "chunk_strategy": {"mode": "paragraph", "size": 1200, "overlap": 150},
            "metadata_mapping": {"doc_type": "document"},
            "embedding_profile": "none",
            "fts_config": "simple",
            "quality_gates": {"min_chars": 40}
        }'::jsonb,
        '[]'::jsonb
    ),
    (
        'pdf_german', 1,
        'PDF (German) → searchable knowledge base',
        'Like the generic PDF preset, but indexed with German word stemming, so '
        || 'a search for "Versicherung" also finds "Versicherungen". Use this for '
        || 'German contracts, policies and reports.',
        'pdf_upload',
        '{
            "domain_tag": "document_de",
            "extraction_schema": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "page_count": {"type": "integer"}
                }
            },
            "chunk_strategy": {"mode": "paragraph", "size": 1200, "overlap": 150},
            "metadata_mapping": {"doc_type": "document", "language": "de"},
            "embedding_profile": "none",
            "fts_config": "german",
            "quality_gates": {"min_chars": 40}
        }'::jsonb,
        '[]'::jsonb
    ),
    (
        'text_generic', 1,
        'Text or JSON → searchable knowledge base',
        'For anything pushed in as text: notes, transcripts, exported records. '
        || 'Splits on paragraph boundaries.',
        'text_generic',
        '{
            "domain_tag": "text",
            "extraction_schema": {"type": "object", "properties": {}},
            "chunk_strategy": {"mode": "paragraph", "size": 1000, "overlap": 120},
            "metadata_mapping": {"doc_type": "text"},
            "embedding_profile": "none",
            "fts_config": "simple",
            "quality_gates": {"min_chars": 20}
        }'::jsonb,
        '[]'::jsonb
    )
on conflict (key, version) do update
    set display_name = excluded.display_name,
        description  = excluded.description,
        definition   = excluded.definition,
        requires     = excluded.requires;
