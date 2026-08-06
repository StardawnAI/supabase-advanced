#!/usr/bin/env python3
"""
Supabase Advanced — ingest layer, raw payload storage.

Binary originals are kept as files, not in the database. Two reasons: the raw
layer is never discarded (improving a contract must not mean fetching
everything again), and a table holding megabytes per row makes every backup and
every replica pay for content nothing queries.

Stage 1 stores them on a local volume. Moving to object storage means replacing
the two functions here — the reference recorded on `raw_document.storage_ref`
already carries a scheme, so both can exist side by side during a migration.
"""

from __future__ import annotations

import os
from pathlib import Path

# Mirrors the object-storage layout so the reference format does not change
# when the backend does: <root>/<tenant>/<first two hex chars>/<hash>
_ROOT = Path(os.environ.get("INGEST_STORAGE_DIR", "/var/lib/ingest/raw"))

SCHEME = "file://"


class StorageError(RuntimeError):
    """The payload could not be written or read back."""


def store(tenant_id: str, content_hash: str, data: bytes) -> str:
    """Writes a payload and returns the reference to record on the document.

    Writing is idempotent by construction: the name is the content hash, so a
    document ingested twice writes identical bytes to the same path.
    """
    target = _path_for(tenant_id, content_hash)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        # Write to a temporary name and rename, so a crash mid-write cannot
        # leave a truncated file under a hash that claims to describe it.
        staging = target.with_suffix(".part")
        staging.write_bytes(data)
        staging.replace(target)
    except OSError as exc:
        raise StorageError(f"cannot write {target}: {exc}") from exc
    return f"{SCHEME}{target}"


def read(reference: str) -> bytes:
    """Reads back a payload written by `store`."""
    if not reference.startswith(SCHEME):
        raise StorageError(f"unsupported storage reference: {reference!r}")
    path = Path(reference[len(SCHEME):])
    try:
        return path.read_bytes()
    except OSError as exc:
        raise StorageError(f"cannot read {path}: {exc}") from exc


def _path_for(tenant_id: str, content_hash: str) -> Path:
    # Two-character fan-out keeps directory listings usable once a tenant has
    # tens of thousands of documents.
    return _ROOT / tenant_id / content_hash[:2] / content_hash
