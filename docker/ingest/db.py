#!/usr/bin/env python3
"""
Supabase Advanced — ingest layer, database access.

Postgres is reached through the `psql` client rather than a driver, for the
same reason `docker/ha` does it: no wheel tree to keep patched on a service
that has to keep running.

That choice makes one thing the central concern of this module. `psql` takes
SQL as text, so any value pasted into that text is an injection. Nothing here
ever does that. Every value crosses the boundary as a single JSON document fed
through `COPY ... FROM STDIN` into a temporary table, and the statement reads
its values from that table. Consequences:

  * There is exactly one escaping rule to get right (COPY's, which is four
    character replacements and fully specified), instead of one per call site.
  * Payload size is bounded by the pipe, not by the 128 KB limit on a single
    command line argument — a book-sized document is not a special case.
  * A statement's text never varies with its data, so it is reviewable on its
    own.

Identifiers cannot be parameterised this way and are never taken from input:
the only ones used are literals written in this repository.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from pathlib import Path
from typing import Any

LOG = logging.getLogger("ingest.db")


class DatabaseError(RuntimeError):
    """psql could not run the script (connection, syntax, constraint, ...)."""


# --------------------------------------------------------------------------
# COPY text format escaping
# --------------------------------------------------------------------------
#
# Postgres' COPY text format reserves exactly these: backslash is the escape
# character, and the three whitespace characters would otherwise end a field or
# a row. Backslash has to be replaced first, otherwise the backslashes
# introduced by the later replacements would be escaped a second time.

_COPY_ESCAPES = (
    ("\\", "\\\\"),
    ("\n", "\\n"),
    ("\r", "\\r"),
    ("\t", "\\t"),
)


def copy_escape(value: str) -> str:
    """Encodes one field for COPY's text format."""
    for raw, escaped in _COPY_ESCAPES:
        value = value.replace(raw, escaped)
    return value


# --------------------------------------------------------------------------
# Connection
# --------------------------------------------------------------------------


class Config:
    def __init__(self) -> None:
        env = os.environ.get
        self.host = env("INGEST_PG_HOST", "db")
        self.port = int(env("INGEST_PG_PORT", "5432"))
        self.user = env("INGEST_PG_USER", "supabase_admin")
        self.password = env("INGEST_PG_PASSWORD", "")
        self.database = env("INGEST_PG_DATABASE", "postgres")
        self.timeout = int(env("INGEST_PG_TIMEOUT", "30"))


class Db:
    """Runs scripts against one Postgres database."""

    def __init__(self, cfg: Config | None = None) -> None:
        self.cfg = cfg or Config()

    # ----------------------------------------------------------------------
    # Core
    # ----------------------------------------------------------------------

    def run(self, script: str, timeout: int | None = None) -> str:
        """Runs a psql script given on stdin and returns its output.

        The script is passed through a pipe, so it may be of any size and no
        part of it appears in the process list — which matters because these
        scripts carry customer content.
        """
        limit = timeout or self.cfg.timeout
        cmd = [
            "psql", "-X", "-q", "-A", "-t",
            "-h", self.cfg.host,
            "-p", str(self.cfg.port),
            "-U", self.cfg.user,
            "-d", self.cfg.database,
            "-v", "ON_ERROR_STOP=1",
            "-f", "-",
        ]
        env = dict(
            os.environ,
            PGPASSWORD=self.cfg.password,
            PGCONNECT_TIMEOUT=str(limit),
            # Keeps error text parseable regardless of the host's locale.
            LC_MESSAGES="C",
        )
        try:
            proc = subprocess.run(
                cmd,
                input=script,
                capture_output=True,
                text=True,
                env=env,
                timeout=limit + 15,
            )
        except subprocess.TimeoutExpired as exc:
            raise DatabaseError(f"psql timed out after {limit + 15}s") from exc
        except FileNotFoundError as exc:
            raise DatabaseError("psql client not found in PATH") from exc
        if proc.returncode != 0:
            raise DatabaseError(proc.stderr.strip() or f"psql exited {proc.returncode}")
        return proc.stdout.strip()

    def execute(
        self,
        statement: str,
        payload: Any = None,
        *,
        tenant_id: str | None = None,
        timeout: int | None = None,
    ) -> str:
        """Runs one statement with an optional JSON payload.

        `statement` reads its values from the temporary table `_payload`, which
        holds a single row with a single `jsonb` column named `data`:

            insert into ingest.tenant (name)
            select data->>'name' from _payload

        `tenant_id` sets the row level security context for the transaction.
        It is a value like any other and travels inside the payload, never in
        the statement text.
        """
        data = payload if payload is not None else {}
        if tenant_id is not None:
            if not isinstance(data, dict):
                raise DatabaseError(
                    "a tenant-scoped statement needs an object payload, "
                    f"got {type(data).__name__}"
                )
            data = dict(data)
            data["__tenant"] = tenant_id

        parts = [
            "begin;",
            "create temp table _payload (data jsonb) on commit drop;",
            "copy _payload (data) from stdin;",
            copy_escape(json.dumps(data)),
            "\\.",
        ]
        if tenant_id is not None:
            # Transaction-scoped (the trailing `true`), so it cannot survive
            # into the next user of a pooled connection.
            parts.append(
                "select set_config('ingest.tenant_id', "
                "coalesce((select data->>'__tenant' from _payload), ''), true);"
            )
        parts.append(statement.rstrip().rstrip(";") + ";")
        parts.append("commit;")
        return self.run("\n".join(parts), timeout=timeout)

    def query(
        self,
        statement: str,
        payload: Any = None,
        *,
        tenant_id: str | None = None,
        timeout: int | None = None,
    ) -> Any:
        """Runs a statement that returns one JSON value, and decodes it.

        The statement is responsible for producing JSON, which keeps the
        row-to-object mapping in SQL where the shape is visible:

            select coalesce(json_agg(t), '[]'::json)
              from (select id, name from ingest.tenant) t
        """
        raw = self.execute(
            statement, payload, tenant_id=tenant_id, timeout=timeout
        )
        # psql prints nothing for an empty result set.
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise DatabaseError(
                f"expected a JSON result, got: {raw[:200]!r}"
            ) from exc

    def copy_rows(
        self,
        copy_statement: str,
        rows: list[list[str | None]],
        *,
        follow_up: str = "",
        payload: dict | None = None,
        tenant_id: str | None = None,
        timeout: int | None = None,
    ) -> str:
        """Streams many rows in one COPY, then runs a follow-up statement.

        Used for chunks: a large document produces hundreds of them, and one
        statement per chunk would mean one round trip per chunk.

        `copy_statement` names the target and its columns; `rows` supplies the
        values positionally, with None meaning SQL NULL. `follow_up` runs in
        the same transaction and reads its values from `_payload`, exactly as
        `execute` does — so the rows and the statement that acts on them commit
        together or not at all.
        """
        data = dict(payload or {})
        if tenant_id is not None:
            data["__tenant"] = tenant_id

        parts = [
            "begin;",
            "create temp table _payload (data jsonb) on commit drop;",
            "copy _payload (data) from stdin;",
            copy_escape(json.dumps(data)),
            "\\.",
        ]
        if tenant_id is not None:
            parts.append(
                "select set_config('ingest.tenant_id', "
                "coalesce((select data->>'__tenant' from _payload), ''), true);"
            )
        parts.append(copy_statement.rstrip().rstrip(";") + ";")
        for row in rows:
            parts.append(
                "\t".join(
                    "\\N" if field is None else copy_escape(str(field))
                    for field in row
                )
            )
        parts.append("\\.")
        if follow_up:
            parts.append(follow_up.rstrip().rstrip(";") + ";")
        parts.append("commit;")
        return self.run("\n".join(parts), timeout=timeout)

    # ----------------------------------------------------------------------
    # Migrations
    # ----------------------------------------------------------------------

    # Every process that migrates takes this advisory lock first, so only one
    # of them is ever inside a migration. Without it the API and the worker
    # start together, both find a migration unapplied, and both run it — at
    # which point `create extension if not exists` is not the guard it looks
    # like: both see it missing, both create it, and one gets a unique
    # violation on pg_extension. The number is arbitrary but must never change,
    # or two versions of this code would not exclude each other.
    MIGRATION_LOCK = 4759283746152837

    def migrate(self, sql_dir: Path) -> list[str]:
        """Applies every not-yet-applied .sql file, in filename order.

        Runs on every service start, in every container, concurrently — so it
        is serialised by an advisory lock and each file is written to be safe
        to run twice. A half-applied migration recovered by hand therefore does
        not leave the runner unable to continue either.
        """
        self.run(
            "begin;\n"
            f"select pg_advisory_xact_lock({self.MIGRATION_LOCK});\n"
            "create schema if not exists ingest;\n"
            "create table if not exists ingest.schema_migration ("
            "  filename text primary key,"
            "  applied_at timestamptz not null default now());\n"
            "commit;"
        )
        applied = set(
            (self.query(
                "select coalesce(json_agg(filename), '[]'::json) "
                "from ingest.schema_migration"
            ) or [])
        )

        files = sorted(p for p in sql_dir.glob("*.sql") if p.is_file())
        if not files:
            raise DatabaseError(f"no .sql files found in {sql_dir}")

        newly: list[str] = []
        for path in files:
            if path.name in applied:
                continue
            LOG.info("applying migration %s", path.name)
            body = path.read_text(encoding="utf-8")
            # One transaction per file: a failure leaves nothing half-created,
            # and the file is not recorded, so the next start retries it.
            #
            # The lock is taken inside that transaction and released by the
            # commit. A second process waiting on it runs the file again after
            # it is let through — harmless, because the files are idempotent —
            # and its bookkeeping insert then does nothing.
            script = (
                "begin;\n"
                f"select pg_advisory_xact_lock({self.MIGRATION_LOCK});\n"
                + body
                + "\ncreate temp table _payload (data jsonb) on commit drop;\n"
                "copy _payload (data) from stdin;\n"
                + copy_escape(json.dumps({"filename": path.name}))
                + "\n\\.\n"
                "insert into ingest.schema_migration (filename) "
                "select data->>'filename' from _payload "
                "on conflict (filename) do nothing;\n"
                "commit;\n"
            )
            self.run(script, timeout=300)
            newly.append(path.name)
        return newly

    def wait_until_ready(self, attempts: int = 60, delay: float = 2.0) -> None:
        """Blocks until the database answers.

        The stack starts everything at once; without this the service would
        exit on the first start simply because Postgres was still opening.
        """
        import time

        last: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                self.run("select 1;", timeout=5)
                return
            except DatabaseError as exc:
                last = exc
                if attempt == 1 or attempt % 10 == 0:
                    LOG.info("waiting for postgres (%s/%s)", attempt, attempts)
                time.sleep(delay)
        raise DatabaseError(f"database not reachable: {last}")


# --------------------------------------------------------------------------
# Small helpers shared by api.py and worker.py
# --------------------------------------------------------------------------

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def is_uuid(value: str) -> bool:
    """True for a canonical UUID.

    Used to reject malformed ids early with a clear message instead of letting
    them surface as a cast error from deep inside a statement.
    """
    return bool(_UUID_RE.match(value or ""))
