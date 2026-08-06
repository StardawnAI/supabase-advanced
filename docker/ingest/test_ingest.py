#!/usr/bin/env python3
"""
Unit tests for the ingest layer.

Run:  python3 -m unittest discover -s docker/ingest

No database and no network. What is tested here is what can be wrong without
either: the escaping that keeps customer content out of SQL text, the chunking
that decides retrieval quality, and the request handling that decides what
reaches the database at all.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import api  # noqa: E402
import db  # noqa: E402
import storage  # noqa: E402
import worker  # noqa: E402


# --------------------------------------------------------------------------
# COPY escaping — the single boundary customer content crosses
# --------------------------------------------------------------------------


class CopyEscapeTest(unittest.TestCase):
    def test_backslash_is_escaped_first(self):
        # If newlines were escaped before backslashes, the backslash of "\n"
        # would be escaped again and the value would arrive corrupted.
        self.assertEqual(db.copy_escape("a\\nb"), "a\\\\nb")

    def test_whitespace_that_would_end_a_field_or_row(self):
        self.assertEqual(db.copy_escape("a\tb"), "a\\tb")
        self.assertEqual(db.copy_escape("a\nb"), "a\\nb")
        self.assertEqual(db.copy_escape("a\r\nb"), "a\\r\\nb")

    def test_leaves_ordinary_text_alone(self):
        self.assertEqual(db.copy_escape("Grüße, Straße"), "Grüße, Straße")

    def test_a_payload_that_tries_to_end_the_copy_block(self):
        # "\." on its own line terminates COPY. Escaped, it cannot.
        hostile = "harmless\n\\.\ndrop table ingest.chunk;"
        escaped = db.copy_escape(hostile)
        self.assertNotIn("\n", escaped)
        self.assertEqual(escaped, "harmless\\n\\\\.\\ndrop table ingest.chunk;")


class ScriptBuildingTest(unittest.TestCase):
    """The script must carry values as data, never as SQL text."""

    def setUp(self):
        self.database = db.Db(db.Config())
        self.scripts = []
        patcher = mock.patch.object(
            self.database, "run", side_effect=self._capture
        )
        self.addCleanup(patcher.stop)
        patcher.start()

    def _capture(self, script, timeout=None):
        self.scripts.append(script)
        return ""

    def test_payload_never_appears_in_the_statement(self):
        self.database.execute(
            "insert into ingest.tenant (name) select data->>'name' from _payload",
            {"name": "Robert'); drop table ingest.tenant;--"},
        )
        script = self.scripts[0]
        # The value is present exactly once, inside the COPY block, JSON
        # encoded — not spliced into the insert.
        self.assertIn("copy _payload (data) from stdin;", script)
        statement_line = [
            line for line in script.splitlines()
            if line.startswith("insert into ingest.tenant")
        ][0]
        self.assertNotIn("drop table", statement_line)

    def test_transaction_wraps_everything(self):
        self.database.execute("select 1", {})
        script = self.scripts[0]
        self.assertTrue(script.startswith("begin;"))
        self.assertTrue(script.rstrip().endswith("commit;"))

    def test_tenant_is_set_transaction_scoped(self):
        self.database.execute(
            "select 1", {"a": 1}, tenant_id="6f1d5f92-0000-4000-8000-000000000001"
        )
        script = self.scripts[0]
        self.assertIn("set_config('ingest.tenant_id'", script)
        # The trailing `true` is what keeps it from leaking into the next user
        # of a pooled connection.
        self.assertIn("), true);", script)
        # It travels inside the payload, not in the statement.
        self.assertIn("__tenant", script)

    def test_tenant_scoped_call_rejects_a_non_object_payload(self):
        with self.assertRaises(db.DatabaseError):
            self.database.execute("select 1", ["a", "b"], tenant_id="x")

    def test_copy_rows_escapes_every_field_and_marks_nulls(self):
        self.database.copy_rows(
            "copy ingest.chunk (a, b) from stdin",
            [["line\none", None], ["tab\there", "plain"]],
            follow_up="update ingest.chunk set x = 1",
        )
        script = self.scripts[0]
        self.assertIn("line\\none\t\\N", script)
        self.assertIn("tab\\there\tplain", script)
        self.assertIn("update ingest.chunk set x = 1;", script)

    def test_copy_rows_follow_up_can_read_its_own_payload(self):
        self.database.copy_rows(
            "copy ingest.chunk (a) from stdin",
            [["x"]],
            follow_up="update ingest.chunk set g = (select data->>'g' from _payload)::int",
            payload={"g": 7},
        )
        script = self.scripts[0]
        self.assertIn('{"g": 7}', script)


class MigrationTest(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: [p.unlink() for p in self.dir.glob("*.sql")])
        (self.dir / "002_second.sql").write_text("select 2;", encoding="utf-8")
        (self.dir / "001_first.sql").write_text("select 1;", encoding="utf-8")
        (self.dir / "010_tenth.sql").write_text("select 10;", encoding="utf-8")

    def test_applies_in_filename_order_and_skips_the_applied(self):
        database = db.Db(db.Config())
        scripts = []
        with mock.patch.object(database, "run", side_effect=lambda s, timeout=None: scripts.append(s) or ""):
            with mock.patch.object(database, "query", return_value=["001_first.sql"]):
                applied = database.migrate(self.dir)
        self.assertEqual(applied, ["002_second.sql", "010_tenth.sql"])
        # Zero-padding is what keeps 010 after 002 rather than after 001.
        bodies = [s for s in scripts if "select 2;" in s or "select 10;" in s]
        self.assertEqual(len(bodies), 2)
        self.assertIn("select 2;", bodies[0])

    def test_empty_directory_is_an_error_not_a_silent_success(self):
        database = db.Db(db.Config())
        with mock.patch.object(database, "run", return_value=""):
            with mock.patch.object(database, "query", return_value=[]):
                with self.assertRaises(db.DatabaseError):
                    database.migrate(Path(tempfile.mkdtemp()))


# --------------------------------------------------------------------------
# Chunking — what decides whether retrieval works
# --------------------------------------------------------------------------


class NormaliseTest(unittest.TestCase):
    def test_rejoins_words_hyphenated_across_a_line_break(self):
        # pdftotext produces these constantly; leaving them splits a word into
        # two tokens that neither stemming nor a vector recognises.
        self.assertEqual(worker.normalise("Versiche-\nrung"), "Versicherung")

    def test_collapses_runs_of_spaces_but_keeps_paragraphs(self):
        self.assertEqual(
            worker.normalise("a    b\n\n\n\nc"), "a b\n\nc"
        )

    def test_carriage_returns(self):
        self.assertEqual(worker.normalise("a\r\n\r\nb"), "a\n\nb")


class ChunkTest(unittest.TestCase):
    def test_paragraphs_are_packed_not_cut(self):
        text = "\n\n".join(["Sentence one." * 3, "Sentence two." * 3])
        chunks = worker.chunk_text(text, {"mode": "paragraph", "size": 200, "overlap": 0})
        for chunk in chunks:
            self.assertLessEqual(len(chunk), 200)
        self.assertIn("Sentence one.", chunks[0])

    def test_an_oversized_paragraph_is_split_rather_than_emitted_whole(self):
        # A chunk far larger than the others skews every similarity score it
        # takes part in, so it must not survive as one piece.
        text = "x" * 5000
        chunks = worker.chunk_text(text, {"mode": "paragraph", "size": 500, "overlap": 0})
        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(len(chunk), 500)

    def test_overlap_repeats_the_tail_of_the_previous_chunk(self):
        units = [f"Paragraph number {i} with some filler text." for i in range(12)]
        chunks = worker.chunk_text(
            "\n\n".join(units), {"mode": "paragraph", "size": 120, "overlap": 60}
        )
        self.assertGreater(len(chunks), 2)
        overlapping = [
            i for i in range(1, len(chunks))
            if any(part and part in chunks[i] for part in chunks[i - 1].split("\n\n"))
        ]
        self.assertTrue(overlapping, "no chunk repeated anything from its predecessor")

    def test_overlap_at_or_above_size_cannot_loop_forever(self):
        # Left unclamped this re-emits the same window and never advances.
        chunks = worker.chunk_text(
            "word " * 400, {"mode": "fixed", "size": 100, "overlap": 100}
        )
        self.assertGreater(len(chunks), 1)
        self.assertLess(len(chunks), 200)

    def test_fixed_mode_covers_the_whole_text(self):
        text = "abcdefghij" * 30
        chunks = worker.chunk_text(text, {"mode": "fixed", "size": 100, "overlap": 0})
        self.assertEqual("".join(chunks), text)

    def test_sentence_mode_does_not_split_mid_sentence(self):
        text = "First one. Second one! Third one? Fourth one."
        chunks = worker.chunk_text(text, {"mode": "sentence", "size": 25, "overlap": 0})
        for chunk in chunks:
            self.assertTrue(chunk.endswith((".", "!", "?")), chunk)

    def test_empty_input_yields_no_chunks(self):
        self.assertEqual(worker.chunk_text("   \n\n  ", {}), [])

    def test_defaults_apply_when_the_contract_says_nothing(self):
        chunks = worker.chunk_text("a\n\nb", {})
        self.assertEqual(chunks, ["a\n\nb"])


class VectorFormatTest(unittest.TestCase):
    def test_pgvector_literal(self):
        self.assertEqual(worker.to_pgvector([1, 2.5, -0.25]), "[1.0,2.5,-0.25]")

    def test_integers_become_floats_so_the_type_is_unambiguous(self):
        self.assertTrue(worker.to_pgvector([0, 1]).startswith("[0.0,"))


class EmbedderTest(unittest.TestCase):
    def test_disabled_without_a_url(self):
        with mock.patch.dict(os.environ, {"INGEST_EMBEDDING_URL": ""}, clear=False):
            self.assertFalse(worker.Embedder().enabled)

    def test_a_wrong_width_is_reported_against_the_column_not_the_row(self):
        with mock.patch.dict(
            os.environ,
            {"INGEST_EMBEDDING_URL": "http://x/embed", "INGEST_EMBEDDING_DIMENSIONS": "1024"},
            clear=False,
        ):
            embedder = worker.Embedder()
            payload = json.dumps({"data": [{"embedding": [0.1] * 768}]}).encode()
            response = mock.MagicMock()
            response.read.return_value = payload
            response.__enter__ = lambda s: s
            response.__exit__ = lambda *a: False
            with mock.patch("urllib.request.urlopen", return_value=response):
                with self.assertRaises(RuntimeError) as ctx:
                    embedder.embed(["one"])
        self.assertIn("768", str(ctx.exception))
        self.assertIn("1024", str(ctx.exception))

    def test_a_short_response_is_caught_rather_than_misaligned(self):
        # Silently zipping 3 texts with 2 vectors would attach the wrong
        # embedding to a chunk, which no later check would notice.
        with mock.patch.dict(
            os.environ, {"INGEST_EMBEDDING_URL": "http://x/embed"}, clear=False
        ):
            embedder = worker.Embedder()
            payload = json.dumps({"data": [{"embedding": [0.0] * 1024}]}).encode()
            response = mock.MagicMock()
            response.read.return_value = payload
            response.__enter__ = lambda s: s
            response.__exit__ = lambda *a: False
            with mock.patch("urllib.request.urlopen", return_value=response):
                with self.assertRaises(RuntimeError):
                    embedder.embed(["one", "two"])


# --------------------------------------------------------------------------
# Extraction
# --------------------------------------------------------------------------


class ExtractTest(unittest.TestCase):
    def test_plain_text_passes_through(self):
        self.assertEqual(worker.extract_text("text/plain", b"hello"), "hello")

    def test_undecodable_bytes_do_not_kill_the_job(self):
        self.assertIn("�", worker.extract_text("text/plain", b"\xff\xfe"))

    def test_an_unknown_media_type_is_permanent(self):
        # Permanent, not retryable: the same bytes will have the same type on
        # every attempt, so retrying only spends the budget.
        with self.assertRaises(worker.PermanentError):
            worker.extract_text("image/png", b"\x89PNG")

    def test_a_pdf_without_a_text_layer_names_the_real_cause(self):
        proc = mock.MagicMock(returncode=0, stdout=b"  \n \n", stderr=b"")
        with mock.patch("subprocess.run", return_value=proc):
            with self.assertRaises(worker.PermanentError) as ctx:
                worker.extract_text("application/pdf", b"%PDF-1.4")
        self.assertIn("scanned", str(ctx.exception))

    def test_a_broken_pdf_is_retryable_but_a_scanned_one_is_not(self):
        # The distinction that matters: pdftotext crashing could be a resource
        # problem that clears, so it is retried; a valid PDF with no text
        # never will be, so it is not.
        proc = mock.MagicMock(returncode=1, stdout=b"", stderr=b"Syntax Error")
        with mock.patch("subprocess.run", return_value=proc):
            with self.assertRaises(worker.ExtractionError) as ctx:
                worker.extract_text("application/pdf", b"%PDF-1.4")
        self.assertNotIsInstance(ctx.exception, worker.PermanentError)


# --------------------------------------------------------------------------
# API request handling
# --------------------------------------------------------------------------


class AuthTest(unittest.TestCase):
    def test_no_token_configured_refuses_everything(self):
        # The dangerous default would be to serve everything when the variable
        # is missing, turning a deployment slip into an open write endpoint.
        with mock.patch.dict(os.environ, {"INGEST_API_TOKEN": ""}, clear=False):
            service = api.Service()
        self.assertFalse(service.authorised("Bearer anything"))
        self.assertFalse(service.authorised(None))

    def test_correct_token_only(self):
        with mock.patch.dict(os.environ, {"INGEST_API_TOKEN": "s3cret"}, clear=False):
            service = api.Service()
        self.assertTrue(service.authorised("Bearer s3cret"))
        self.assertFalse(service.authorised("Bearer s3cre"))
        self.assertFalse(service.authorised("s3cret"))
        self.assertFalse(service.authorised("Basic s3cret"))


class PayloadTest(unittest.TestCase):
    def test_text_is_stored_inline(self):
        data, media, inline = api.Service._payload_of({"text": "hallo"})
        self.assertEqual(data, b"hallo")
        self.assertEqual(media, "text/plain")
        self.assertEqual(inline, "hallo")

    def test_json_payload_is_hashed_deterministically(self):
        first, _, _ = api.Service._payload_of({"payload": {"b": 1, "a": 2}})
        second, _, _ = api.Service._payload_of({"payload": {"a": 2, "b": 1}})
        # Key order must not change the content hash, or the same record
        # re-sent would be ingested as a new document every time.
        self.assertEqual(first, second)

    def test_binary_goes_to_storage_not_inline(self):
        import base64 as b64
        encoded = b64.b64encode(b"%PDF-1.4 body").decode()
        data, media, inline = api.Service._payload_of(
            {"content_base64": encoded, "media_type": "application/pdf"}
        )
        self.assertEqual(data, b"%PDF-1.4 body")
        self.assertEqual(media, "application/pdf")
        self.assertIsNone(inline)

    def test_invalid_base64_is_a_client_error(self):
        with self.assertRaises(api.ApiError) as ctx:
            api.Service._payload_of({"content_base64": "not base64!!"})
        self.assertEqual(ctx.exception.status, 400)

    def test_empty_text_is_refused(self):
        with self.assertRaises(api.ApiError):
            api.Service._payload_of({"text": "   "})

    def test_missing_content_names_the_accepted_forms(self):
        with self.assertRaises(api.ApiError) as ctx:
            api.Service._payload_of({"source_id": "x"})
        self.assertIn("content_base64", ctx.exception.message)


class UuidTest(unittest.TestCase):
    def test_accepts_canonical_form(self):
        self.assertTrue(db.is_uuid("6f1d5f92-1c3a-4a5b-9d2e-000000000001"))

    def test_rejects_injection_shaped_input(self):
        self.assertFalse(db.is_uuid("' or 1=1--"))
        self.assertFalse(db.is_uuid(""))
        self.assertFalse(db.is_uuid("6f1d5f92"))


class SearchValidationTest(unittest.TestCase):
    def setUp(self):
        with mock.patch.dict(os.environ, {"INGEST_API_TOKEN": "t"}, clear=False):
            self.service = api.Service()

    def test_tenant_must_be_a_uuid(self):
        with self.assertRaises(api.ApiError) as ctx:
            self.service.search({"tenant": "all", "query": "x"})
        self.assertEqual(ctx.exception.status, 400)

    def test_empty_query_is_refused(self):
        with self.assertRaises(api.ApiError):
            self.service.search(
                {"tenant": "6f1d5f92-1c3a-4a5b-9d2e-000000000001", "query": "  "}
            )

    def test_limit_is_clamped_in_sql_not_in_python(self):
        # A caller asking for a million rows must not be able to make the
        # database materialise them.
        self.assertIn("least(", api.SEARCH_SQL)
        self.assertIn("greatest(", api.SEARCH_SQL)


# --------------------------------------------------------------------------
# Storage
# --------------------------------------------------------------------------


class StorageTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        patcher = mock.patch.object(storage, "_ROOT", self.root)
        self.addCleanup(patcher.stop)
        patcher.start()

    def test_round_trip(self):
        ref = storage.store("tenant-a", "abcdef123456", b"payload")
        self.assertTrue(ref.startswith("file://"))
        self.assertEqual(storage.read(ref), b"payload")

    def test_writing_the_same_hash_twice_is_harmless(self):
        first = storage.store("tenant-a", "abcdef123456", b"payload")
        second = storage.store("tenant-a", "abcdef123456", b"payload")
        self.assertEqual(first, second)
        self.assertEqual(storage.read(first), b"payload")

    def test_no_partial_file_is_left_behind(self):
        storage.store("tenant-a", "abcdef123456", b"payload")
        self.assertEqual(list(self.root.rglob("*.part")), [])

    def test_tenants_do_not_share_a_directory(self):
        a = storage.store("tenant-a", "abcdef123456", b"a")
        b = storage.store("tenant-b", "abcdef123456", b"b")
        self.assertNotEqual(a, b)
        self.assertEqual(storage.read(a), b"a")
        self.assertEqual(storage.read(b), b"b")

    def test_an_unknown_scheme_is_refused(self):
        with self.assertRaises(storage.StorageError):
            storage.read("s3://bucket/key")


# --------------------------------------------------------------------------
# Statement shape — cheap guards on SQL that cannot be run here
# --------------------------------------------------------------------------


class StatementTest(unittest.TestCase):
    def test_accept_is_one_statement_so_dedup_cannot_race(self):
        # Checking for an existing document and then inserting in a second
        # round trip lets two concurrent callers both see "not there".
        self.assertIn("with existing as", api.ACCEPT_SQL)
        self.assertIn("on conflict (idempotency_key) do nothing", api.ACCEPT_SQL)

    def test_claim_skips_locked_rows(self):
        # Without SKIP LOCKED a second worker blocks on the first worker's row
        # instead of taking the next job.
        self.assertIn("for update skip locked", worker.CLAIM_SQL)
        self.assertIn("limit 1", worker.CLAIM_SQL)

    def test_search_only_reads_current_chunks(self):
        self.assertIn("ch.is_current", api.SEARCH_SQL)

    def test_schema_enables_rls_on_every_tenant_table(self):
        schema = (Path(__file__).parent / "sql" / "001_schema.sql").read_text(
            encoding="utf-8"
        )
        for table in (
            "ingest_contract", "source", "raw_document",
            "structured_record", "chunk", "ingest_job",
        ):
            self.assertIn(f"'{table}'", schema, f"{table} missing from the RLS loop")
        self.assertIn("enable row level security", schema)

    def test_schema_cascades_deletes_from_the_raw_layer(self):
        schema = (Path(__file__).parent / "sql" / "001_schema.sql").read_text(
            encoding="utf-8"
        )
        # Orphaned chunks stay findable after their document is deleted, which
        # is the failure that makes a deletion request only look honoured.
        self.assertIn(
            "raw_document_id      uuid not null references ingest.raw_document (id) on delete cascade",
            schema,
        )

    def test_shipped_templates_are_versioned_not_overwritten(self):
        templates = (Path(__file__).parent / "sql" / "002_templates.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("on conflict (key, version) do update", templates)


if __name__ == "__main__":
    unittest.main(verbosity=2)
