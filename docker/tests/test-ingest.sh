#!/bin/sh
#
# End-to-end test for the ingest layer.
#
#   sh docker/tests/test-ingest.sh
#
# Brings up an isolated Postgres plus the ingest services, pushes a real PDF
# through the whole pipeline and checks that what comes out the other end is
# findable. Everything it creates is destroyed at the end; it never touches a
# running stack.
#
# What it proves, beyond "no crash":
#   * a PDF is extracted, chunked and indexed without a model server
#   * the same bytes twice do not produce a second document
#   * reprocessing switches generations without ever showing both
#   * row level security actually confines a non-owner role to its tenant
#   * a scanned PDF fails with the reason, instead of storing nothing quietly

set -e

cd "$(dirname "$0")"

# Git Bash on Windows rewrites arguments that look like absolute paths, so
# `/app/admin.py` reaches the container as `C:/Program Files/Git/app/...`.
# Unknown and harmless everywhere else.
export MSYS_NO_PATHCONV=1

COMPOSE="docker compose -f ingest.compose.yml"
PASS=0
FAIL=0

pass() { PASS=$((PASS + 1)); echo "  PASS  $*"; }
fail() { FAIL=$((FAIL + 1)); echo "  FAIL  $*" >&2; }
info() { echo "  ....  $*"; }
section() { echo ""; echo "== $* =="; }

cleanup() {
    status=$?
    section "Cleanup"
    $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
    
    echo "  torn down"
    echo ""
    echo "-------------------------------------------"
    echo "  $PASS passed, $FAIL failed"
    # `set -e` means a command that simply dies never reaches its check, so a
    # non-zero exit with nothing marked failed still has to read as a failure
    # rather than as a clean run that happened to stop early.
    if [ "$status" -ne 0 ] && [ "$FAIL" -eq 0 ]; then
        echo "  ABORTED — a step exited $status before it could be checked"
    fi
    echo "-------------------------------------------"
    [ "$FAIL" -eq 0 ] && [ "$status" -eq 0 ] || exit 1
    exit 0
}
trap cleanup EXIT INT TERM

admin() { $COMPOSE exec -T api python3 /app/admin.py "$@"; }
psql_() {
    $COMPOSE exec -T db psql -X -q -A -t -U postgres -d postgres -v ON_ERROR_STOP=1 -c "$1"
}

# A minimal but genuinely valid PDF, written by hand so the test does not need
# a PDF library. `pdftotext` has to parse this for the test to mean anything.
#
# Built inside the container rather than on the host: the host's python may be
# a Windows one, for which "/tmp/x.pdf" is not the path the shell means. Doing
# it container-side makes the test behave the same everywhere.
write_pdf() {
    target="$1"
    body="$2"
    $COMPOSE exec -T api python3 - "$target" "$body" <<'PY'
import sys

target, body = sys.argv[1], sys.argv[2]
lines = body.split("|")
text = "BT /F1 12 Tf 72 720 Td 14 TL\n"
for line in lines:
    escaped = line.replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")
    text += f"({escaped}) Tj T*\n"
text += "ET"
stream = text.encode("latin-1")

objects = [
    b"<< /Type /Catalog /Pages 2 0 R >>",
    b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
    b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
    b"/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
    b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream",
    b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
]

out = bytearray(b"%PDF-1.4\n")
offsets = []
for i, obj in enumerate(objects, start=1):
    offsets.append(len(out))
    out += f"{i} 0 obj\n".encode() + obj + b"\nendobj\n"

xref_at = len(out)
out += f"xref\n0 {len(objects) + 1}\n".encode()
out += b"0000000000 65535 f \n"
for off in offsets:
    out += f"{off:010d} 00000 n \n".encode()
out += (
    f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
    f"startxref\n{xref_at}\n%%EOF\n"
).encode()

open(target, "wb").write(bytes(out))
PY
}

json_field() {
    # Reads one field out of a JSON object on stdin. Keeps the test readable
    # without depending on jq being installed.
    python3 -c "import json,sys; print(json.load(sys.stdin).get('$1',''))"
}

# --------------------------------------------------------------------------

section "Bring up an isolated stack"
$COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
$COMPOSE build --quiet
$COMPOSE up -d --wait db
info "postgres up"
$COMPOSE up -d api worker

# The services apply the migrations themselves on start.
tries=0
until $COMPOSE exec -T api python3 -c "
import urllib.request, sys
try:
    urllib.request.urlopen('http://127.0.0.1:8010/health', timeout=3)
except Exception:
    sys.exit(1)
" >/dev/null 2>&1; do
    tries=$((tries + 1))
    [ "$tries" -lt 40 ] || { fail "api never became healthy"; $COMPOSE logs api | tail -30; exit 1; }
    sleep 2
done
pass "api is healthy and the schema was migrated on start"

section "Schema"
tables=$(psql_ "select count(*) from information_schema.tables where table_schema='ingest'")
[ "$tables" -ge 9 ] && pass "ingest schema has $tables tables" \
                    || fail "expected at least 9 tables, found $tables"

rls=$(psql_ "select count(*) from pg_tables where schemaname='ingest' and rowsecurity")
[ "$rls" -ge 7 ] && pass "row level security enabled on $rls tables" \
                 || fail "expected RLS on at least 7 tables, found $rls"

# Re-running must not be a way to lose data or fail.
applied=$($COMPOSE exec -T api python3 -c "
import sys; sys.path.insert(0, '/app')
from pathlib import Path
from db import Db
print(len(Db().migrate(Path('/app/sql'))))
")
[ "$applied" = "0" ] && pass "migrations are idempotent (0 re-applied)" \
                     || fail "re-running migrations applied $applied files again"

templates=$(admin templates | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")
[ "$templates" -ge 3 ] && pass "$templates contract templates shipped" \
                       || fail "expected at least 3 templates, found $templates"

section "Create a tenant and a source"
init_out=$(admin init --tenant acme --template pdf_generic --source-name "Uploads")
TENANT=$(echo "$init_out" | json_field tenant_id)
SOURCE=$(echo "$init_out" | json_field source_id)
[ -n "$TENANT" ] && [ -n "$SOURCE" ] && pass "tenant $TENANT, source $SOURCE" \
                                     || { fail "init did not return ids: $init_out"; exit 1; }

# --------------------------------------------------------------------------

section "A PDF goes in"
write_pdf /tmp/report.pdf \
    "Quarterly maintenance report|The turbine bearing was replaced on Tuesday.|Vibration levels returned to normal afterwards.|No further action is required this quarter."

push_pdf() {
    $COMPOSE exec -T api sh -c \
        "python3 /app/admin.py push --source '$1' --media-type application/pdf --uri '$2' < '$3'"
}

push_out=$(push_pdf "$SOURCE" report.pdf /tmp/report.pdf)
DOC=$(echo "$push_out" | json_field raw_document_id)
created=$(echo "$push_out" | json_field created)
[ -n "$DOC" ] && [ "$created" = "True" ] && pass "accepted as document $DOC" \
                                        || fail "unexpected push result: $push_out"

info "waiting for the worker"
tries=0
until [ "$(psql_ "select count(*) from ingest.chunk where is_current")" -gt 0 ]; do
    tries=$((tries + 1))
    if [ "$tries" -ge 30 ]; then
        fail "no chunks after 60s"
        psql_ "select status, attempts, last_error from ingest.ingest_job"
        $COMPOSE logs worker | tail -30
        exit 1
    fi
    sleep 2
done

chunks=$(psql_ "select count(*) from ingest.chunk where is_current")
pass "$chunks searchable chunk(s) produced"

job_status=$(psql_ "select status from ingest.ingest_job limit 1")
[ "$job_status" = "done" ] && pass "job finished as 'done'" \
                           || fail "job status is '$job_status'"

text_ok=$(psql_ "select count(*) from ingest.structured_record where text_content ilike '%turbine bearing%'")
[ "$text_ok" = "1" ] && pass "pdftotext extracted the real text" \
                     || fail "extracted text does not contain the expected words"

fts_ok=$(psql_ "select count(*) from ingest.chunk where is_current and fts is not null")
[ "$fts_ok" -gt 0 ] && pass "full text vectors were built" \
                    || fail "chunks have no fts vector"

no_vectors=$(psql_ "select count(*) from ingest.chunk where embedding is not null")
[ "$no_vectors" = "0" ] && pass "no embeddings, as configured — still searchable" \
                        || fail "embeddings appeared without an endpoint configured"

section "and comes out findable"
hits=$(admin search --tenant "$TENANT" "turbine bearing" --json \
       | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")
[ "$hits" -gt 0 ] && pass "search for 'turbine bearing' returned $hits hit(s)" \
                  || fail "search returned nothing"

misses=$(admin search --tenant "$TENANT" "helicopter" --json \
         | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")
[ "$misses" = "0" ] && pass "an unrelated search returns nothing" \
                    || fail "search matched a word that is not in the document"

# --------------------------------------------------------------------------

section "The same bytes twice"
again=$(push_pdf "$SOURCE" report.pdf /tmp/report.pdf)
again_created=$(echo "$again" | json_field created)
again_doc=$(echo "$again" | json_field raw_document_id)
[ "$again_created" = "False" ] && pass "reported as already known" \
                              || fail "a duplicate was reported as new"
[ "$again_doc" = "$DOC" ] && pass "returned the original document id" \
                          || fail "duplicate created a second document"
docs=$(psql_ "select count(*) from ingest.raw_document")
[ "$docs" = "1" ] && pass "still exactly one raw document" \
                  || fail "$docs raw documents after ingesting the same file twice"

section "Reprocessing switches generations atomically"
before=$(psql_ "select count(*) from ingest.chunk where is_current")
$COMPOSE exec -T api python3 -c "
import sys; sys.path.insert(0, '/app')
from db import Db
Db().execute('''
    insert into ingest.ingest_job
        (tenant_id, source_id, raw_document_id, stage, idempotency_key)
    select (data->>'t')::uuid, (data->>'s')::uuid, (data->>'d')::uuid,
           'extract', 'e2e-reprocess-1'
      from _payload
''', {'t': '$TENANT', 's': '$SOURCE', 'd': '$DOC'})
" >/dev/null

tries=0
until [ "$(psql_ "select coalesce(max(generation),0) from ingest.chunk")" -ge 2 ]; do
    tries=$((tries + 1))
    [ "$tries" -lt 30 ] || { fail "reprocessing never produced a second generation"; break; }
    sleep 2
done

gens=$(psql_ "select count(distinct generation) from ingest.chunk where is_current")
[ "$gens" = "1" ] && pass "only one generation is current after reprocessing" \
                  || fail "$gens generations are current at once — searches would see duplicates"

current_gen=$(psql_ "select distinct generation from ingest.chunk where is_current")
[ "$current_gen" = "2" ] && pass "the current generation is the new one (2)" \
                         || fail "current generation is $current_gen, expected 2"

after=$(psql_ "select count(*) from ingest.chunk where is_current")
[ "$after" = "$before" ] && pass "chunk count unchanged ($after), old generation retired" \
                         || fail "expected $before current chunks, found $after"

total=$(psql_ "select count(*) from ingest.chunk")
[ "$total" -gt "$after" ] && pass "the previous generation is still on disk, just not current" \
                          || fail "the old generation vanished instead of being retired"

# --------------------------------------------------------------------------

section "Row level security"
psql_ "insert into ingest.tenant (id, name) values ('11111111-1111-4111-8111-111111111111', 'other')" >/dev/null
psql_ "grant usage on schema ingest to authenticated; grant select on ingest.chunk to authenticated" >/dev/null

own=$(psql_ "set role authenticated;
             select set_config('ingest.tenant_id', '$TENANT', false);
             select count(*) from ingest.chunk;" | tail -1)
[ "$own" -gt 0 ] && pass "a confined role sees its own tenant's chunks ($own)" \
                 || fail "a confined role cannot see its own data"

other=$(psql_ "set role authenticated;
               select set_config('ingest.tenant_id', '11111111-1111-4111-8111-111111111111', false);
               select count(*) from ingest.chunk;" | tail -1)
[ "$other" = "0" ] && pass "and none of another tenant's" \
                   || fail "LEAK: another tenant's role saw $other chunks"

unset_=$(psql_ "set role authenticated;
                select set_config('ingest.tenant_id', '', false);
                select count(*) from ingest.chunk;" | tail -1)
[ "$unset_" = "0" ] && pass "and nothing at all without a tenant set" \
                    || fail "LEAK: $unset_ chunks visible with no tenant context"

section "Deleting a document takes its chunks with it"
psql_ "delete from ingest.raw_document where id = '$DOC'" >/dev/null
orphans=$(psql_ "select count(*) from ingest.chunk")
[ "$orphans" = "0" ] && pass "no orphaned chunks survive the delete" \
                     || fail "$orphans chunks outlived their document and stay searchable"

section "A scanned PDF fails with the reason"
# The same generator, but the page draws no text — which is exactly what a
# scanned page looks like to a text extractor.
write_pdf /tmp/scan.pdf ""
push_pdf "$SOURCE" scan.pdf /tmp/scan.pdf >/dev/null

tries=0
until [ -n "$(psql_ "select last_error from ingest.ingest_job where last_error is not null limit 1")" ]; do
    tries=$((tries + 1))
    [ "$tries" -lt 20 ] || break
    sleep 2
done
err=$(psql_ "select last_error from ingest.ingest_job where last_error is not null limit 1")
case "$err" in
    *scanned*|*OCR*) pass "reported as needing OCR rather than stored empty" ;;
    "")              fail "no error was recorded for an unextractable PDF" ;;
    *)               fail "unexpected error text: $err" ;;
esac

# Marked dead at once rather than retried: the same file extracts to the same
# nothing every time, so five attempts with growing backoff would only spend
# the retry budget to be told the same thing again.
retry=$(psql_ "select status from ingest.ingest_job where last_error is not null limit 1")
[ "$retry" = "dead" ] && pass "and given up on immediately instead of retried five times" \
                      || fail "job is '$retry', expected 'dead' for an unextractable file"

attempts=$(psql_ "select attempts from ingest.ingest_job where last_error is not null limit 1")
[ "$attempts" = "1" ] && pass "after exactly one attempt" \
                      || fail "took $attempts attempts to give up on a permanent failure"
