#!/bin/sh
#
# End-to-end test for the YouTube side of the ingest layer.
#
#   sh docker/tests/test-ingest-youtube.sh
#
# Runs the real request chain — curl really connects, the client rotation
# really rotates, the caption file is really downloaded and parsed. Only the
# far end is a stand-in (`youtube-stub.py`), so the test does not depend on
# YouTube being reachable or unblocked.
#
# What it proves:
#   * a playlist link becomes one fetch job per video, then chunks
#   * a refused client identity is skipped and the next one succeeds
#   * a video with no captions fails without taking the route down with it
#   * every chunk carries the time span it covers, so an answer can point back
#   * the breaker takes a route out after three failures and `--reset` restores it
#   * an unchanged transcript on a second run is not ingested again

set -e

cd "$(dirname "$0")"

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
json_field() {
    python3 -c "import json,sys; print(json.load(sys.stdin).get('$1',''))"
}

wait_for() {
    # wait_for <sql returning a count> <expected-at-least> <label>
    tries=0
    until [ "$(psql_ "$1")" -ge "$2" ]; do
        tries=$((tries + 1))
        if [ "$tries" -ge 40 ]; then
            fail "$3 (waited 80s)"
            psql_ "select stage, status, attempts, left(coalesce(last_error,''),120) from ingest.ingest_job order by created_at"
            $COMPOSE logs worker | tail -40
            return 1
        fi
        sleep 2
    done
    return 0
}

# --------------------------------------------------------------------------

section "Bring up the stack and the YouTube stand-in"
$COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
$COMPOSE build --quiet
$COMPOSE up -d --wait db
$COMPOSE up -d api worker youtube

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
pass "api healthy, migrations applied"

providers=$(psql_ "select count(*) from ingest.transcript_provider where is_enabled")
[ "$providers" -ge 2 ] && pass "$providers transcript providers in the chain" \
                       || fail "expected at least 2 providers, found $providers"

templates=$(psql_ "select count(*) from ingest.ingest_contract_template where source_type_key like 'youtube%'")
[ "$templates" -ge 3 ] && pass "$templates YouTube templates shipped" \
                       || fail "expected 3 YouTube templates, found $templates"

section "A tenant, then a playlist link"
init_out=$(admin init --tenant acme --template pdf_generic --source-name "Uploads")
TENANT=$(echo "$init_out" | json_field tenant_id)
[ -n "$TENANT" ] || { fail "init returned no tenant"; exit 1; }

yt_out=$(admin youtube --tenant "$TENANT" \
    "https://www.youtube.com/playlist?list=PLstubplaylist01" --language de)
SOURCE=$(echo "$yt_out" | json_field source_id)
kind=$(echo "$yt_out" | json_field kind)
[ "$kind" = "playlist" ] && pass "recognised as a playlist without being told" \
                         || fail "recognised as '$kind'"
[ -n "$SOURCE" ] && pass "source $SOURCE created from the shipped template" \
                 || { fail "no source created: $yt_out"; exit 1; }

section "Discovery turns the playlist into one job per video"
wait_for "select count(*) from ingest.ingest_job where stage = 'fetch'" 3 \
    "discovery never produced fetch jobs" || true
fetch_jobs=$(psql_ "select count(*) from ingest.ingest_job where stage = 'fetch'")
[ "$fetch_jobs" = "3" ] && pass "3 fetch jobs queued, one per video" \
                        || fail "expected 3 fetch jobs, found $fetch_jobs"

section "Transcripts are fetched despite a refused client"
wait_for "select count(*) from ingest.raw_document" 2 \
    "transcripts were never stored" || true
docs=$(psql_ "select count(*) from ingest.raw_document")
[ "$docs" = "2" ] && pass "2 transcripts stored (the third video has none)" \
                  || fail "expected 2 raw documents, found $docs"

via=$(psql_ "select distinct raw_payload->>'fetched_via' from ingest.raw_document")
[ "$via" = "WEB" ] && pass "ANDROID was refused and WEB took over — rotation works" \
                   || fail "fetched via '$via', expected the rotation to land on WEB"

lang=$(psql_ "select distinct raw_payload->>'language' from ingest.raw_document")
[ "$lang" = "de" ] && pass "the requested caption language was honoured" \
                   || fail "language is '$lang', expected de"

automatic=$(psql_ "select distinct raw_payload->>'automatic_captions' from ingest.raw_document")
[ "$automatic" = "false" ] && pass "manual captions preferred over automatic ones" \
                           || fail "picked automatic captions ($automatic)"

section "A video without captions fails alone"
tries=0
until [ -n "$(psql_ "select last_error from ingest.ingest_job where stage='fetch' and last_error is not null limit 1")" ]; do
    tries=$((tries + 1)); [ "$tries" -lt 20 ] || break; sleep 2
done
err=$(psql_ "select last_error from ingest.ingest_job where stage='fetch' and last_error is not null limit 1")
case "$err" in
    *"no caption"*) pass "reported as having no captions" ;;
    "")             fail "the captionless video produced no error at all" ;;
    *)              fail "unexpected error: $err" ;;
esac

# The important half: that failure must not have taken the route down.
health=$(psql_ "select health from ingest.transcript_provider where key='warp_innertube'")
[ "$health" = "ok" ] && pass "the route stayed healthy — a video is not a route" \
                     || fail "provider health is '$health' after one captionless video"

section "Chunks carry the time span they cover"
wait_for "select count(*) from ingest.chunk where is_current" 2 \
    "no chunks were produced from the transcripts" || true
chunks=$(psql_ "select count(*) from ingest.chunk where is_current")
[ "$chunks" -ge 2 ] && pass "$chunks chunks produced" \
                    || fail "expected at least 2 chunks, found $chunks"

spanned=$(psql_ "select count(*) from ingest.chunk where is_current and span ? 'start_ms'")
[ "$spanned" = "$chunks" ] && pass "every chunk has a start and end time" \
                           || fail "only $spanned of $chunks chunks carry a span"

ordered=$(psql_ "select count(*) from ingest.chunk where is_current and (span->>'end_ms')::int > (span->>'start_ms')::int")
[ "$ordered" = "$chunks" ] && pass "and every span ends after it starts" \
                           || fail "$((chunks - ordered)) chunk(s) have a broken span"

meta_ok=$(psql_ "select count(*) from ingest.structured_record where extracted->>'title' is not null")
[ "$meta_ok" -ge 2 ] && pass "video metadata was carried into the record" \
                     || fail "only $meta_ok record(s) carry a title"

section "and it is findable"
hits=$(admin search --tenant "$TENANT" "Turbinenlager" --json \
       | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")
[ "$hits" -gt 0 ] && pass "search for 'Turbinenlager' returned $hits hit(s)" \
                  || fail "the transcript is not findable"

section "Running it again changes nothing"
psql_ "insert into ingest.ingest_job (tenant_id, source_id, stage, payload, idempotency_key)
       select '$TENANT'::uuid, '$SOURCE'::uuid, 'fetch',
              jsonb_build_object('video_id','vid00000001'),
              'rerun-vid00000001'" >/dev/null
sleep 6
docs_after=$(psql_ "select count(*) from ingest.raw_document")
[ "$docs_after" = "2" ] && pass "an unchanged transcript was not ingested twice" \
                        || fail "raw documents grew to $docs_after on a re-fetch"

section "The breaker takes a route out, and --reset puts it back"
psql_ "select ingest.record_provider_result('warp_innertube', false, 'simulated');
       select ingest.record_provider_result('warp_innertube', false, 'simulated');" >/dev/null
degraded=$(psql_ "select health from ingest.transcript_provider where key='warp_innertube'")
[ "$degraded" = "degraded" ] && pass "two failures: degraded, still in the chain" \
                             || fail "health after two failures is '$degraded'"

in_chain=$(psql_ "select count(*) from ingest.transcript_chain() where key='warp_innertube'")
[ "$in_chain" = "1" ] && pass "and still being tried" \
                      || fail "a degraded provider was dropped from the chain too early"

psql_ "select ingest.record_provider_result('warp_innertube', false, 'simulated');" >/dev/null
down=$(psql_ "select health from ingest.transcript_provider where key='warp_innertube'")
[ "$down" = "down" ] && pass "three in a row: down" \
                     || fail "health after three failures is '$down'"

left_chain=$(psql_ "select count(*) from ingest.transcript_chain() where key='warp_innertube'")
[ "$left_chain" = "0" ] && pass "and out of the chain" \
                        || fail "a provider marked down is still being tried"

remaining=$(psql_ "select count(*) from ingest.transcript_chain()")
[ "$remaining" -ge 1 ] && pass "$remaining route(s) left — a blocked route is a degradation, not an outage" \
                       || fail "no routes left at all"

admin providers --reset >/dev/null
restored=$(psql_ "select count(*) from ingest.transcript_chain()")
[ "$restored" -ge 2 ] && pass "--reset put every route back" \
                      || fail "after reset only $restored route(s) are in the chain"

section "Reprocessing under an improved contract"
doc=$(psql_ "select id from ingest.raw_document limit 1")
before=$(psql_ "select count(*) from ingest.chunk where is_current and raw_document_id='$doc'")
admin reprocess --document "$doc" >/dev/null
wait_for "select coalesce(max(generation),0) from ingest.chunk where raw_document_id='$doc'" 2 \
    "reprocessing never produced a second generation" || true

gens=$(psql_ "select count(distinct generation) from ingest.chunk where is_current and raw_document_id='$doc'")
[ "$gens" = "1" ] && pass "exactly one generation current after reprocessing" \
                  || fail "$gens generations current at once"

after=$(psql_ "select count(*) from ingest.chunk where is_current and raw_document_id='$doc'")
[ "$after" = "$before" ] && pass "same chunk count ($after), old generation retired" \
                         || fail "expected $before chunks, found $after"

section "Stalled jobs come back"
psql_ "insert into ingest.ingest_job
         (tenant_id, source_id, stage, status, locked_at, locked_by, attempts, idempotency_key)
       values ('$TENANT'::uuid, '$SOURCE'::uuid, 'fetch', 'running',
               now() - interval '2 hours', 'a-worker-that-died', 1, 'stalled-1')" >/dev/null
revived=$(psql_ "select ingest.requeue_stalled(30)")
[ "$revived" = "1" ] && pass "a job abandoned by a dead worker was requeued" \
                     || fail "requeue_stalled returned $revived"

status=$(psql_ "select status from ingest.ingest_job where idempotency_key='stalled-1'")
[ "$status" = "pending" ] && pass "and is pending again rather than lost" \
                          || fail "requeued job is '$status'"

section "Job retention"
psql_ "update ingest.ingest_job set status='done', updated_at = now() - interval '30 days'
        where status='done'" >/dev/null
pruned=$(psql_ "select ingest.prune_jobs(7, 90)")
[ "$pruned" -ge 1 ] && pass "$pruned finished job(s) older than a week were pruned" \
                    || fail "prune_jobs removed nothing"

kept=$(psql_ "select count(*) from ingest.ingest_job where status in ('dead','failed')")
info "$kept failed job(s) kept as evidence"
