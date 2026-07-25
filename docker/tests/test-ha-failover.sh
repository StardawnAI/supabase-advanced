#!/bin/sh
#
# End-to-end test for streaming replication and automatic failover.
#
# It proves the five things the feature claims:
#   1. a standby clones the primary and streams its WAL continuously,
#   2. each agent reports the role its node actually has,
#   3. the router sends writes to the primary and reads to the standby,
#   4. losing the primary promotes the standby on its own, and
#   5. the router follows the new primary with committed data intact.
#
# Everything runs in an isolated compose project on one host; nothing is
# published to the host and no existing stack is touched.
#
# Usage:
#   cd docker/
#   sh tests/test-ha-failover.sh
#
# Options:
#   HA_TEST_IMAGE=supabase/postgres:15.8.1.048   use an image you already have
#   HA_TEST_KEEP=1                               leave the stack up for poking
#   HA_TEST_FAILOVER_MODE=manual                 check that manual mode waits

set -eu

cd "$(dirname "$0")"

COMPOSE_FILE=ha-failover.compose.yml
PG_PASSWORD="${HA_TEST_PG_PASSWORD:-testpass}"
REPL_PASSWORD="${HA_TEST_REPL_PASSWORD:-replpass}"
REPL_USER=supabase_replicator
FAILURES=0
CHECKS=0

dc() { docker compose -f "$COMPOSE_FILE" "$@"; }

step() { echo ""; echo "=== $* ==="; }
pass() { CHECKS=$((CHECKS + 1)); echo "  PASS  $*"; }
fail() { CHECKS=$((CHECKS + 1)); FAILURES=$((FAILURES + 1)); echo "  FAIL  $*"; }

check_eq() { # description expected actual
    if [ "$2" = "$3" ]; then
        pass "$1"
    else
        fail "$1 (expected '$2', got '$3')"
    fi
}

# Runs SQL on a node and returns the bare result.
sql() { # service statement
    svc="$1"
    shift
    dc exec -T -e PGPASSWORD="$PG_PASSWORD" "$svc" \
        psql -X -q -A -t -U postgres -d postgres -h 127.0.0.1 \
        -v ON_ERROR_STOP=1 -c "$*" 2>/dev/null | tr -d '\r' | head -n1
}

# Runs SQL through the router, the way an application would.
sql_via_router() { # port statement
    port="$1"
    shift
    dc exec -T -e PGPASSWORD="$PG_PASSWORD" client \
        psql -X -q -A -t -U postgres -d postgres -h router -p "$port" \
        -v ON_ERROR_STOP=1 -c "$*" 2>/dev/null | tr -d '\r' | head -n1
}

# True once the primary accepts the physical replication connection a clone
# makes. pg_reload_conf() returns before the postmaster has applied the new
# pg_hba.conf, so this has to be polled. replication=true matters: a logical
# connection (replication=database) is matched against ordinary `host all all`
# rules and would pass while the clone still fails.
replication_ready() {
    dc exec -T -e PGPASSWORD="$REPL_PASSWORD" client \
        psql "postgresql://$REPL_USER@primary-db:5432/postgres?replication=true" \
        -X -A -t -c "IDENTIFY_SYSTEM" >/dev/null 2>&1
}

# HTTP status of an agent endpoint, 0 when it cannot be reached.
http_status() { # url
    dc exec -T witness python3 -c '
import sys, urllib.request, urllib.error
try:
    print(urllib.request.urlopen(sys.argv[1], timeout=5).status)
except urllib.error.HTTPError as exc:
    print(exc.code)
except Exception:
    print(0)
' "$1" 2>/dev/null | tr -d '\r' | head -n1
}

# Promotes a node through its agent — the same call `run.sh ha promote` makes,
# token and all, rather than reaching into Postgres directly.
agent_promote() { # host
    dc exec -T witness python3 -c '
import sys, urllib.request, urllib.error
req = urllib.request.Request(sys.argv[1], method="POST",
                             headers={"Authorization": "Bearer " + sys.argv[2]})
try:
    print(urllib.request.urlopen(req, timeout=70).status)
except urllib.error.HTTPError as exc:
    print(exc.code)
except Exception:
    print(0)
' "http://$1:8008/promote" "${HA_TEST_TOKEN:-testtoken}" 2>/dev/null | tr -d '\r' | head -n1
}

# Polls a shell condition until it holds or the deadline passes.
wait_for() { # description seconds condition...
    desc="$1"
    deadline="$2"
    shift 2
    waited=0
    while [ "$waited" -lt "$deadline" ]; do
        if eval "$@" >/dev/null 2>&1; then
            echo "  ok    $desc (after ${waited}s)"
            return 0
        fi
        sleep 2
        waited=$((waited + 2))
    done
    echo "  TIMEOUT after ${deadline}s waiting for: $desc"
    return 1
}

cleanup() {
    code=$?
    if [ "$code" -ne 0 ] || [ "$FAILURES" -ne 0 ]; then
        step "Logs (the run did not come out clean)"
        dc logs --tail 25 standby-bootstrap 2>/dev/null || true
        dc logs --tail 40 standby-agent 2>/dev/null || true
        dc logs --tail 20 router 2>/dev/null || true
    fi
    if [ "${HA_TEST_KEEP:-0}" = "1" ]; then
        echo ""
        echo "HA_TEST_KEEP=1 — leaving the stack up."
        echo "Tear it down with: docker compose -f tests/$COMPOSE_FILE down -v"
    else
        step "Cleaning up"
        dc down -v --remove-orphans >/dev/null 2>&1 || true
        echo "  done"
    fi
}
trap cleanup EXIT

step "Starting the primary"
dc down -v --remove-orphans >/dev/null 2>&1 || true
dc up -d --build --wait primary-db primary-agent witness router client
echo "  primary is up: $(sql primary-db 'SELECT version()' | cut -c1-40)"

step "Preparing the primary to serve standbys"
dc exec -T -e PGPASSWORD="$PG_PASSWORD" primary-db \
    psql -X -q -U postgres -d postgres -h 127.0.0.1 -v ON_ERROR_STOP=1 \
    -v repl_user="$REPL_USER" -v repl_password="$REPL_PASSWORD" -f - <<'EOSQL' >/dev/null
SELECT format(
  CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'repl_user')
       THEN 'ALTER ROLE %I WITH REPLICATION LOGIN PASSWORD %L'
       ELSE 'CREATE ROLE %I WITH REPLICATION LOGIN PASSWORD %L'
  END, :'repl_user', :'repl_password') \gexec
EOSQL
HBA_FILE=$(sql primary-db 'SHOW hba_file')
dc exec -T -u root primary-db sh -c \
    "grep -q 'replication $REPL_USER' '$HBA_FILE' || printf '\nhost replication $REPL_USER 0.0.0.0/0 scram-sha-256\n' >> '$HBA_FILE'"
# pg_reload_conf() needs superuser and the `postgres` role is not one in
# Supabase, so signal the postmaster instead.
dc exec -T -u root primary-db sh -c '
    set -e
    data_dir=$(psql -U postgres -h 127.0.0.1 -X -A -t -c "SHOW data_directory")
    kill -HUP "$(head -1 "$data_dir/postmaster.pid")"
'
echo "  replication role and pg_hba rule in place ($HBA_FILE)"
wait_for "the primary to accept replication connections" 30 replication_ready \
    || fail "primary never accepted a replication connection"
check_eq "wal_level allows streaming replication" "logical" "$(sql primary-db 'SHOW wal_level')"

step "Seeding data before the standby exists"
sql primary-db "CREATE TABLE IF NOT EXISTS public.ha_test (
    id serial PRIMARY KEY, note text NOT NULL, at timestamptz DEFAULT now())" >/dev/null
sql primary-db "INSERT INTO public.ha_test (note) VALUES ('seeded-before-clone')" >/dev/null
check_eq "seed row is on the primary" "1" \
    "$(sql primary-db "SELECT count(*) FROM public.ha_test WHERE note='seeded-before-clone'")"

step "Cloning the primary into a standby"
dc up -d --build --wait standby-db standby-agent
check_eq "standby is running in recovery" "t" "$(sql standby-db 'SELECT pg_is_in_recovery()')"
check_eq "the pre-clone row came across" "1" \
    "$(sql standby-db "SELECT count(*) FROM public.ha_test WHERE note='seeded-before-clone'")"
check_eq "standby has a WAL receiver running" "1" \
    "$(sql standby-db 'SELECT count(*) FROM pg_stat_wal_receiver')"
check_eq "primary sees the standby streaming" "streaming" \
    "$(sql primary-db "SELECT state FROM pg_stat_replication WHERE application_name='standby'")"
check_eq "a replication slot is holding WAL for it" "1" \
    "$(sql primary-db "SELECT count(*) FROM pg_replication_slots WHERE active")"

step "Replicating a live write"
sql primary-db "INSERT INTO public.ha_test (note) VALUES ('replicated-live')" >/dev/null
if wait_for "the write to reach the standby" 20 \
    "[ \"\$(sql standby-db \"SELECT count(*) FROM public.ha_test WHERE note='replicated-live'\")\" = 1 ]"; then
    pass "writes replicate continuously"
else
    fail "writes replicate continuously"
fi
echo "  replay lag: $(sql standby-db 'SELECT COALESCE(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::numeric(10,3), 0)')s"

step "Agents report the roles their nodes actually have"
check_eq "primary answers /primary with 200" "200" "$(http_status http://primary-db:8008/primary)"
check_eq "primary is not offered as a read replica" "503" "$(http_status http://primary-db:8008/replica)"
check_eq "standby does not claim to be primary" "503" "$(http_status http://standby-db:8008/primary)"
check_eq "standby is offered for reads" "200" "$(http_status http://standby-db:8008/replica)"
check_eq "witness stays out of the routing" "503" "$(http_status http://witness:8008/primary)"
check_eq "witness reports itself healthy" "200" "$(http_status http://witness:8008/health)"

step "The router follows the agents"
check_eq "writes through the router land on the primary" "f" "$(sql_via_router 5432 'SELECT pg_is_in_recovery()')"
check_eq "reads through the router land on the standby" "t" "$(sql_via_router 5433 'SELECT pg_is_in_recovery()')"
sql_via_router 5432 "INSERT INTO public.ha_test (note) VALUES ('through-router')" >/dev/null
check_eq "the router write was accepted" "1" \
    "$(sql primary-db "SELECT count(*) FROM public.ha_test WHERE note='through-router'")"

BEFORE_FAILOVER=$(sql primary-db 'SELECT count(*) FROM public.ha_test')
echo "  rows committed before the failover: $BEFORE_FAILOVER"

step "Killing the primary"
dc stop -t 0 primary-db primary-agent >/dev/null
echo "  primary stopped — the standby and the witness both lose sight of it"

MODE="${HA_TEST_FAILOVER_MODE:-auto}"
if [ "$MODE" = "auto" ]; then
    step "Waiting for the standby to promote itself"
    if wait_for "the standby to become primary" 90 \
        "[ \"\$(sql standby-db 'SELECT pg_is_in_recovery()')\" = f ]"; then
        pass "the standby promoted itself once the witness agreed"
    else
        fail "the standby promoted itself once the witness agreed"
    fi
else
    step "Manual mode: the standby must NOT promote itself"
    sleep 25
    check_eq "standby is still a standby" "t" "$(sql standby-db 'SELECT pg_is_in_recovery()')"
    check_eq "an unauthenticated promote is refused" "401" \
        "$(HA_TEST_TOKEN=wrong-token agent_promote standby-db)"
    check_eq "standby is still a standby after that" "t" "$(sql standby-db 'SELECT pg_is_in_recovery()')"
    echo "  promoting it through the agent, the way an operator would"
    check_eq "the operator's promote succeeded" "200" "$(agent_promote standby-db)"
fi

check_eq "the new primary accepts writes" "f" "$(sql standby-db 'SELECT pg_is_in_recovery()')"
check_eq "it still answers /primary with 200" "200" "$(http_status http://standby-db:8008/primary)"

step "The router follows the failover"
if wait_for "the router to route to the new primary" 60 \
    "[ \"\$(sql_via_router 5432 'SELECT pg_is_in_recovery()')\" = f ]"; then
    pass "the router switched without any config change"
else
    fail "the router switched without any config change"
fi

step "No data was lost"
check_eq "every committed row survived" "$BEFORE_FAILOVER" \
    "$(sql standby-db 'SELECT count(*) FROM public.ha_test')"
sql_via_router 5432 "INSERT INTO public.ha_test (note) VALUES ('after-failover')" >/dev/null
check_eq "new writes go through the router to the new primary" "1" \
    "$(sql standby-db "SELECT count(*) FROM public.ha_test WHERE note='after-failover'")"
check_eq "the pre-failover history is intact" "1" \
    "$(sql standby-db "SELECT count(*) FROM public.ha_test WHERE note='seeded-before-clone'")"

step "Result"
echo "  $((CHECKS - FAILURES))/$CHECKS checks passed"
if [ "$FAILURES" -ne 0 ]; then
    echo "  $FAILURES FAILED"
    exit 1
fi
echo "  replication and automatic failover both work"
