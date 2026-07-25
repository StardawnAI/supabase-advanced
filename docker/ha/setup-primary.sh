#!/bin/sh
#
# Prepares the local Postgres node to serve streaming standbys.
#
# Run this once on the primary server, from the docker/ directory:
#   sh ha/setup-primary.sh
#
# It creates the replication role, opens pg_hba for replication connections,
# verifies the WAL settings, and prints everything a standby needs to attach.
# Running it again is safe — every step is idempotent.

set -e

cd "$(dirname "$0")/.."

DB_SERVICE="${HA_DB_SERVICE:-db}"
REPL_USER="${HA_REPLICATION_USER:-supabase_replicator}"
# Which addresses may open replication connections. Narrow this to the standby
# subnet when you can; the default relies on the password plus your firewall.
REPL_CIDR="${HA_REPLICATION_CIDR:-0.0.0.0/0}"

info() { echo "  $*"; }
warn() { echo "WARNING: $*" >&2; }
die() { echo "ERROR: $*" >&2; exit 1; }

[ -f docker-compose.yml ] || die "run this from the docker/ directory"
[ -f .env ] || die ".env not found in $(pwd)"

# Runs SQL as superuser over the container's unix socket.
psql_su() {
    docker compose exec -T "$DB_SERVICE" \
        psql -X -q -A -t -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"
}

gen_password() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex 24
    else
        LC_ALL=C tr -dc 'a-f0-9' < /dev/urandom | head -c 48
    fi
}

docker compose ps --status running --services 2>/dev/null | grep -qx "$DB_SERVICE" \
    || die "service '$DB_SERVICE' is not running — start the stack first (sh run.sh start)"

echo "==> Replication password"
REPL_PASSWORD=$(grep '^HA_REPLICATION_PASSWORD=' .env 2>/dev/null | head -n1 | cut -d= -f2- | tr -d "\r\"'")
if [ -z "$REPL_PASSWORD" ]; then
    REPL_PASSWORD=$(gen_password)
    cat >> .env <<EOF

############
# High availability — password for the streaming replication role.
# The standby server needs this value in its own .env.
############
HA_REPLICATION_PASSWORD=$REPL_PASSWORD
EOF
    info "generated a new password and appended HA_REPLICATION_PASSWORD to .env"
else
    info "reusing HA_REPLICATION_PASSWORD from .env"
fi

echo "==> Replication role '$REPL_USER'"
# The name and password go in as psql variables and are quoted by format(),
# so a value containing quotes cannot break out of the statement.
docker compose exec -T "$DB_SERVICE" \
    psql -X -q -A -t -U postgres -d postgres -v ON_ERROR_STOP=1 \
        -v repl_user="$REPL_USER" -v repl_password="$REPL_PASSWORD" -f - <<'SQL' >/dev/null
SELECT format(
  CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'repl_user')
       THEN 'ALTER ROLE %I WITH REPLICATION LOGIN PASSWORD %L'
       ELSE 'CREATE ROLE %I WITH REPLICATION LOGIN PASSWORD %L'
  END, :'repl_user', :'repl_password') \gexec
SQL
info "role is present with REPLICATION LOGIN"

echo "==> WAL configuration"
WAL_LEVEL=$(psql_su -c "SHOW wal_level")
MAX_SENDERS=$(psql_su -c "SHOW max_wal_senders")
MAX_SLOTS=$(psql_su -c "SHOW max_replication_slots")
info "wal_level=$WAL_LEVEL max_wal_senders=$MAX_SENDERS max_replication_slots=$MAX_SLOTS"
case "$WAL_LEVEL" in
    replica|logical) ;;
    *) die "wal_level must be 'replica' or 'logical' for streaming replication (got '$WAL_LEVEL')" ;;
esac
[ "$MAX_SENDERS" -ge 1 ] 2>/dev/null || die "max_wal_senders must be at least 1 (got '$MAX_SENDERS')"
[ "$MAX_SLOTS" -ge 1 ] 2>/dev/null || die "max_replication_slots must be at least 1 (got '$MAX_SLOTS')"

echo "==> pg_hba.conf"
HBA_FILE=$(psql_su -c "SHOW hba_file")
[ -n "$HBA_FILE" ] || die "could not determine hba_file"
HBA_RULE="host replication $REPL_USER $REPL_CIDR scram-sha-256"
if docker compose exec -T "$DB_SERVICE" grep -qF "replication $REPL_USER $REPL_CIDR" "$HBA_FILE"; then
    info "replication rule already present in $HBA_FILE"
else
    docker compose exec -T -u root "$DB_SERVICE" sh -c \
        "printf '\n# Added by supabase-advanced ha/setup-primary.sh\n%s\n' '$HBA_RULE' >> '$HBA_FILE'"
    info "appended to $HBA_FILE: $HBA_RULE"
fi
psql_su -c "SELECT pg_reload_conf()" >/dev/null
info "configuration reloaded"

echo "==> Verifying"
# A reloaded pg_hba.conf is applied asynchronously, so poll instead of assuming.
# The connection goes over TCP to the service name, which is what a remote
# standby does — connecting over localhost would hit a different, laxer rule.
# replication=true is the physical kind pg_basebackup uses, and the only kind
# matched against the `host replication` rule added above.
verified=false
attempt=0
while [ "$attempt" -lt 10 ]; do
    if docker compose exec -T -e PGPASSWORD="$REPL_PASSWORD" "$DB_SERVICE" psql \
        "postgresql://${REPL_USER}@${DB_SERVICE}:${POSTGRES_PORT:-5432}/postgres?replication=true" \
        -X -A -t -c "IDENTIFY_SYSTEM" >/dev/null 2>&1; then
        verified=true
        break
    fi
    attempt=$((attempt + 1))
    sleep 2
done
if [ "$verified" = true ]; then
    info "replication connections are accepted"
else
    warn "could not open a replication connection yet. The standby retries on its"
    warn "own, so this is often just timing — but if the clone keeps failing, check"
    warn "the role, the pg_hba rule and the firewall between the servers."
fi

[ "$REPL_CIDR" = "0.0.0.0/0" ] && warn \
    "replication is open to every address. Restrict it with HA_REPLICATION_CIDR, or make sure the Postgres port is only reachable over a private network or VPN."

echo "==> pgsodium root key"
if docker compose exec -T "$DB_SERVICE" test -f /etc/postgresql-custom/pgsodium_root.key 2>/dev/null; then
    info "present — the standby MUST use the same key, or data encrypted with it"
    info "(Vault secrets, TCE columns) becomes unreadable after a failover."
    info "Copy it with: sh run.sh ha export-key   (then import it on the standby)"
else
    info "no pgsodium root key on this node — nothing to copy"
fi

echo ""
echo "Primary is ready to serve standbys."
echo ""
echo "On the standby server, set these in docker/.env:"
echo "  HA_PRIMARY_HOST=<this server's address reachable from the standby>"
echo "  HA_PRIMARY_PORT=<the published Postgres port, see HA_DB_REPLICATION_PORT>"
echo "  HA_REPLICATION_PASSWORD=$REPL_PASSWORD"
echo ""
echo "Then start the standby with:"
echo "  docker compose -f ha/docker-compose.standby.yml up -d"
echo ""
