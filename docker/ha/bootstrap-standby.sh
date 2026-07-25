#!/bin/sh
#
# Clones the primary into this node's data directory so Postgres can come up as
# a streaming standby. Runs to completion before the standby's Postgres starts.
#
# If the data directory already holds a cluster, this exits without touching
# it — so restarting the standby resumes replication instead of re-cloning.
#
# Required environment:
#   HA_PRIMARY_HOST          address of the primary, reachable from this host
#   HA_REPLICATION_PASSWORD  password of the replication role
# Optional:
#   HA_PRIMARY_PORT          (default 5432)
#   HA_REPLICATION_USER      (default supabase_replicator)
#   HA_REPLICATION_SLOT      (default derived from HA_NODE_NAME)
#   HA_BOOTSTRAP_TIMEOUT     seconds to wait for the primary (default 300)
#   PGDATA                   (default /var/lib/postgresql/data)

set -e

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
PRIMARY_HOST="${HA_PRIMARY_HOST:?HA_PRIMARY_HOST must be set}"
PRIMARY_PORT="${HA_PRIMARY_PORT:-5432}"
REPL_USER="${HA_REPLICATION_USER:-supabase_replicator}"
REPL_PASSWORD="${HA_REPLICATION_PASSWORD:?HA_REPLICATION_PASSWORD must be set}"
NODE_NAME="${HA_NODE_NAME:-standby}"
# Slot names allow letters, digits and underscore only.
DEFAULT_SLOT=$(printf '%s' "$NODE_NAME" | tr -c 'a-zA-Z0-9_' '_')
REPL_SLOT="${HA_REPLICATION_SLOT:-${DEFAULT_SLOT}_slot}"
TIMEOUT="${HA_BOOTSTRAP_TIMEOUT:-300}"

info() { echo "[bootstrap] $*"; }
die() { echo "[bootstrap] ERROR: $*" >&2; exit 1; }

if [ -f "$PGDATA/PG_VERSION" ]; then
    if [ -f "$PGDATA/standby.signal" ]; then
        info "data directory already holds a standby — resuming replication"
    else
        info "data directory already holds a cluster — leaving it untouched"
        info "(to re-clone from the primary, wipe the volume first)"
    fi
    exit 0
fi

# Waits for the exact kind of connection pg_basebackup will make.
#
# pg_isready only proves the server is listening, and a logical connection
# (replication=database) proves nothing either: Postgres matches those against
# ordinary `host all all` rules, which most setups already have. Only
# replication=true is matched against the `host replication` rule this clone
# depends on. Checking the wrong one reports success and then fails the clone.
# The primary also applies a reloaded pg_hba.conf asynchronously, so a rule
# added moments ago may not be live yet.
info "waiting for primary ${PRIMARY_HOST}:${PRIMARY_PORT} to accept replication (up to ${TIMEOUT}s)"
waited=0
while :; do
    # Keeps stderr (the reason it failed) and discards the result row.
    if last_error=$(PGPASSWORD="$REPL_PASSWORD" psql \
            "postgresql://${REPL_USER}@${PRIMARY_HOST}:${PRIMARY_PORT}/postgres?replication=true" \
            -X -A -t -c "IDENTIFY_SYSTEM" 2>&1 >/dev/null); then
        break
    fi
    waited=$((waited + 3))
    if [ "$waited" -ge "$TIMEOUT" ]; then
        echo "[bootstrap] last error: $last_error" >&2
        die "primary did not accept a replication connection within ${TIMEOUT}s"
    fi
    sleep 3
done
info "primary accepts replication connections"

mkdir -p "$PGDATA"
chown postgres:postgres "$PGDATA"
chmod 0700 "$PGDATA"

# -R writes standby.signal and primary_conninfo, so the cluster starts in
# recovery and connects straight back to the primary.
# -X stream keeps a second connection open for WAL, so the clone is
# self-contained even if it takes a while.
run_basebackup() {
    su postgres -c "\
PGPASSWORD='$REPL_PASSWORD' PGAPPNAME='$NODE_NAME' \
pg_basebackup \
  --host='$PRIMARY_HOST' --port='$PRIMARY_PORT' --username='$REPL_USER' \
  --pgdata='$PGDATA' \
  --write-recovery-conf \
  --wal-method=stream \
  --slot='$REPL_SLOT' $1 \
  --checkpoint=fast \
  --progress --verbose"
}

info "cloning primary into $PGDATA (slot: $REPL_SLOT)"
if ! run_basebackup --create-slot; then
    info "could not create slot '$REPL_SLOT' — assuming it already exists, retrying"
    # The failed attempt may have left a partial directory behind. Clearing it
    # is safe: we only reach this point when PGDATA held no cluster.
    find "$PGDATA" -mindepth 1 -delete
    chown postgres:postgres "$PGDATA"
    chmod 0700 "$PGDATA"
    run_basebackup "" || die "pg_basebackup failed"
fi

[ -f "$PGDATA/standby.signal" ] || die "standby.signal missing — the clone is not a standby"
info "clone complete, node will start in recovery"

# pgsodium encrypts Vault secrets and TCE columns with a key stored outside the
# data directory, so pg_basebackup does not bring it along. Without the
# primary's key this standby holds ciphertext it cannot read.
KEY_FILE=/etc/postgresql-custom/pgsodium_root.key
if [ -f "$KEY_FILE" ]; then
    info "pgsodium root key is present"
else
    echo "[bootstrap] WARNING: $KEY_FILE is missing on this standby." >&2
    echo "[bootstrap]          If the primary uses pgsodium (Vault secrets, encrypted" >&2
    echo "[bootstrap]          columns), copy its key over before relying on this node:" >&2
    echo "[bootstrap]            primary:  sh run.sh ha export-key > key.b64" >&2
    echo "[bootstrap]            standby:  sh run.sh ha import-key < key.b64" >&2
fi

info "done"
