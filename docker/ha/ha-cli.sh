#!/bin/sh
#
# High-availability commands. Reached through `sh run.sh ha <command>`.
#
# Run from the docker/ directory. Every command works on the .env of the
# server it runs on, so the same commands apply on a primary and a standby.

set -e

cd "$(dirname "$0")/.."

AGENT_PORT_DEFAULT=8008

info() { echo "  $*"; }
warn() { echo "WARNING: $*" >&2; }
die() { echo "ERROR: $*" >&2; exit 1; }

[ -f docker-compose.yml ] || die "run this from the docker/ directory"

env_get() {
    [ -f .env ] || return 0
    grep "^${1}=" .env 2>/dev/null | head -n1 | cut -d= -f2- | tr -d "\r\"'"
}

env_set() {
    key="$1"
    value="$2"
    [ -f .env ] || die ".env not found in $(pwd)"
    if grep -q "^${key}=" .env; then
        sed -i.bak -e "s|^${key}=.*$|${key}=${value}|" .env
        rm -f .env.bak
    else
        printf '%s=%s\n' "$key" "$value" >> .env
    fi
}

gen_token() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex 32
    else
        LC_ALL=C tr -dc 'a-f0-9' < /dev/urandom | head -c 64
    fi
}

# GET/POST against an agent, with the API token attached.
agent_call() {
    method="$1"
    url="$2"
    token=$(env_get HA_API_TOKEN)
    if command -v curl >/dev/null 2>&1; then
        curl -sS --max-time 10 -X "$method" \
            -H "Authorization: Bearer $token" "$url"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- --timeout=10 --method="$method" \
            --header="Authorization: Bearer $token" "$url"
    else
        die "neither curl nor wget is available"
    fi
}

# Pretty-prints agent JSON when python3 is around, otherwise leaves it raw.
format_status() {
    if command -v python3 >/dev/null 2>&1; then
        python3 -c '
import json, sys

try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit("  (no valid response)")

role = d.get("role", "?")
lines = ["  role=" + role]

if role == "primary":
    replicas = d.get("replicas") or []
    lines[0] += "  replicas=%d" % len(replicas)
    for r in replicas:
        lines.append("    <- %s  %s  lag=%sB" % (
            r.get("client_addr"), r.get("state"), r.get("lag_bytes")))
elif role == "standby":
    lines[0] += "  streaming=%s  lag=%ss / %sB" % (
        d.get("streaming"), d.get("lag_seconds"), d.get("lag_bytes"))

if d.get("last_error"):
    lines.append("    last_error: %s" % d["last_error"])
if d.get("error"):
    lines.append("    error: %s" % d["error"])

print("\n".join(lines))
'
    else
        cat
        echo ""
    fi
}

usage() {
    cat <<'EOF'
Usage: sh run.sh ha <command>

  init                Prepare this server for HA: generate the API token and
                      point POSTGRES_HOST at the router
  setup-primary       Create the replication role and open pg_hba (run on the primary)
  status              Show the role and lag of every configured node
  promote [url]       Promote a standby to primary (default: this server's agent)
  nodes               List the nodes the router knows about
  nodes add <name> <host> <pgport> [agentport]
                      Register another node with the router
  export-key          Print the pgsodium root key, base64 (run on the primary)
  import-key          Read a base64 pgsodium key from stdin (run on the standby)
  router-reload       Restart the router after changing the node list

EOF
}

CMD="${1:-help}"
[ "$#" -gt 0 ] && shift

case "$CMD" in
    init)
        [ -f .env ] || die ".env not found — copy .env.example first"

        token=$(env_get HA_API_TOKEN)
        if [ -z "$token" ]; then
            token=$(gen_token)
            env_set HA_API_TOKEN "$token"
            info "generated HA_API_TOKEN"
        else
            info "HA_API_TOKEN already set"
        fi
        info "every node and witness in this group must share that token:"
        info "  HA_API_TOKEN=$token"

        node_name=$(env_get HA_NODE_NAME)
        if [ -z "$node_name" ]; then
            env_set HA_NODE_NAME "node-a"
            info "set HA_NODE_NAME=node-a"
        fi

        current_host=$(env_get POSTGRES_HOST)
        if [ "$current_host" = "pg-router" ]; then
            info "POSTGRES_HOST already points at the router"
        else
            env_set POSTGRES_HOST "pg-router"
            info "POSTGRES_HOST: $current_host -> pg-router"
            info "this is what makes a failover invisible to the other services"
        fi

        if [ -z "$(env_get HA_NODES)" ]; then
            env_set HA_NODES "$(env_get HA_NODE_NAME || echo node-a)=db:$(env_get POSTGRES_PORT || echo 5432):8008"
            info "seeded HA_NODES with this node"
        fi

        echo ""
        info "next:"
        info "  1. sh run.sh config add ha"
        info "  2. sh run.sh start"
        info "  3. sh run.sh ha setup-primary"
        info "  4. set up the standby server, then: sh run.sh ha nodes add <name> <host> <pgport>"
        ;;

    setup-primary)
        exec sh ha/setup-primary.sh "$@"
        ;;

    status)
        nodes=$(env_get HA_NODES)
        if [ -z "$nodes" ]; then
            port=$(env_get HA_AGENT_PORT)
            echo "node: local"
            agent_call GET "http://127.0.0.1:${port:-$AGENT_PORT_DEFAULT}/status" | format_status
            exit 0
        fi
        OLD_IFS=$IFS
        IFS=,
        for entry in $nodes; do
            IFS=$OLD_IFS
            name=${entry%%=*}
            addr=${entry#*=}
            host=$(echo "$addr" | cut -d: -f1)
            agent_port=$(echo "$addr" | cut -d: -f3)
            # `db` is the router's name for the local node; from the host it is
            # reachable on the published agent port instead.
            [ "$host" = "db" ] && host=127.0.0.1
            echo "node: $name ($host:${agent_port:-$AGENT_PORT_DEFAULT})"
            agent_call GET "http://${host}:${agent_port:-$AGENT_PORT_DEFAULT}/status" \
                | format_status || echo "  (unreachable)"
            IFS=,
        done
        IFS=$OLD_IFS
        ;;

    promote)
        target="$1"
        if [ -z "$target" ]; then
            port=$(env_get HA_AGENT_PORT)
            target="http://127.0.0.1:${port:-$AGENT_PORT_DEFAULT}"
        fi
        echo "Promoting $target to primary."
        echo "Only do this when the old primary is really gone — two primaries"
        echo "accepting writes at once will diverge and cannot be merged back."
        printf "Type 'promote' to continue: "
        read -r answer
        [ "$answer" = "promote" ] || die "aborted"
        agent_call POST "${target%/}/promote" | format_status
        ;;

    nodes)
        sub="${1:-show}"
        [ "$#" -gt 0 ] && shift
        case "$sub" in
            show)
                echo "HA_NODES=$(env_get HA_NODES)"
                ;;
            add)
                [ $# -ge 3 ] || die "Usage: sh run.sh ha nodes add <name> <host> <pgport> [agentport]"
                name="$1"; host="$2"; pgport="$3"; agentport="${4:-$AGENT_PORT_DEFAULT}"
                current=$(env_get HA_NODES)
                case ",$current," in
                    *",$name="*) die "node '$name' is already registered" ;;
                esac
                new="${current:+$current,}${name}=${host}:${pgport}:${agentport}"
                env_set HA_NODES "$new"
                info "HA_NODES=$new"
                info "apply it with: sh run.sh ha router-reload"
                ;;
            *)
                die "unknown nodes subcommand: $sub"
                ;;
        esac
        ;;

    export-key)
        docker compose exec -T "${HA_DB_SERVICE:-db}" \
            sh -c 'test -f /etc/postgresql-custom/pgsodium_root.key' 2>/dev/null \
            || die "no pgsodium root key on this node"
        docker compose exec -T "${HA_DB_SERVICE:-db}" \
            base64 -w0 /etc/postgresql-custom/pgsodium_root.key
        echo ""
        ;;

    import-key)
        key_b64=$(cat)
        [ -n "$key_b64" ] || die "no key on stdin"
        # Written through a throwaway container so it works whether or not the
        # standby's Postgres is running.
        vol="${HA_DB_CONFIG_VOLUME:-supabase-standby_standby-db-config}"
        docker volume inspect "$vol" >/dev/null 2>&1 \
            || die "volume '$vol' not found — set HA_DB_CONFIG_VOLUME to the right name (docker volume ls)"
        printf '%s' "$key_b64" | docker run --rm -i \
            -v "$vol:/etc/postgresql-custom" alpine:3 \
            sh -c 'base64 -d > /etc/postgresql-custom/pgsodium_root.key \
                   && chmod 0600 /etc/postgresql-custom/pgsodium_root.key \
                   && chown 105:106 /etc/postgresql-custom/pgsodium_root.key 2>/dev/null; true'
        info "key written to volume '$vol'"
        warn "restart the standby database so Postgres picks it up: docker compose -f standby.compose.yml restart db"
        ;;

    router-reload)
        docker compose up -d --force-recreate --no-deps pg-router
        info "router restarted with the current HA_NODES"
        ;;

    help|-h|--help)
        usage
        ;;
    *)
        echo "Unknown ha command: $CMD" >&2
        usage
        exit 1
        ;;
esac
