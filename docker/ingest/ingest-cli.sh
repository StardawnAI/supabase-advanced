#!/bin/sh
#
# Ingest layer commands. Reached through `sh run.sh ingest <command>`.
#
# Run from the docker/ directory. Everything that touches the database is in
# admin.py inside the container; this wrapper only handles .env, the token,
# and getting files into the container.

set -e

cd "$(dirname "$0")/.."

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

# Runs admin.py in the api container. -T because most of these are piped.
admin() {
    docker compose exec -T ingest-api python3 /app/admin.py "$@"
}

running() {
    docker compose ps --status running --services 2>/dev/null \
        | grep -qx ingest-api
}

require_running() {
    running || die "ingest-api is not running — 'sh run.sh start' first"
}

usage() {
    cat <<'EOF'
Usage: sh run.sh ingest <command>

Setup
  init [name]           generate a token, create a tenant and a first source
                        (name defaults to "default")
  enable                add the ingest overlay to COMPOSE_FILE

Look around
  status                service health and counts
  templates             contract templates shipped with the stack
  tenants               tenants and their retention
  sources <tenant-id>   configured sources
  jobs <tenant-id> [status]   recent jobs (pending|running|done|failed|dead)

Documents
  push <source-id> <file>     ingest a file
  search <tenant-id> <query>  search current chunks

YouTube
  youtube <tenant-id> <link>  add a video, playlist or channel and start it
  providers [--reset]         transcript routes and their health
  warp on|off|status          route transcript requests through Cloudflare WARP

Maintenance
  reprocess <document-id>            build one document again
  reprocess-contract <contract-id>   build everything still on an older version
  reembed <tenant-id>   fill in vectors for chunks stored without them
  prune [--dry-run]     apply retention to documents and finished jobs
  test                  run the unit tests

EOF
}

CMD="${1:-help}"
[ $# -gt 0 ] && shift

case "$CMD" in
    enable)
        sh run.sh config add ingest
        ;;

    init)
        name="${1:-default}"

        token=$(env_get INGEST_API_TOKEN)
        if [ -z "$token" ]; then
            token=$(gen_token)
            env_set INGEST_API_TOKEN "$token"
            info "generated INGEST_API_TOKEN"
        else
            info "INGEST_API_TOKEN already set, keeping it"
        fi

        case "$(env_get COMPOSE_FILE)" in
            *docker-compose.ingest.yml*) ;;
            *)
                sh run.sh config add ingest
                info "added the ingest overlay to COMPOSE_FILE"
                ;;
        esac

        if ! running; then
            info "starting the ingest services"
            docker compose up -d --wait ingest-api ingest-worker
        fi

        info "creating tenant '$name' and a first source"
        admin init --tenant "$name" --template pdf_generic --source-name "Uploads"
        echo ""
        info "Push a document:  sh run.sh ingest push <source-id> file.pdf"
        info "Then search it:   sh run.sh ingest search <tenant-id> 'some words'"
        ;;

    status)
        port=$(env_get INGEST_PORT)
        [ -n "$port" ] || port=8010
        if command -v curl >/dev/null 2>&1; then
            curl -sS --max-time 10 "http://127.0.0.1:${port}/health"
        elif command -v wget >/dev/null 2>&1; then
            wget -qO- --timeout=10 "http://127.0.0.1:${port}/health"
        else
            die "neither curl nor wget available"
        fi
        echo ""
        ;;

    templates)
        require_running
        admin templates
        ;;

    tenants)
        require_running
        admin tenants
        ;;

    sources)
        require_running
        [ -n "$1" ] || die "usage: sh run.sh ingest sources <tenant-id>"
        admin sources --tenant "$1"
        ;;

    jobs)
        require_running
        [ -n "$1" ] || die "usage: sh run.sh ingest jobs <tenant-id> [status]"
        if [ -n "$2" ]; then
            admin jobs --tenant "$1" --status "$2"
        else
            admin jobs --tenant "$1"
        fi
        ;;

    push)
        require_running
        source_id="$1"
        file="$2"
        [ -n "$source_id" ] && [ -n "$file" ] \
            || die "usage: sh run.sh ingest push <source-id> <file>"
        [ -f "$file" ] || die "no such file: $file"

        # Media type from the extension. Deliberately a short list: an
        # unknown type should be named explicitly rather than guessed at.
        case "$file" in
            *.pdf)              media="application/pdf" ;;
            *.txt|*.md)         media="text/plain" ;;
            *.json)             media="application/json" ;;
            *)
                die "cannot tell the type of '$file' — supported: .pdf .txt .md .json"
                ;;
        esac

        admin push --source "$source_id" --media-type "$media" \
            --uri "$(basename "$file")" < "$file"
        ;;

    search)
        require_running
        [ $# -ge 2 ] || die "usage: sh run.sh ingest search <tenant-id> <query>"
        tenant="$1"
        shift
        # The rest of the line is the query, so it works with or without quotes.
        admin search --tenant "$tenant" "$*"
        ;;

    youtube)
        require_running
        [ $# -ge 2 ] || die "usage: sh run.sh ingest youtube <tenant-id> <link-or-id>"
        tenant="$1"
        shift
        admin youtube --tenant "$tenant" "$1"
        ;;

    providers)
        require_running
        if [ "$1" = "--reset" ]; then
            admin providers --reset
        else
            admin providers
        fi
        ;;

    warp)
        case "${1:-status}" in
            on)
                case "$(env_get COMPOSE_FILE)" in
                    *docker-compose.warp.yml*) info "warp overlay already enabled" ;;
                    *) sh run.sh config add warp ;;
                esac
                # The container name is fixed in the overlay, so the workers
                # can address it without knowing anything generated.
                env_set INGEST_PROXY_URL "socks5h://supabase-warp:1080"
                info "INGEST_PROXY_URL=socks5h://supabase-warp:1080"
                info "now run: sh run.sh start"
                ;;
            off)
                sh run.sh config remove warp || true
                env_set INGEST_PROXY_URL ""
                info "proxy disabled — the provider chain falls through to the direct routes"
                info "now run: sh run.sh start"
                ;;
            status)
                if ! docker compose ps --status running --services 2>/dev/null | grep -qx warp; then
                    die "the warp container is not running — 'sh run.sh ingest warp on' then 'sh run.sh start'"
                fi
                # Cloudflare's own answer to "am I coming through WARP", which
                # is the only check that distinguishes a proxy that is up from
                # a proxy that is actually tunnelling.
                docker compose exec -T warp \
                    curl -s --socks5-hostname 127.0.0.1:1080 \
                    https://www.cloudflare.com/cdn-cgi/trace \
                    | grep -E '^(warp|ip|loc)=' || die "WARP is not answering through the proxy"
                ;;
            *)
                die "usage: sh run.sh ingest warp on|off|status"
                ;;
        esac
        ;;

    reprocess)
        require_running
        [ -n "$1" ] || die "usage: sh run.sh ingest reprocess <document-id>"
        admin reprocess --document "$1"
        ;;

    reprocess-contract)
        require_running
        [ -n "$1" ] || die "usage: sh run.sh ingest reprocess-contract <contract-id>"
        admin reprocess --contract "$1" --behind
        ;;

    reembed)
        require_running
        [ -n "$1" ] || die "usage: sh run.sh ingest reembed <tenant-id>"
        admin reembed --tenant "$1"
        ;;

    prune)
        require_running
        if [ "$1" = "--dry-run" ]; then
            admin prune --dry-run
        else
            admin prune
        fi
        ;;

    test)
        if command -v python3 >/dev/null 2>&1; then
            cd ingest && python3 -m unittest discover -s . -v
        else
            docker compose exec -T ingest-api \
                python3 -m unittest discover -s /app -v
        fi
        ;;

    help|-h|--help)
        usage
        ;;
    *)
        echo "Unknown ingest command: $CMD" >&2
        usage
        exit 1
        ;;
esac
