#!/bin/sh
#
# Checks that everything this fork adds is still present.
#
# Reads .github/fork/protected-paths.txt and fails if a file we own has
# vanished, or if a hook we placed inside an upstream file is no longer there.
#
# Run it after any upstream merge — the automated sync does this for you, and
# stops rather than pushing when it fails:
#
#   sh .github/fork/verify-intact.sh
#
# An upstream merge cannot normally delete files upstream has never heard of,
# so most of this is a safety net. The marker checks are the ones that earn
# their keep: those live in files upstream edits too, and a conflict resolved
# the wrong way — by hand or by a merge driver — drops them silently.

set -eu

cd "$(dirname "$0")/../.."

MANIFEST=.github/fork/protected-paths.txt
[ -f "$MANIFEST" ] || { echo "ERROR: $MANIFEST not found" >&2; exit 1; }

missing=0
checked=0

# Trims leading and trailing whitespace.
trim() {
    printf '%s' "$1" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

while IFS= read -r line || [ -n "$line" ]; do
    case "$(trim "$line")" in
        ''|'#'*) continue ;;
    esac

    case "$line" in
        *'|'*)
            path=$(trim "${line%%|*}")
            marker=$(trim "${line#*|}")
            ;;
        *)
            path=$(trim "$line")
            marker=""
            ;;
    esac

    checked=$((checked + 1))

    if [ ! -f "$path" ]; then
        echo "MISSING FILE     $path"
        missing=$((missing + 1))
        continue
    fi

    if [ -n "$marker" ]; then
        # -F: the markers are literal strings, not patterns.
        if ! grep -qF "$marker" "$path"; then
            echo "MISSING MARKER   $path"
            echo "                 expected to find: $marker"
            missing=$((missing + 1))
            continue
        fi
    fi

    echo "ok               $path"
done < "$MANIFEST"

echo ""
if [ "$missing" -ne 0 ]; then
    echo "$missing of $checked checks FAILED — the fork's own changes are not intact."
    echo ""
    echo "This usually means an upstream merge was resolved in a way that dropped"
    echo "them. Restore the missing pieces before pushing. To see what happened:"
    echo "  git log --oneline -5"
    echo "  git diff HEAD~1 -- <the path above>"
    exit 1
fi

echo "$checked/$checked present — the fork's changes survived."
