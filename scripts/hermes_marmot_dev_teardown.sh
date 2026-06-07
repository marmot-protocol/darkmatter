#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage: scripts/hermes_marmot_dev_teardown.sh [options]

Stops generated background processes, then deletes the isolated Hermes Marmot
development root.

Options:
  --root PATH      Dev root (default: ${TMPDIR:-/tmp}/hermes-marmot-test)
  --dry-run        Print what would be removed
  --force          Do not prompt before deletion
  -h, --help       Show this help
USAGE
}

default_tmp="${TMPDIR:-/tmp}"
dev_root="${HERMES_MARMOT_DEV_ROOT:-${default_tmp%/}/hermes-marmot-test}"
dry_run=0
force=0

while [ "$#" -gt 0 ]; do
    case "$1" in
        --root)
            dev_root="$2"
            shift 2
            ;;
        --dry-run)
            dry_run=1
            shift
            ;;
        --force)
            force=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [ -z "$dev_root" ]; then
    echo "error: empty dev root" >&2
    exit 1
fi

dev_parent="$(dirname "$dev_root")"
dev_base="$(basename "$dev_root")"
if [ ! -d "$dev_parent" ]; then
    echo "nothing to remove: $dev_root"
    exit 0
fi
dev_root="$(cd "$dev_parent" && pwd)/$dev_base"

case "$dev_root" in
    "/"|"$HOME"|"$PWD")
        echo "error: refusing to delete unsafe root: $dev_root" >&2
        exit 1
        ;;
esac

if [ ! -e "$dev_root" ]; then
    echo "nothing to remove: $dev_root"
    exit 0
fi

if [ -x "$dev_root/stop-dev-processes.sh" ]; then
    "$dev_root/stop-dev-processes.sh" || true
fi

if [ "$dry_run" -eq 1 ]; then
    echo "would remove: $dev_root"
    exit 0
fi

if [ "$force" -ne 1 ]; then
    printf 'Delete %s? [y/N] ' "$dev_root" >&2
    read -r answer
    case "$answer" in
        y|Y|yes|YES)
            ;;
        *)
            echo "aborted"
            exit 1
            ;;
    esac
fi

rm -rf "$dev_root"
echo "removed: $dev_root"
