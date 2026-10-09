#!/bin/sh
set -eu

# Initialize once in the parent, before worker imports create metric files.
if [ -n "${PROMETHEUS_MULTIPROC_DIR:-}" ]; then
    mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
    rm -f "$PROMETHEUS_MULTIPROC_DIR"/*.db
fi
exec "$@"
