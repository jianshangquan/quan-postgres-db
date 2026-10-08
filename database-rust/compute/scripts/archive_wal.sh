#!/bin/bash
# Archives a WAL segment to the safekeeper HTTP gateway.
# Usage: archive_wal.sh <path> <filename>
set -euo pipefail

PATH_FILE="${1:?}"
FILE_NAME="${2:?}"
SAFEKEEPER_HTTP="${SAFEKEEPER_HTTP:-http://safekeeper-1:8080}"
TENANT="${TENANT_ID:-default}"
TIMELINE="${TIMELINE_ID:-main}"

# Derive a synthetic LSN range from the segment filename when possible (000000010000000000000001).
START_LSN=0
END_LSN=0
if [[ "$FILE_NAME" =~ ([0-9A-Fa-f]{8})([0-9A-Fa-f]{8})([0-9A-Fa-f]{8})$ ]]; then
  # Rough mapping for demo: use last 8 hex digits as end offset.
  END_LSN=$((16#${BASH_REMATCH[3]}))
  START_LSN=$END_LSN
fi

curl -fsS -X POST \
  "${SAFEKEEPER_HTTP}/wal?tenant=${TENANT}&timeline=${TIMELINE}&start_lsn=${START_LSN}&end_lsn=${END_LSN}" \
  --data-binary @"${PATH_FILE}" \
  -o /dev/null

exit 0
