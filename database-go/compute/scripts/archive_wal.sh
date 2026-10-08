#!/bin/bash
# archive_command: push a completed WAL segment to safekeeper via TCP QWAL (qwal).
set -euo pipefail

PATH_FILE="${1:?}"
FILE_NAME="${2:?}"
SAFEKEEPER_ADDR="${SAFEKEEPER_ADDR:-127.0.0.1:5454}"
TENANT="${TENANT_ID:-default}"
TIMELINE="${TIMELINE_ID:-main}"
SEGSIZE=$((16 * 1024 * 1024))

if [[ ! "$FILE_NAME" =~ ([0-9A-Fa-f]{8})([0-9A-Fa-f]{8})([0-9A-Fa-f]{8})$ ]]; then
  echo "archive_wal: bad filename $FILE_NAME" >&2
  exit 1
fi
LOGID=$((16#${BASH_REMATCH[2]}))
SEG=$((16#${BASH_REMATCH[3]}))
START_LSN=$((LOGID * 4294967296 + SEG * SEGSIZE))
END_LSN=$((START_LSN + SEGSIZE))

exec qwal \
  -safekeeper="$SAFEKEEPER_ADDR" \
  -tenant="$TENANT" \
  -timeline="$TIMELINE" \
  -start-lsn="$START_LSN" \
  -end-lsn="$END_LSN" \
  -file="$PATH_FILE"
