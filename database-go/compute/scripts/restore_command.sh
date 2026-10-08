#!/bin/bash
# restore_command: fetch one WAL segment payload from safekeeper HTTP into %p.
set -euo pipefail

DEST="${1:?}"
FILE_NAME="${2:?}"
SAFEKEEPER_HTTP="${SAFEKEEPER_HTTP:-http://safekeeper:8080}"
TENANT="${TENANT_ID:-default}"
TIMELINE="${TIMELINE_ID:-main}"
SEGSIZE=$((16 * 1024 * 1024))

if [[ ! "$FILE_NAME" =~ ^([0-9A-Fa-f]{8})([0-9A-Fa-f]{8})([0-9A-Fa-f]{8})$ ]]; then
  echo "restore_command: bad filename $FILE_NAME" >&2
  exit 1
fi
LOGID=$((16#${BASH_REMATCH[2]}))
SEG=$((16#${BASH_REMATCH[3]}))
START_LSN=$((LOGID * 4294967296 + SEG * SEGSIZE))
SEGNO=$((START_LSN / SEGSIZE))

AUTH=()
if [ -n "${QUAN_STORAGE_TOKEN:-}" ]; then
  AUTH=(-H "Authorization: Bearer ${QUAN_STORAGE_TOKEN}" -H "X-Quan-Token: ${QUAN_STORAGE_TOKEN}")
fi

tmp="${DEST}.tmp"
if ! curl -fsS "${AUTH[@]}" \
  "${SAFEKEEPER_HTTP}/wal?tenant=${TENANT}&timeline=${TIMELINE}&segno=${SEGNO}" \
  -o "$tmp"; then
  echo "restore_command: fetch failed for $FILE_NAME (segno=$SEGNO)" >&2
  rm -f "$tmp"
  exit 1
fi

# Postgres expects a full 16MiB segment file; pad short streamed payloads.
size=$(wc -c <"$tmp" | tr -d ' ')
if [ "$size" -lt "$SEGSIZE" ]; then
  dd if=/dev/zero bs=1 count=$((SEGSIZE - size)) status=none >>"$tmp"
fi
mv "$tmp" "$DEST"
exit 0
