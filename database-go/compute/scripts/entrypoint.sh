#!/bin/bash
# Ephemeral compute for database-go-2:
#   first boot → initdb + quansync seed
#   later boots → quanrestore (basebackup + stubs) + GetPage-native smgr
set -euo pipefail

export PGDATA="${PGDATA:-/var/lib/postgresql/data}"
export PAGESERVER_HTTP="${PAGESERVER_HTTP:-https://pageserver:8081}"
export SAFEKEEPER_ADDR="${SAFEKEEPER_ADDR:-safekeeper-1:5454}"
export SAFEKEEPER_ADDRS="${SAFEKEEPER_ADDRS:-$SAFEKEEPER_ADDR}"
export SAFEKEEPER_HTTP="${SAFEKEEPER_HTTP:-https://safekeeper-1:8080}"
export TENANT_ID="${TENANT_ID:-default}"
export TIMELINE_ID="${TIMELINE_ID:-main}"
export POSTGRES_USER="${POSTGRES_USER:-postgres}"
if [ -z "${POSTGRES_PASSWORD:-}" ]; then
  echo "POSTGRES_PASSWORD is required" >&2
  exit 1
fi
if [ -z "${QUAN_STORAGE_TOKEN:-}" ]; then
  echo "QUAN_STORAGE_TOKEN is required" >&2
  exit 1
fi
export POSTGRES_DB="${POSTGRES_DB:-app}"
export PGURL="${PGURL:-postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:5432/postgres?sslmode=disable}"
export PATH="/usr/local/pgsql/bin:${PATH}"
export QUAN_TLS_CA="${QUAN_TLS_CA:-${TLS_CA_FILE:-}}"

# GetPage-first smgr hangs clients when pageserver is slow. Opt-in only.
# Image ENV may still set QUAN_PAGESERVER_HTTP — ignore unless explicitly enabled.
if [ "${QUAN_ENABLE_GETPAGE:-}" = "1" ]; then
  export QUAN_PAGESERVER_HTTP="${QUAN_PAGESERVER_HTTP:-$PAGESERVER_HTTP}"
  if [ -z "${QUAN_PAGESERVER_HTTP_URLS:-}" ]; then
    export QUAN_PAGESERVER_HTTP_URLS="${QUAN_PAGESERVER_HTTP},${PAGESERVER_HTTP}"
  fi
else
  unset QUAN_PAGESERVER_HTTP || true
  unset QUAN_PAGESERVER_HTTP_URLS || true
fi

HELPER_PIDS=()

cleanup() {
  echo "entrypoint: shutting down helpers + postgres..."
  if gosu postgres pg_isready -U "$POSTGRES_USER" >/dev/null 2>&1; then
    if [ "${QUAN_DISABLE_PERIODIC_SYNC:-}" != "1" ]; then
      gosu postgres env PGDATA="$PGDATA" PAGESERVER_HTTP="$PAGESERVER_HTTP" \
        QUAN_STORAGE_TOKEN="$QUAN_STORAGE_TOKEN" TENANT_ID="$TENANT_ID" TIMELINE_ID="$TIMELINE_ID" \
        quansync -pgdata="$PGDATA" || true
    fi
    gosu postgres pg_ctl -D "$PGDATA" -m fast stop -t 20 || true
  fi
  for p in "${HELPER_PIDS[@]:-}"; do
    kill "$p" 2>/dev/null || true
  done
}
trap cleanup SIGTERM SIGINT

mkdir -p "$PGDATA" /var/run/postgresql
chown -R postgres:postgres "$PGDATA" /var/run/postgresql

curl_tls_args() {
  if [ -n "${QUAN_TLS_CA:-}" ] && [ -f "${QUAN_TLS_CA}" ]; then
    echo --cacert "${QUAN_TLS_CA}"
  elif [[ "${1:-}" == https://* ]]; then
    echo -k
  fi
}

curl_auth() {
  local tls
  tls=$(curl_tls_args "${1:-}")
  # shellcheck disable=SC2086
  if [ -n "${QUAN_STORAGE_TOKEN:-}" ]; then
    curl -fsS $tls -H "Authorization: Bearer ${QUAN_STORAGE_TOKEN}" -H "X-Quan-Token: ${QUAN_STORAGE_TOKEN}" "$@"
  else
    curl -fsS $tls "$@"
  fi
}

wait_http() {
  local url="$1" name="$2"
  local tls
  tls=$(curl_tls_args "$url")
  for _ in $(seq 1 90); do
    # shellcheck disable=SC2086
    if curl -fsS $tls "$url" >/dev/null 2>&1; then
      echo "$name ready"
      return 0
    fi
    sleep 1
  done
  echo "timeout waiting for $name at $url" >&2
  return 1
}

wait_http "${PAGESERVER_HTTP}/healthz" pageserver
wait_http "${SAFEKEEPER_HTTP}/healthz" safekeeper

has_remote() {
  local body pc
  body=$(curl_auth "${PAGESERVER_HTTP}/v1/timeline?tenant=${TENANT_ID}&timeline=${TIMELINE_ID}" 2>/dev/null || echo '{}')
  pc=$(echo "$body" | sed -n 's/.*"page_count":\([0-9]*\).*/\1/p')
  if [ -n "$pc" ] && [ "$pc" -gt 0 ]; then
    return 0
  fi
  if curl_auth "${PAGESERVER_HTTP}/v1/basebackup?tenant=${TENANT_ID}&timeline=${TIMELINE_ID}" -o /dev/null 2>/dev/null; then
    return 0
  fi
  return 1
}

fetch_request_lsn() {
  local body lsn
  body=$(curl_auth "${PAGESERVER_HTTP}/v1/timeline?tenant=${TENANT_ID}&timeline=${TIMELINE_ID}" 2>/dev/null || echo '{}')
  lsn=$(echo "$body" | sed -n 's/.*"last_lsn":\([0-9]*\).*/\1/p')
  if [ -n "$lsn" ] && [ "$lsn" != "0" ]; then
    export QUAN_REQUEST_LSN="$lsn"
    echo "QUAN_REQUEST_LSN=$QUAN_REQUEST_LSN"
  fi
}

start_helpers() {
  if [ "${QUAN_EXTERNAL_WALSTREAMER:-}" = "1" ]; then
    echo "walstreamer disabled in-process (QUAN_EXTERNAL_WALSTREAMER=1)"
  else
    gosu postgres env \
      PGDATA="$PGDATA" \
      SAFEKEEPER_ADDRS="$SAFEKEEPER_ADDRS" \
      QUAN_STORAGE_TOKEN="$QUAN_STORAGE_TOKEN" \
      QUORUM="${QUORUM:-2}" \
      TENANT_ID="$TENANT_ID" TIMELINE_ID="$TIMELINE_ID" \
      PGURL="$PGURL" \
      walstreamer -pgdata="$PGDATA" -safekeepers="$SAFEKEEPER_ADDRS" &
    HELPER_PIDS+=($!)
  fi
  if [ "${QUAN_DISABLE_PERIODIC_SYNC:-}" = "1" ]; then
    echo "periodic quansync disabled"
    return
  fi
  (
    while true; do
      sleep 30
      if gosu postgres pg_isready -U "$POSTGRES_USER" >/dev/null 2>&1; then
        gosu postgres env \
          PGDATA="$PGDATA" PAGESERVER_HTTP="$PAGESERVER_HTTP" QUAN_STORAGE_TOKEN="$QUAN_STORAGE_TOKEN" \
          TENANT_ID="$TENANT_ID" TIMELINE_ID="$TIMELINE_ID" \
          quansync -pgdata="$PGDATA" || true
      fi
    done
  ) &
  HELPER_PIDS+=($!)
}

write_conf() {
  cat >>"$PGDATA/postgresql.conf" <<EOF
listen_addresses = '*'
wal_level = replica
archive_mode = on
archive_command = '/usr/local/bin/archive_wal.sh %p %f'
archive_timeout = 2s
restore_command = '/usr/local/bin/restore_command.sh %p %f'
max_wal_senders = 3
hot_standby = on
logging_collector = off
log_min_messages = warning
EOF
  echo "host all all all scram-sha-256" >>"$PGDATA/pg_hba.conf"
  echo "local all all trust" >>"$PGDATA/pg_hba.conf"
}

FIRST_BOOT=0
if [ ! -f "$PGDATA/PG_VERSION" ]; then
  if has_remote; then
    echo "Slim bootstrap from pageserver (GetPage-native)..."
    set +e
    gosu postgres env \
      PAGESERVER_HTTP="$PAGESERVER_HTTP" \
      SAFEKEEPER_HTTP="$SAFEKEEPER_HTTP" \
      QUAN_STORAGE_TOKEN="${QUAN_STORAGE_TOKEN:-}" \
      TENANT_ID="$TENANT_ID" TIMELINE_ID="$TIMELINE_ID" \
      quanrestore -pgdata="$PGDATA" -wal
    qr=$?
    set -e
    mkdir -p "$PGDATA/pg_wal/archive_status" \
      "$PGDATA/pg_logical/snapshots" "$PGDATA/pg_logical/mappings" \
      "$PGDATA/pg_stat_tmp" "$PGDATA/pg_snapshots" "$PGDATA/pg_notify"
    grep -q "listen_addresses" "$PGDATA/postgresql.conf" 2>/dev/null || write_conf
    grep -q "restore_command" "$PGDATA/postgresql.conf" 2>/dev/null || \
      echo "restore_command = '/usr/local/bin/restore_command.sh %p %f'" >>"$PGDATA/postgresql.conf"
    grep -q "scram-sha-256" "$PGDATA/pg_hba.conf" 2>/dev/null || echo "host all all all scram-sha-256" >>"$PGDATA/pg_hba.conf"
    grep -q "local all all trust" "$PGDATA/pg_hba.conf" 2>/dev/null || echo "local all all trust" >>"$PGDATA/pg_hba.conf"
    touch "$PGDATA/postgresql.auto.conf"
    wal_count=$(find "$PGDATA/pg_wal" -maxdepth 1 -type f -name '0*' 2>/dev/null | wc -l | tr -d ' ')
    if [ "$qr" = "0" ] && [ "${wal_count:-0}" -gt 0 ]; then
      echo "wal restore ok ($wal_count segments)"
    else
      echo "compat: pg_resetwal -f (WAL restore incomplete qr=$qr wal_files=$wal_count)"
      rm -rf "${PGDATA}/pg_wal"/*
      mkdir -p "$PGDATA/pg_wal/archive_status"
      gosu postgres pg_resetwal -f "$PGDATA"
    fi
    chown -R postgres:postgres "$PGDATA"
    fetch_request_lsn
  else
    echo "First boot: initdb and seed pageserver timeline"
    FIRST_BOOT=1
    echo "$POSTGRES_PASSWORD" >/tmp/pw
    # initdb must write local base/ — never inherit GetPage/PutPage env here.
    gosu postgres env -u QUAN_PAGESERVER_HTTP -u QUAN_REQUEST_LSN \
      initdb -D "$PGDATA" --auth-host=scram-sha-256 --auth-local=trust \
      --username="$POSTGRES_USER" --pwfile=/tmp/pw
    rm -f /tmp/pw
    write_conf
  fi
fi

if [ "$FIRST_BOOT" = "1" ]; then
  # Seed locally only — clear GetPage env (image ENV would otherwise enable remote reads).
  gosu postgres env -u QUAN_PAGESERVER_HTTP -u QUAN_REQUEST_LSN \
    TENANT_ID="$TENANT_ID" TIMELINE_ID="$TIMELINE_ID" \
    postgres -D "$PGDATA" &
  PID=$!
  for _ in $(seq 1 60); do
    gosu postgres pg_isready -U "$POSTGRES_USER" && break
    sleep 1
  done
  if ! gosu postgres psql -U "$POSTGRES_USER" -d postgres -Atc \
      "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'" | grep -q 1; then
    gosu postgres psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE ${POSTGRES_DB};"
  fi
  LSN=$(gosu postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT pg_current_wal_lsn()::text;")
  gosu postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CHECKPOINT; SELECT pg_switch_wal(); CHECKPOINT; SELECT pg_switch_wal();"
  sleep 3
  LSN_NUM=$(gosu postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint;")
  gosu postgres env PGDATA="$PGDATA" PAGESERVER_HTTP="$PAGESERVER_HTTP" QUAN_STORAGE_TOKEN="$QUAN_STORAGE_TOKEN" \
    TENANT_ID="$TENANT_ID" TIMELINE_ID="$TIMELINE_ID" \
    QUAN_PAGE_LSN="${LSN_NUM:-1}" \
    quansync -pgdata="$PGDATA"
  echo "Seed complete (lsn=$LSN num=$LSN_NUM)"
  gosu postgres pg_ctl -D "$PGDATA" -m fast stop -t 30 || kill "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  fetch_request_lsn
fi

start_helpers

# Drop stale pid from unclean shutdowns (bind-mount PGDATA).
if [ -f "$PGDATA/postmaster.pid" ]; then
  oldpid=$(head -1 "$PGDATA/postmaster.pid" || true)
  if [ -n "${oldpid:-}" ] && [ ! -d "/proc/$oldpid" ]; then
    echo "removing stale postmaster.pid (pid $oldpid not running)"
    rm -f "$PGDATA/postmaster.pid"
  fi
fi

# Ready marker for compose healthcheck / walstreamer.
touch "$PGDATA/.quan_compute_ready"
chown postgres:postgres "$PGDATA/.quan_compute_ready" 2>/dev/null || true

COMMON_ENV=(
  QUAN_STORAGE_TOKEN="$QUAN_STORAGE_TOKEN"
  TENANT_ID="$TENANT_ID"
  TIMELINE_ID="$TIMELINE_ID"
  SAFEKEEPER_ADDR="$SAFEKEEPER_ADDR"
  SAFEKEEPER_ADDRS="$SAFEKEEPER_ADDRS"
  SAFEKEEPER_HTTP="$SAFEKEEPER_HTTP"
  PAGESERVER_HTTP="$PAGESERVER_HTTP"
  QUORUM="${QUORUM:-2}"
)

# Primary compute always reads latest pages. A pinned QUAN_REQUEST_LSN hides
# quansync/WAL pages whose LSN is above the pin and breaks RelSize/GetPage.
if [ -n "${QUAN_PAGESERVER_HTTP:-}" ]; then
  echo "Starting patched Postgres (GetPage enabled → $QUAN_PAGESERVER_HTTP)..."
  gosu postgres env -u QUAN_REQUEST_LSN QUAN_PAGESERVER_HTTP="$QUAN_PAGESERVER_HTTP" \
    "${COMMON_ENV[@]}" postgres -D "$PGDATA" &
else
  echo "Starting Postgres (local md; set QUAN_PAGESERVER_HTTP to enable GetPage)..."
  gosu postgres env -u QUAN_PAGESERVER_HTTP -u QUAN_REQUEST_LSN \
    "${COMMON_ENV[@]}" postgres -D "$PGDATA" &
fi
PG_PID=$!
wait "$PG_PID"
cleanup
