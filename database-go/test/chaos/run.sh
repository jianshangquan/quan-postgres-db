#!/usr/bin/env bash
# Chaos / crash-recovery checks against a running docker compose stack.
# Expects: cd database-go && docker compose --env-file .env up -d --build
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

TOKEN="${QUAN_STORAGE_TOKEN:-}"
if [ -z "$TOKEN" ] && [ -f .env ]; then
  # shellcheck disable=SC1091
  set -a; source .env; set +a
  TOKEN="${QUAN_STORAGE_TOKEN:-}"
fi
if [ -z "$TOKEN" ]; then
  echo "QUAN_STORAGE_TOKEN required" >&2
  exit 1
fi
CA="${QUAN_TLS_CA:-$ROOT/certs/ca.crt}"
CURL=(curl -fsS --cacert "$CA" -H "Authorization: Bearer $TOKEN" -H "X-Quan-Token: $TOKEN")
PRIMARY="${PAGESERVER_PRIMARY:-https://127.0.0.1:8081}"
REPLICA="${PAGESERVER_REPLICA:-https://127.0.0.1:8082}"
TENANT="${TENANT_ID:-default}"
TIMELINE="${TIMELINE_ID:-main}"
COMPOSE=(docker compose --env-file .env)

pass=0
fail=0
ok() { echo "PASS: $*"; pass=$((pass + 1)); }
bad() { echo "FAIL: $*" >&2; fail=$((fail + 1)); }

put_page() {
  local url="$1" byte="$2" tmp
  tmp=$(mktemp)
  python3 -c "import sys; sys.stdout.buffer.write(bytes([$byte])*8192)" >"$tmp"
  if "${CURL[@]}" -X PUT --data-binary @"$tmp" -H "Content-Type: application/octet-stream" "$url" >/dev/null; then
    rm -f "$tmp"
    return 0
  fi
  rm -f "$tmp"
  return 1
}

# --- PutPage ACK + crash recovery ---
URL="$PRIMARY/v1/page?tenant=$TENANT&timeline=$TIMELINE&db=1&rel=999&fork=0&block=0&lsn=42"
if put_page "$URL" 0x42; then
  ok "sync PutPage to primary (peer fanout)"
else
  bad "PutPage to primary"
fi

"${COMPOSE[@]}" kill -s SIGKILL pageserver || true
sleep 1
"${COMPOSE[@]}" start pageserver
for _ in $(seq 1 60); do
  if "${CURL[@]}" "$PRIMARY/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
GOT=$("${CURL[@]}" "$URL" | wc -c | tr -d ' ')
if [ "$GOT" = "8192" ]; then
  ok "GetPage after primary crash-restart"
else
  bad "GetPage after crash got $GOT bytes"
fi

# --- Read failover to replica ---
if "${CURL[@]}" "$REPLICA/v1/page?tenant=$TENANT&timeline=$TIMELINE&db=1&rel=999&fork=0&block=0&lsn=42" | wc -c | grep -q 8192; then
  ok "GetPage on replica (read failover target)"
else
  bad "GetPage on replica (fanout may have failed)"
fi

# --- Safekeeper kill (quorum still works) ---
"${COMPOSE[@]}" stop safekeeper-3
TMP=$(mktemp)
python3 -c 'import sys; sys.stdout.buffer.write(b"chaos"*1024)' >"$TMP"
if QUAN_STORAGE_TOKEN="$TOKEN" QUAN_TLS_CA="$CA" \
  go run ./compute/cmd/qwal -file="$TMP" -start-lsn=$((32*1024*1024)) \
    -safekeepers=127.0.0.1:5454,127.0.0.1:5455,127.0.0.1:5456 \
    -tenant="$TENANT" -timeline="$TIMELINE" -quorum=2; then
  ok "qwal quorum with safekeeper-3 down"
else
  bad "qwal quorum with safekeeper-3 down"
fi
rm -f "$TMP"
"${COMPOSE[@]}" start safekeeper-3

# --- Short soak ---
SOAK_SECS="${SOAK_SECS:-30}"
end=$((SECONDS + SOAK_SECS))
i=0
while [ "$SECONDS" -lt "$end" ]; do
  i=$((i + 1))
  LSN=$((1000 + i))
  BYTE=$((i % 256))
  URL="$PRIMARY/v1/page?tenant=$TENANT&timeline=$TIMELINE&db=1&rel=998&fork=0&block=0&lsn=$LSN"
  if ! put_page "$URL" "$BYTE"; then
    bad "soak PutPage i=$i"
    break
  fi
  GOT=$("${CURL[@]}" "$URL" | wc -c | tr -d ' ')
  if [ "$GOT" != "8192" ]; then
    bad "soak GetPage i=$i bytes=$GOT"
    break
  fi
  if [ $((i % 10)) -eq 0 ]; then
    "${COMPOSE[@]}" kill -s SIGKILL safekeeper-2 >/dev/null 2>&1 || true
    sleep 1
    "${COMPOSE[@]}" start safekeeper-2 >/dev/null 2>&1 || true
  fi
done
if [ "$fail" -eq 0 ]; then
  ok "soak ${SOAK_SECS}s ($i puts)"
fi

echo "chaos summary: pass=$pass fail=$fail"
echo
echo "Note: PutPage writes require primary (replica rejects external PUT)."
echo "GetPage/RelSize can fail over via QUAN_PAGESERVER_HTTP_URLS."
[ "$fail" -eq 0 ]
