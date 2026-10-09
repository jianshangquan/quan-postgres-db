# database-go

Neon-style split with a **3-safekeeper quorum**, TLS, and a catching-up pageserver replica.

```
compute (patched PG)
    │ WAL: walstreamer / archive → TCP QWAL over TLS (quorum 2 of 3)
    │ GetPage/PutPage ←────────────── HTTPS (primary; URLs list for read failover)
    ▼
safekeeper-1..3  TCP :5454 (QWAL+TLS) + HTTPS :8080 (restore / peer pull)
    │
    ▼  pageserver pulls /wal (FPI catch-up only)
pageserver (primary) + pageserver-2 (replica)
    PutPage/basebackup: sync fanout to peer before ACK
```

## Consistency model

- **PutPage / basebackup are authoritative** for page state. The primary sync-replicates to configured peers (`PAGESERVER_PEER_URLS`) and only then ACKs the client.
- WAL apply on the pageserver remains **FPI-only** (full-page images) for catch-up/bootstrap — non-FPI records are not redone.
- **Reads** may fail over across `QUAN_PAGESERVER_HTTP_URLS`. **Writes** still require the primary (replica rejects external PUT); there is no automatic promotion.

## Auth + TLS

All components require `QUAN_STORAGE_TOKEN`:

- HTTPS: `Authorization: Bearer …` or `X-Quan-Token` (except `/healthz`)
- QWAL TCP: TLS wrap + `QWAU` handshake before appends

Dev certs live under [`certs/`](certs/) (`./certs/gen-certs.sh`). Compose mounts them and sets `TLS_CERT_FILE` / `TLS_KEY_FILE` / `QUAN_TLS_CA`.

Copy [`.env.example`](.env.example) to `.env` and set token + `POSTGRES_PASSWORD`.

## Run (compose)

```bash
cp .env.example .env
# edit QUAN_STORAGE_TOKEN and POSTGRES_PASSWORD
./certs/gen-certs.sh   # if certs missing
docker compose --env-file .env up --build
```

| Service | Ports |
|---------|-------|
| pageserver (primary) | 8081 |
| pageserver-2 (replica) | 8082 |
| safekeeper-1 | 5454 / 8080 |
| safekeeper-2 | 5455 / 8083 |
| safekeeper-3 | 5456 / 8084 |
| compute | 5432 |

## Layout

| Dir | Role |
|-----|------|
| `walstreamer/` | Tail `pg_wal`, push QWAL to all safekeepers (quorum) |
| `safekeeper/` | TCP WAL ingest (fsync before ack) + HTTPS restore + peer heal |
| `pageserver/` | Pull WAL → FPI; GetPage / PutPage / image layers + GC |
| `compute/` | Patched Postgres 16 smgr + seed/restore helpers |
| `test/chaos/` | Crash / quorum / soak scripts |

## Local (plaintext, single safekeeper)

Omit TLS env vars for unit tests / quick smoke:

```bash
export QUAN_STORAGE_TOKEN=dev-token
export PGDATA="/path/to/pgdata"

cd pageserver && LISTEN_ADDR=:8081 DATA_DIR=./data \
  SAFEKEEPER_HTTP_URLS=http://127.0.0.1:8080 go run ./src/

cd safekeeper && LISTEN_ADDR=:5454 DATA_DIR=./data go run ./src/

cd walstreamer && go run ./src/ \
  -pgdata="$PGDATA" -safekeeper=127.0.0.1:5454
```

## Env

| Component | Var | Notes |
|-----------|-----|-------|
| all | `QUAN_STORAGE_TOKEN` | required |
| all | `TLS_CERT_FILE` / `TLS_KEY_FILE` | enable HTTPS / QWAL TLS |
| clients | `QUAN_TLS_CA` / `TLS_CA_FILE` | trust store |
| safekeeper | `SAFEKEEPER_PEERS` | peer HTTPS bases for segment heal |
| pageserver | `SAFEKEEPER_HTTP_URLS` | pull WAL list/fetch |
| pageserver | `PAGESERVER_ROLE` | `primary` or `replica` |
| pageserver | `PAGESERVER_PEER_URLS` | sync fanout targets (comma-separated) |
| pageserver | `CACHE_PAGES` | default 8192 |
| compute | `QUAN_PAGESERVER_HTTP_URLS` | read failover list |
| walstreamer / qwal | `SAFEKEEPER_ADDRS` | comma-separated host:ports |
| walstreamer / qwal | `QUORUM` | default majority |

## Storage

New `PutPage` values append into **image layer** files under `imagelayers/` (~16 MiB), with a JSON `.idx` sidecar. Legacy one-file-per-version paths are still readable; `CompactLoose` packs them into layers. GC keeps the newest LSN per block.

## Chaos

```bash
./test/chaos/run.sh
```

See [`test/chaos/README.md`](test/chaos/README.md).

## Notes

- Tenant/timeline IDs must match `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`.
- Replica rejects external `PUT` unless `X-Quan-Peer: 1` is set with a valid token.
