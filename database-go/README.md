# database-go-2

Minimal Neon-style split: compute → safekeeper → pageserver.

```
compute (patched PG, optional)
    │ WAL: walstreamer / archive → TCP QWAL :5454
    │ GetPage ←──────────────────────────── HTTP :8081
    ▼
safekeeper :5454 (QWAL) + :8080 (restore HTTP)
    │
    ▼  HTTP POST /v1/ingest
pageserver :8081          page layers + GetPage + basebackup
```

Stock Postgres.app can still feed the stack via `walstreamer/` alone (read-only copy-out).  
For GetPage-native compute, use `compute/` (Docker).

## Layout

| Dir | Role |
|-----|------|
| `walstreamer/` | Tail `pg_wal`, push QWAL frames to safekeeper |
| `safekeeper/` | TCP WAL ingest + HTTP restore, fanout to pageserver |
| `pageserver/` | Ingest WAL → FPI pages; GetPage / PutPage / basebackup |
| `compute/` | Patched Postgres 16 smgr + seed/restore helpers |

## Run (3 terminals)

Replace `PGDATA` with your cluster (example is Postgres.app 17):

```bash
export PGDATA="/Users/jianshangquan/Library/Application Support/Postgres/var-17"
```

**1. Pageserver**

```bash
cd serverless/database-go-2/pageserver
LISTEN_ADDR=:8081 DATA_DIR=./data go run ./src/
```

**2. Safekeeper**

```bash
cd serverless/database-go-2/safekeeper
LISTEN_ADDR=:5454 DATA_DIR=./data PAGESERVER_URL=http://127.0.0.1:8081 go run ./src/
```

**3. Walstreamer**

```bash
cd serverless/database-go-2/walstreamer
go run ./src/ \
  -pgdata="$PGDATA" \
  -safekeeper=127.0.0.1:5454
```

## Smoke

```bash
psql -h 127.0.0.1 -U postgres -c "CREATE TABLE IF NOT EXISTS t(i int); INSERT INTO t VALUES (1);"
psql -h 127.0.0.1 -U postgres -c "SELECT pg_switch_wal();"
```

Expect:

- walstreamer: `streamed 0000000… +N bytes`
- safekeeper: `wal append …`
- pageserver: ingest OK (check `GET http://127.0.0.1:8081/v1/timeline?tenant=default&timeline=main`)

## Env

| Component | Var | Default |
|-----------|-----|---------|
| safekeeper | `LISTEN_ADDR` | `:5454` |
| safekeeper | `DATA_DIR` | `./data` |
| safekeeper | `PAGESERVER_URL` | `http://127.0.0.1:8081` |
| pageserver | `LISTEN_ADDR` | `:8081` |
| pageserver | `DATA_DIR` | `./data` |
| walstreamer | `-pgdata` / `PGDATA` | required |
| walstreamer | `-safekeeper` | `127.0.0.1:5454` |
| walstreamer | `-pgurl` | `postgres://postgres@127.0.0.1:5432/postgres?sslmode=disable` |

## Notes

- Stock Postgres does **not** dial safekeeper; **walstreamer** is required.
- WAL segment files are always 16 MiB; walstreamer tracks flush LSN so inserts into the current/recycled segment still stream.
- Pageserver HTTP is intentional (compute GetPage + safekeeper ingest). Safekeeper↔compute is TCP (`QWAL`), not HTTP.
