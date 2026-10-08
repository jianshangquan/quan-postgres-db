# Patched Postgres compute (`database-go-2`)

Postgres 16 built from source with an smgr patch that calls pageserver HTTP `GetPage` / `RelSize` when `QUAN_PAGESERVER_HTTP` is set.

## Stack wiring

```
compute (patched PG)
  ├─ mdread → HTTP GET pageserver /v1/page
  ├─ mdwrite/mdextend → HTTP PUT pageserver /v1/page (no local base/ write)
  ├─ archive_command → qwal → safekeeper TCP :5454 (QWAL)
  ├─ restore_command → HTTP GET safekeeper :8080 /wal
  └─ walstreamer → safekeeper TCP :5454
```

## Build

From `serverless/database-go-2`:

```bash
docker build -f compute/Dockerfile -t quan-compute .
```

Or via compose (see `../docker-compose.yml`).

## Helpers in the image

| Binary | Role |
|--------|------|
| `walstreamer` | Tail `pg_wal`, push QWAL to safekeeper |
| `qwal` | One-shot QWAL push (used by `archive_wal.sh`) |
| `quansync` | Upload relation pages + basebackup tar to pageserver |
| `quanrestore` | Slim PGDATA from basebackup + page stubs (+ optional WAL) |

## Env

| Var | Default |
|-----|---------|
| `PAGESERVER_HTTP` / `QUAN_PAGESERVER_HTTP` | `http://pageserver:8081` |
| `SAFEKEEPER_ADDR` | `safekeeper:5454` (TCP ingest) |
| `SAFEKEEPER_HTTP` | `http://safekeeper:8080` (restore / health) |
| `TENANT_ID` / `TIMELINE_ID` | `default` / `main` |

## Notes

- First boot runs `initdb` with GetPage off so local `base/` is seeded, then uploads to pageserver. With `QUAN_ENABLE_GETPAGE=1`, relation I/O is remote: `mdread`→GetPage, `mdwrite`/`mdextend`/`mdzeroextend`→PutPage (no local page FileWrite). `base/` may still get empty stubs + `pg_internal.init`; heap/index page bodies do not grow locally.
- Do **not** point this at Postgres.app — run the containerized patched binary.
