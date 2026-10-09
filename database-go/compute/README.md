# Patched Postgres compute (`database-go`)

Postgres 16 built from source with an smgr patch that calls pageserver HTTPS `GetPage` / `RelSize` / `PutPage` when remote mode is enabled.

## Stack wiring

```
compute (patched PG)
  ├─ mdread → HTTPS GET pageserver /v1/page (URL list failover)
  ├─ mdwrite/mdextend → HTTPS PUT pageserver /v1/page (primary; sync peer fanout)
  ├─ archive_command → qwal → safekeeper TCP quorum (QWAL over TLS + QWAU auth)
  ├─ restore_command → HTTPS GET safekeeper /wal
  └─ walstreamer → safekeeper TCP quorum
```

## Auth + TLS

Set `QUAN_STORAGE_TOKEN` (required). Helpers and the smgr patch send `Authorization: Bearer` / `X-Quan-Token`. Set `QUAN_TLS_CA` (or `TLS_CA_FILE`) to the CA PEM when talking HTTPS / TLS QWAL.

## Failover

- `QUAN_PAGESERVER_HTTP_URLS` — comma-separated bases; GetPage/PutPage/RelSize try in order with sticky last-good.
- Falls back to single `QUAN_PAGESERVER_HTTP` / `PAGESERVER_HTTP`.
- **Reads** can use the replica. **Writes** still need the primary (replica rejects external PUT).

## Build

From `database-go`:

```bash
docker build -f compute/Dockerfile -t quan-compute .
```

Or via compose (see `../docker-compose.yml` — needs `.env` from `.env.example`).

## Helpers in the image

| Binary | Role |
|--------|------|
| `walstreamer` | Tail `pg_wal`, push QWAL to all safekeepers (quorum) |
| `qwal` | One-shot QWAL push to quorum (used by `archive_wal.sh`) |
| `quansync` | Upload relation pages + basebackup tar to pageserver |
| `quanrestore` | Slim PGDATA from basebackup + page stubs (+ optional WAL) |

## Env

| Var | Default |
|-----|---------|
| `PAGESERVER_HTTP` / `QUAN_PAGESERVER_HTTP` | `https://pageserver:8081` |
| `QUAN_PAGESERVER_HTTP_URLS` | primary,replica HTTPS list |
| `QUAN_TLS_CA` | CA PEM for HTTPS / QWAL TLS |
| `SAFEKEEPER_ADDRS` | comma-separated TCP ingest addresses |
| `SAFEKEEPER_HTTP` | restore / health |
| `QUAN_STORAGE_TOKEN` | required |
| `POSTGRES_PASSWORD` | required (no default) |
| `TENANT_ID` / `TIMELINE_ID` | `default` / `main` |
| `QUORUM` | majority of `SAFEKEEPER_ADDRS` |

## Notes

- First boot runs `initdb` with GetPage off so local `base/` is seeded, then uploads to pageserver. With `QUAN_ENABLE_GETPAGE=1`, relation I/O is remote.
- Do **not** point this at Postgres.app — run the containerized patched binary.
- PutPage is the durable page source of truth; pageserver WAL apply is FPI-only catch-up.
