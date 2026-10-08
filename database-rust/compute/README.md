# Compute (Postgres 16)

Vanilla Postgres with `archive_command` shipping WAL segments to the safekeeper HTTP `/wal` endpoint.

## Limits (v1)

- Durable source of truth is the Go safekeeper + S3, but Postgres still uses a local ephemeral `PGDATA` for buffers and catalog.
- **GetPage is not wired into the Postgres buffer manager.** True Neon parity needs a patched Postgres / Neon compute that issues `GetPage` on buffer miss.
- Use the pageserver gRPC client / smoke script to validate page materialization from synthetic FPI WAL.

## Env

| Variable | Default | Meaning |
|----------|---------|---------|
| `SAFEKEEPER_HTTP` | `http://safekeeper-1:8080` | Archive gateway |
| `TENANT_ID` | `default` | Tenant |
| `TIMELINE_ID` | `main` | Timeline |
