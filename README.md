# quan-postgres-db

Neon-style split Postgres storage: patched compute talks to a pageserver for pages and to safekeepers for WAL.

This repo has two implementations of the same idea:

| Path | Stack | Notes |
|------|--------|--------|
| [`database-go/`](database-go/) | Go + HTTPS/QWAL TLS | 3-safekeeper quorum, sync PutPage fanout, image layers, chaos scripts |
| [`database-rust/`](database-rust/) | Rust + gRPC/Raft + S3 | Raft safekeepers, pageserver, local object store |

Use **database-go** for the current hardened compose lab. Use **database-rust** for the Raft/S3 experiment (different host ports so both can run).

## Architecture (shared)

```text
SQL clients → Postgres compute (patched smgr)
                │ WAL archive / stream → safekeepers (quorum)
                │ GetPage / PutPage     → pageserver
                ▼
         durable page store + WAL retention
```

## Quick start (Go)

```bash
cd database-go
cp .env.example .env
# set QUAN_STORAGE_TOKEN and POSTGRES_PASSWORD
./certs/gen-certs.sh   # if certs are missing
docker compose --env-file .env up --build
```

Postgres: `localhost:5432`. Details, env, TLS, and chaos: [`database-go/README.md`](database-go/README.md).

## Quick start (Rust)

```bash
cd database-rust
docker compose up --build
./scripts/smoke.sh
```

Postgres: `localhost:25432`. Details: [`database-rust/README.md`](database-rust/README.md).

## Docs

- [database-go README](database-go/README.md) — quorum, TLS, PutPage model, failover limits
- [database-go compute](database-go/compute/README.md) — patched Postgres image
- [database-go chaos](database-go/test/chaos/README.md) — crash / soak
- [database-rust README](database-rust/README.md) — Raft + S3 layout
