# Neon-style Postgres storage (`serverless/database-rust`)

Rust port of [`../database`](../database): Postgres compute + **Raft safekeeper cluster** + **pageserver** + S3-compatible object store.

## Architecture

```text
SQL clients → Postgres compute
                │ AppendWAL (archive / gRPC)
                ▼
         Safekeeper Raft (3 nodes, Rust)
                │ quorum commit → S3 WAL keys
                │ fan-out IngestWAL
                ▼
            Pageserver ──► S3 page layers
                ▲
                └── GetPage (gRPC)
```

| Role | Binary | Notes |
|------|--------|--------|
| Compute | Postgres 16 | Same as Go stack; `archive_command` → HTTP `/wal` |
| Safekeeper | `safekeeper` | In-process Raft (vote + append-entries over gRPC) |
| Pageserver | `pageserver` | FPI-first WAL apply; hot LRU + S3 |
| Object store | `objstore` | Path-style S3 for local compose; client is plain HTTP/1.1 (MinIO/AWS path-style compatible) |

## Quick start

```bash
cd serverless/database-rust
docker compose up --build
./scripts/smoke.sh
```

Host ports (offset from the Go stack to avoid clashes):

| Service | Host |
|---------|------|
| Postgres | `25432` |
| Safekeeper gRPC | `25051` / `25052` / `25053` |
| Safekeeper HTTP | `28080` |
| Pageserver gRPC / HTTP | `25061` / `28081` |
| Object store | `29000` |

Unit tests:

```bash
cargo test
```

## Env

Same as the Go service: `NODE_ID`, `LISTEN_ADDR`, `HTTP_ADDR`, `SAFEKEEPER_PEERS` (`id@host:raftPort:grpcPort`), `PAGESERVER_ADDR`, `S3_*`, `DATA_DIR`.

Raft peer RPCs use the **gRPC** address (same process as `Storage`); `raftPort` in peers is reserved for parity with the Go layout.

## Limitations (v1)

- FPI-first WAL apply only
- GetPage not inside Postgres buffer manager
- Simplified Raft (no persistent raft log across restart beyond applied S3 state)
- Not production-hardened
