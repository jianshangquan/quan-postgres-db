# Chaos / soak

Requires a healthy compose stack (`docker compose --env-file .env up -d --build`).

```bash
cd database-go
./test/chaos/run.sh
```

What it checks:

| Check | Behavior |
|-------|----------|
| Sync PutPage | Primary ACK only after peer fanout succeeds |
| Crash recovery | `SIGKILL` primary, restart, GetPage still returns page |
| Replica read | GetPage on pageserver-2 after fanout |
| Safekeeper quorum | Stop sk-3, `qwal` still commits with quorum 2 |
| Soak | Timed Put/Get loop with periodic sk-2 kill |

## Failover limits

- **Reads** (`GetPage` / `RelSize`): compute retries `QUAN_PAGESERVER_HTTP_URLS` (primary then replica).
- **Writes** (`PutPage`): replica still rejects external PUT; write availability needs the primary (or an operator flipping `PAGESERVER_ROLE`). There is no automatic pageserver promotion.
