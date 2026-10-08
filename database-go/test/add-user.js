/**
 * Create public.users (if needed) and INSERT rows — smoke test for WAL / GetPage stack.
 *
 *   cd serverless/database-go/test
 *   npm install
 *   cp .env.example .env   # set PGPASSWORD
 *   npm run insert-user
 */
import { config as loadEnv } from "dotenv";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";

loadEnv({ path: join(dirname(fileURLToPath(import.meta.url)), ".env") });

const {
  PGHOST = "127.0.0.1",
  PGPORT = "5432",
  PGUSER = "postgres",
  PGPASSWORD = "postgres",
  PGDATABASE = "app",
  INSERT_COUNT = "1000",
} = process.env;

const count = Math.max(1, Number(INSERT_COUNT) || 1000);

const client = new pg.Client({
  host: PGHOST,
  port: Number(PGPORT),
  user: PGUSER,
  password: PGPASSWORD,
  database: PGDATABASE,
  ssl: false,
});

const ensureTableSql = `
CREATE TABLE IF NOT EXISTS public.users (
  id          bigserial PRIMARY KEY,
  email       text NOT NULL UNIQUE,
  name        text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);
`;

const insertSql = `
INSERT INTO public.users (email, name)
VALUES ($1, $2)
RETURNING id, email, name, created_at;
`;

async function main() {
  await client.connect();
  console.log(`connected ${PGUSER}@${PGHOST}:${PGPORT}/${PGDATABASE}`);

  await client.query(ensureTableSql);
  console.log("ensured table public.users");

  const before = await client.query("SELECT pg_current_wal_lsn()::text AS lsn");
  console.log("wal before:", before.rows[0].lsn);
  console.log(`inserting ${count} users…`);

  const batch = String(Date.now() % 1_000_000).padStart(6, "0");
  let last;
  for (let i = 0; i < count; i++) {
    const email = `user-${batch}-${String(i).padStart(6, "0")}@example.test`;
    const name = `User ${batch}/${i}`;
    const res = await client.query(insertSql, [email, name]);
    last = res.rows[0];
    if ((i + 1) % 100 === 0 || i === 0) {
      console.log(`  ${i + 1}/${count}`, last);
    }
  }

  console.log("last inserted:", last);

  const tally = await client.query("SELECT count(*)::bigint AS n FROM public.users");
  console.log("users count:", tally.rows[0].n);

  await client.query("CHECKPOINT");
  await client.query("SELECT pg_switch_wal()");
  const after = await client.query("SELECT pg_current_wal_lsn()::text AS lsn");
  console.log("wal after: ", after.rows[0].lsn);

  await client.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
