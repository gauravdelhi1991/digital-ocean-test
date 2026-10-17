import { Pool } from "pg";
import { runMigrations } from "./migration-runner";

async function main(): Promise<void> {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error("DATABASE_URL must be configured");
  }

  const pool = new Pool({ connectionString });
  try {
    await runMigrations(pool);
  } finally {
    await pool.end();
  }
}

main().catch((error: unknown) => {
  console.error("Database migration failed", error);
  process.exitCode = 1;
});
