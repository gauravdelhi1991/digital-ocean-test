import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import type { Pool } from "pg";

export async function runMigrations(pool: Pool): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock($1)", [1_104_202_610]);
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    for (const migration of [
      "001_create_jobs",
      "002_create_job_outbox",
      "003_create_job_dead_letters",
      "004_add_job_cancellation",
      "005_add_job_scheduling",
    ]) {
      const applied = await client.query(
        "SELECT 1 FROM schema_migrations WHERE version = $1",
        [migration],
      );

      if (applied.rowCount === 0) {
        const sql = await readFile(
          resolve(__dirname, `../migrations/${migration}.sql`),
          "utf8",
        );
        await client.query(sql);
        await client.query("INSERT INTO schema_migrations (version) VALUES ($1)", [
          migration,
        ]);
      }
    }

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
