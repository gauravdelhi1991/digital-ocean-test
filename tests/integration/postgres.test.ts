import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { PostgresJobStore } from "../../src/jobs";
import { runMigrations } from "../../src/migration-runner";

const databaseUrl = process.env.TEST_DATABASE_URL;

describe.skipIf(!databaseUrl)("PostgreSQL job persistence", () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const store = new PostgresJobStore(pool);

  beforeAll(async () => {
    await runMigrations(pool);
  });

  afterAll(async () => {
    await pool.end();
  });

  it("durably stores the job payload and submission options", async () => {
    const job = await store.createJob({
      payload: { operation: "integration-test", marker: randomUUID() },
      priority: 4,
      maxRetries: 2,
      timeoutMs: 5000,
    });

    const result = await pool.query(
      `SELECT payload, priority, max_retries, timeout_ms, attempt_count, status
       FROM jobs WHERE id = $1`,
      [job.id],
    );

    expect(result.rows[0]).toMatchObject({
      payload: job.payload,
      priority: 4,
      max_retries: 2,
      timeout_ms: 5000,
      attempt_count: 0,
      status: "accepted",
    });
    expect(job.id).toBeTruthy();
  });
});
