import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { buildApp } from "../../src/app";
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
    const marker = randomUUID();
    const app = buildApp(store, { logger: false });

    try {
      const address = await app.listen({ host: "127.0.0.1", port: 0 });
      const response = await fetch(new URL("/jobs", address), {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          payload: { operation: "integration-test", marker },
          priority: 4,
          maxRetries: 2,
          timeoutMs: 5000,
        }),
      });

      expect(response.status).toBe(201);
      const job = await response.json();
      expect(job).toMatchObject({
        payload: { operation: "integration-test", marker },
        priority: 4,
        maxRetries: 2,
        timeoutMs: 5000,
        attemptCount: 0,
        status: "accepted",
        lastError: null,
        result: null,
        startedAt: null,
        completedAt: null,
      });
      expect(job).toHaveProperty("createdAt");
      expect(job).toHaveProperty("updatedAt");

      const statusResponse = await fetch(new URL(`/jobs/${job.id}`, address));
      expect(statusResponse.status).toBe(200);
      expect(await statusResponse.json()).toEqual(job);

      const result = await pool.query(
        `SELECT jobs.payload, jobs.priority, jobs.max_retries, jobs.timeout_ms,
                jobs.attempt_count, jobs.status, outbox.job_id AS outbox_job_id
         FROM jobs
         JOIN job_outbox AS outbox ON outbox.job_id = jobs.id
         WHERE jobs.id = $1`,
        [job.id],
      );

      expect(result.rows[0]).toMatchObject({
        payload: { operation: "integration-test", marker },
        priority: 4,
        max_retries: 2,
        timeout_ms: 5000,
        attempt_count: 0,
        status: "accepted",
        outbox_job_id: job.id,
      });
    } finally {
      await app.close();
      await pool.query("DELETE FROM jobs WHERE payload->>'marker' = $1", [marker]);
    }
  });
});
