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
        cancellationRequestedAt: null,
        scheduledAt: null,
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

  it("cancels pending jobs and removes unpublished outbox work", async () => {
    const marker = randomUUID();
    const job = await store.createJob({
      payload: { marker },
      priority: 0,
      maxRetries: 0,
      timeoutMs: 5000,
    });

    try {
      const result = await store.requestJobCancellation(job.id);
      expect(result.outcome).toBe("cancelled");
      if (result.outcome !== "cancelled") {
        throw new Error(`Unexpected cancellation outcome: ${result.outcome}`);
      }
      expect(result.job.status).toBe("cancelled");
      expect(result.job.completedAt).not.toBeNull();
      expect(
        await pool.query("SELECT 1 FROM job_outbox WHERE job_id = $1", [job.id]),
      ).toMatchObject({ rowCount: 0 });
      await expect(store.getJob(job.id)).resolves.toMatchObject({
        status: "cancelled",
        cancellationRequestedAt: null,
      });
    } finally {
      await pool.query("DELETE FROM jobs WHERE id = $1", [job.id]);
    }
  });

  it("records cooperative cancellation requests for running jobs", async () => {
    const marker = randomUUID();
    const job = await store.createJob({
      payload: { marker },
      priority: 0,
      maxRetries: 0,
      timeoutMs: 5000,
    });

    try {
      await expect(store.startJobAttempt(job.id)).resolves.toMatchObject({
        status: "running",
        cancellationRequestedAt: null,
      });
      const result = await store.requestJobCancellation(job.id);
      expect(result.outcome).toBe("requested");
      expect(await store.isCancellationRequested(job.id)).toBe(true);
      await expect(store.markJobSucceeded(job.id, { ignored: true })).resolves.toBe(
        "cancelled",
      );
      await expect(store.getJob(job.id)).resolves.toMatchObject({
        status: "cancelled",
        result: null,
      });
    } finally {
      await pool.query("DELETE FROM jobs WHERE id = $1", [job.id]);
    }
  });

  it("cancels instead of retrying when cancellation wins a failure transition", async () => {
    const job = await store.createJob({
      payload: { marker: randomUUID() },
      priority: 0,
      maxRetries: 1,
      timeoutMs: 5000,
    });

    try {
      await store.startJobAttempt(job.id);
      await expect(store.requestJobCancellation(job.id)).resolves.toMatchObject({
        outcome: "requested",
      });

      await expect(
        store.markJobRetryable(job.id, "temporary failure"),
      ).resolves.toBe("cancelled");
      await expect(store.getJob(job.id)).resolves.toMatchObject({
        status: "cancelled",
        lastError: null,
      });
    } finally {
      await pool.query("DELETE FROM jobs WHERE id = $1", [job.id]);
    }
  });

  it("cancels instead of dead-lettering when cancellation wins the failure transition", async () => {
    const job = await store.createJob({
      payload: { marker: randomUUID() },
      priority: 0,
      maxRetries: 0,
      timeoutMs: 5000,
    });

    try {
      await store.startJobAttempt(job.id);
      await expect(store.requestJobCancellation(job.id)).resolves.toMatchObject({
        outcome: "requested",
      });

      await expect(
        store.markJobDeadLettered(job.id, "permanent failure"),
      ).resolves.toBe("cancelled");
      await expect(store.getJob(job.id)).resolves.toMatchObject({
        status: "cancelled",
        lastError: null,
      });
      await expect(
        pool.query("SELECT 1 FROM job_dead_letters WHERE job_id = $1", [job.id]),
      ).resolves.toMatchObject({ rowCount: 0 });
    } finally {
      await pool.query("DELETE FROM jobs WHERE id = $1", [job.id]);
    }
  });
});
