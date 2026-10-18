import { randomUUID } from "node:crypto";
import { Queue } from "bullmq";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { buildApp } from "../../src/app";
import {
  createJobQueuePublisher,
  parseRedisConnectionOptions,
} from "../../src/job-queue";
import { PostgresJobStore, type OutboxStore } from "../../src/jobs";
import { OutboxPublisher } from "../../src/outbox-publisher";
import { runMigrations } from "../../src/migration-runner";
import type { JobQueuePublisher } from "../../src/outbox-publisher";

const databaseUrl = process.env.TEST_DATABASE_URL;
const redisUrl = process.env.TEST_REDIS_URL;

describe.skipIf(!databaseUrl || !redisUrl)("transactional outbox integration", () => {
  const queueName = `outbox-test-${randomUUID()}`;
  let pool: Pool;
  let store: PostgresJobStore;
  let queue: Queue;
  let queuePublisher: JobQueuePublisher;

  function storeForJob(jobId: string): OutboxStore {
    return {
      claimNextOutboxMessage: () => store.claimNextOutboxMessage(jobId),
      markOutboxMessagePublished: (id) => store.markOutboxMessagePublished(id),
      scheduleOutboxRetry: (id, delayMs, error) =>
        store.scheduleOutboxRetry(id, delayMs, error),
    };
  }

  beforeAll(async () => {
    if (!databaseUrl || !redisUrl) {
      throw new Error("TEST_DATABASE_URL and TEST_REDIS_URL are required");
    }
    pool = new Pool({ connectionString: databaseUrl });
    store = new PostgresJobStore(pool);
    queue = new Queue(queueName, {
      connection: parseRedisConnectionOptions(redisUrl),
    });
    queuePublisher = createJobQueuePublisher(redisUrl, queueName);
    await runMigrations(pool);
  });

  afterAll(async () => {
    await queue.obliterate({ force: true });
    await queue.close();
    await queuePublisher.close();
    await pool.end();
  });

  it("atomically stores a job and publishes its outbox message", async () => {
    const job = await store.createJob({
      payload: { marker: randomUUID() },
      priority: 0,
      maxRetries: 3,
      timeoutMs: 30_000,
    });

    try {
      const initial = await pool.query(
        `SELECT jobs.status, outbox.published_at
         FROM jobs
         JOIN job_outbox AS outbox ON outbox.job_id = jobs.id
         WHERE jobs.id = $1`,
        [job.id],
      );
      expect(initial.rows[0]).toEqual({ status: "accepted", published_at: null });

      const publisher = new OutboxPublisher(
        storeForJob(job.id),
        queuePublisher,
        console,
      );
      await expect(publisher.publishNext()).resolves.toBe(true);

      const queuedJob = await queue.getJob(job.id);
      expect(queuedJob?.data).toEqual({ jobId: job.id });
      const published = await pool.query(
        `SELECT jobs.status, outbox.published_at
         FROM jobs
         JOIN job_outbox AS outbox ON outbox.job_id = jobs.id
         WHERE jobs.id = $1`,
        [job.id],
      );
      expect(published.rows[0]?.status).toBe("queued");
      expect(published.rows[0]?.published_at).toBeInstanceOf(Date);
    } finally {
      await (await queue.getJob(job.id))?.remove();
      await pool.query("DELETE FROM jobs WHERE id = $1", [job.id]);
    }
  });

  it("reports accepted then queued through the public job status route", async () => {
    const app = buildApp(store, { logger: false });
    let jobId: string | undefined;

    try {
      const submission = await app.inject({
        method: "POST",
        url: "/jobs",
        payload: { payload: { marker: randomUUID() } },
      });
      expect(submission.statusCode).toBe(201);
      const submittedJob = submission.json();
      jobId = submittedJob.id;
      expect(submittedJob.status).toBe("accepted");

      const acceptedStatus = await app.inject({
        method: "GET",
        url: `/jobs/${jobId}`,
      });
      expect(acceptedStatus.statusCode).toBe(200);
      expect(acceptedStatus.json().status).toBe("accepted");

      const publisher = new OutboxPublisher(
        storeForJob(jobId),
        queuePublisher,
        console,
      );
      await expect(publisher.publishNext()).resolves.toBe(true);

      const queuedStatus = await app.inject({
        method: "GET",
        url: `/jobs/${jobId}`,
      });
      expect(queuedStatus.statusCode).toBe(200);
      expect(queuedStatus.json().status).toBe("queued");
      expect(await queue.getJob(jobId)).not.toBeNull();
    } finally {
      await app.close();
      if (jobId) {
        await (await queue.getJob(jobId))?.remove();
        await pool.query("DELETE FROM jobs WHERE id = $1", [jobId]);
      }
    }
  });

  it("keeps a job accepted during a Redis outage and publishes it after recovery", async () => {
    const job = await store.createJob({
      payload: { marker: randomUUID() },
      priority: 0,
      maxRetries: 3,
      timeoutMs: 30_000,
    });

    try {
      const unavailableQueue = {
        publish: async () => {
          throw new Error("Redis unavailable");
        },
        close: async () => {},
        onError: () => {},
      };
      const logger = { error: console.error, warn: console.warn };
      const failingPublisher = new OutboxPublisher(
        storeForJob(job.id),
        unavailableQueue,
        logger,
      );
      await expect(failingPublisher.publishNext()).resolves.toBe(true);

      const failed = await pool.query(
        `SELECT jobs.status, outbox.attempts, outbox.last_error
         FROM jobs
         JOIN job_outbox AS outbox ON outbox.job_id = jobs.id
         WHERE jobs.id = $1`,
        [job.id],
      );
      expect(failed.rows[0]).toMatchObject({
        status: "accepted",
        attempts: 1,
        last_error: "Redis unavailable",
      });

      await pool.query(
        "UPDATE job_outbox SET available_at = NOW() WHERE job_id = $1",
        [job.id],
      );
      const recoveringPublisher = new OutboxPublisher(
        storeForJob(job.id),
        queuePublisher,
        console,
      );
      await expect(recoveringPublisher.publishNext()).resolves.toBe(true);

      expect(await queue.getJob(job.id)).not.toBeNull();
      const recovered = await pool.query("SELECT status FROM jobs WHERE id = $1", [
        job.id,
      ]);
      expect(recovered.rows[0]?.status).toBe("queued");
    } finally {
      await (await queue.getJob(job.id))?.remove();
      await pool.query("DELETE FROM jobs WHERE id = $1", [job.id]);
    }
  });

  it("retries safely when Redis accepted a job but the database acknowledgement failed", async () => {
    const job = await store.createJob({
      payload: { marker: randomUUID() },
      priority: 0,
      maxRetries: 3,
      timeoutMs: 30_000,
    });
    let failFirstAcknowledgement = true;
    const uncertainStore: OutboxStore = {
      claimNextOutboxMessage: () => store.claimNextOutboxMessage(job.id),
      markOutboxMessagePublished: async (jobId) => {
        if (failFirstAcknowledgement) {
          failFirstAcknowledgement = false;
          throw new Error("PostgreSQL acknowledgement unavailable");
        }
        await store.markOutboxMessagePublished(jobId);
      },
      scheduleOutboxRetry: (jobId, delayMs, error) =>
        store.scheduleOutboxRetry(jobId, delayMs, error),
    };

    try {
      const publisher = new OutboxPublisher(uncertainStore, queuePublisher, console);
      await expect(publisher.publishNext()).resolves.toBe(true);
      await pool.query(
        "UPDATE job_outbox SET available_at = NOW() WHERE job_id = $1",
        [job.id],
      );
      await expect(publisher.publishNext()).resolves.toBe(true);

      expect(await queue.getJob(job.id)).not.toBeNull();
      const result = await pool.query(
        `SELECT jobs.status, outbox.attempts, outbox.published_at
         FROM jobs
         JOIN job_outbox AS outbox ON outbox.job_id = jobs.id
         WHERE jobs.id = $1`,
        [job.id],
      );
      expect(result.rows[0]?.status).toBe("queued");
      expect(result.rows[0]?.attempts).toBe(2);
      expect(result.rows[0]?.published_at).toBeInstanceOf(Date);
      expect(await queue.getJobCounts("waiting")).toMatchObject({ waiting: 1 });
    } finally {
      await (await queue.getJob(job.id))?.remove();
      await pool.query("DELETE FROM jobs WHERE id = $1", [job.id]);
    }
  });
});
