import { randomUUID } from "node:crypto";
import { Queue } from "bullmq";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { buildApp } from "../../src/app";
import {
  createJobQueuePublisher,
  parseRedisConnectionOptions,
} from "../../src/job-queue";
import { createJobWorker } from "../../src/job-worker";
import { PostgresJobStore, type OutboxStore } from "../../src/jobs";
import { OutboxPublisher } from "../../src/outbox-publisher";
import { runMigrations } from "../../src/migration-runner";
import { createQueueOperations } from "../../src/queue-operations";
import type { JobQueuePublisher } from "../../src/outbox-publisher";

const databaseUrl = process.env.TEST_DATABASE_URL;
const redisUrl = process.env.TEST_REDIS_URL;

describe.skipIf(!databaseUrl || !redisUrl)("transactional outbox integration", () => {
  const queueName = `outbox-test-${randomUUID()}`;
  const workerQueueName = `worker-test-${randomUUID()}`;
  let pool: Pool;
  let store: PostgresJobStore;
  let queue: Queue;
  let workerQueue: Queue;
  let workerQueuePublisher: JobQueuePublisher;
  let queuePublisher: JobQueuePublisher;
  let jobWorker: ReturnType<typeof createJobWorker>;
  let queueOperations: ReturnType<typeof createQueueOperations>;

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
    workerQueue = new Queue(workerQueueName, {
      connection: parseRedisConnectionOptions(redisUrl),
    });
    queuePublisher = createJobQueuePublisher(redisUrl, queueName);
    workerQueuePublisher = createJobQueuePublisher(redisUrl, workerQueueName);
    await runMigrations(pool);
    queueOperations = createQueueOperations(
      redisUrl,
      store,
      { warn: vi.fn() },
      queueName,
    );
    jobWorker = createJobWorker(redisUrl, store, console, workerQueueName, 1);
    await jobWorker.waitUntilReady();
  });

  afterAll(async () => {
    await jobWorker.close();
    await queueOperations.close();
    await queue.obliterate({ force: true });
    await workerQueue.obliterate({ force: true });
    await queue.close();
    await workerQueue.close();
    await queuePublisher.close();
    await workerQueuePublisher.close();
    await pool.end();
  });

  it("atomically stores a job and publishes its outbox message", async () => {
    const job = await store.createJob({
      payload: { marker: randomUUID() },
      priority: 0,
      maxRetries: 3,
      timeoutMs: 30_000,
    });

    it("does not publish a scheduled job before its due time", async () => {
      const scheduledAt = new Date(Date.now() + 60_000);
      const job = await store.createJob({
        payload: { marker: randomUUID() },
        priority: 0,
        maxRetries: 0,
        timeoutMs: 30_000,
        scheduledAt,
      });

      try {
        expect(job.scheduledAt).toBe(scheduledAt.toISOString());
        await expect(
          pool.query(
            "SELECT available_at FROM job_outbox WHERE job_id = $1",
            [job.id],
          ),
        ).resolves.toMatchObject({
          rows: [expect.objectContaining({ available_at: scheduledAt })],
        });

        const publisher = new OutboxPublisher(
          storeForJob(job.id),
          queuePublisher,
          console,
        );
        await expect(publisher.publishNext()).resolves.toBe(false);
        await expect(queue.getJob(job.id)).resolves.toBeUndefined();
        await expect(store.getJob(job.id)).resolves.toMatchObject({
          status: "accepted",
          scheduledAt: scheduledAt.toISOString(),
        });

        await pool.query(
          "UPDATE job_outbox SET available_at = NOW() WHERE job_id = $1",
          [job.id],
        );
        await expect(publisher.publishNext()).resolves.toBe(true);
        await expect(queue.getJob(job.id)).resolves.toBeDefined();
        await expect(store.getJob(job.id)).resolves.toMatchObject({
          status: "queued",
          scheduledAt: scheduledAt.toISOString(),
        });
      } finally {
        await (await queue.getJob(job.id))?.remove();
        await pool.query("DELETE FROM jobs WHERE id = $1", [job.id]);
      }
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
      expect(queuedJob?.opts).toMatchObject({
        attempts: job.maxRetries + 1,
        backoff: { type: "custom", delay: 1000 },
        priority: 101 - job.priority,
      });
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

  it("reports queue depth and globally drains and resumes work", async () => {
    const jobId = randomUUID();
    await queue.add("execute", { jobId }, { jobId });

    try {
      const beforeDrain = await queueOperations.getQueueDepth();
      expect(beforeDrain.queued).toBeGreaterThanOrEqual(1);
      expect(beforeDrain.draining).toBe(false);

      await queueOperations.drain();
      const draining = await queueOperations.getQueueDepth();
      expect(draining.draining).toBe(true);
      expect(draining.queued).toBeGreaterThanOrEqual(1);

      await queueOperations.resume();
      expect((await queueOperations.getQueueDepth()).draining).toBe(false);
    } finally {
      await queueOperations.resume();
      await (await queue.getJob(jobId))?.remove();
    }
  });

  it("cancels a queued job and removes its BullMQ entry", async () => {
    const job = await store.createJob({
      payload: { type: "example", marker: randomUUID() },
      priority: 0,
      maxRetries: 0,
      timeoutMs: 30_000,
    });

    try {
      const publisher = new OutboxPublisher(
        storeForJob(job.id),
        queuePublisher,
        console,
      );
      await expect(publisher.publishNext()).resolves.toBe(true);

      await expect(queueOperations.cancelJob(job.id)).resolves.toEqual({
        outcome: "cancelled",
        queueCleanup: "removed",
      });
      await expect(store.getJob(job.id)).resolves.toMatchObject({
        status: "cancelled",
      });
      await expect(queue.getJob(job.id)).resolves.toBeUndefined();
    } finally {
      await (await queue.getJob(job.id))?.remove();
      await pool.query("DELETE FROM jobs WHERE id = $1", [job.id]);
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
      expect(await queue.getJobCounts("prioritized")).toMatchObject({
        prioritized: 1,
      });
    } finally {
      await (await queue.getJob(job.id))?.remove();
      await pool.query("DELETE FROM jobs WHERE id = $1", [job.id]);
    }
  });

  it("consumes a queued job and persists its result", async () => {
    const job = await store.createJob({
      payload: { type: "example", marker: randomUUID() },
      priority: 0,
      maxRetries: 1,
      timeoutMs: 30_000,
    });

    try {
      const publisher = new OutboxPublisher(
        storeForJob(job.id),
        workerQueuePublisher,
        console,
      );
      await expect(publisher.publishNext()).resolves.toBe(true);
      await vi.waitFor(
        async () => {
          const completed = await store.getJob(job.id);
          expect(completed?.status).toBe("succeeded");
        },
        { timeout: 10_000 },
      );

      const completed = await store.getJob(job.id);
      expect(completed).toMatchObject({
        status: "succeeded",
        attemptCount: 1,
        result: { payload: job.payload },
      });
    } finally {
      await (await workerQueue.getJob(job.id))?.remove();
      await pool.query("DELETE FROM jobs WHERE id = $1", [job.id]);
    }
  });

  it("executes a message before its PostgreSQL publication acknowledgement", async () => {
    const job = await store.createJob({
      payload: { type: "example", marker: randomUUID() },
      priority: 0,
      maxRetries: 1,
      timeoutMs: 30_000,
    });
    let markStarted!: () => void;
    let allowMark!: () => void;
    const markStartedPromise = new Promise<void>((resolve) => {
      markStarted = resolve;
    });
    const acknowledgementGate = new Promise<void>((resolve) => {
      allowMark = resolve;
    });
    const delayedAcknowledgementStore: OutboxStore = {
      claimNextOutboxMessage: () => store.claimNextOutboxMessage(job.id),
      markOutboxMessagePublished: async (jobId) => {
        markStarted();
        await acknowledgementGate;
        await store.markOutboxMessagePublished(jobId);
      },
      scheduleOutboxRetry: (jobId, delayMs, error) =>
        store.scheduleOutboxRetry(jobId, delayMs, error),
    };
    let publishTask: Promise<boolean> | undefined;

    try {
      const publisher = new OutboxPublisher(
        delayedAcknowledgementStore,
        workerQueuePublisher,
        console,
      );
      publishTask = publisher.publishNext();
      await markStartedPromise;
      await vi.waitFor(
        async () => {
          const completed = await store.getJob(job.id);
          expect(completed?.status).toBe("succeeded");
        },
        { timeout: 10_000 },
      );
      allowMark();
      await expect(publishTask).resolves.toBe(true);
    } finally {
      allowMark();
      await publishTask?.catch(() => {});
      await (await workerQueue.getJob(job.id))?.remove();
      await pool.query("DELETE FROM jobs WHERE id = $1", [job.id]);
    }
  });

  it("stores exhausted worker failures in the PostgreSQL dead-letter table", async () => {
    const job = await store.createJob({
      payload: { type: "unsupported", marker: randomUUID() },
      priority: 0,
      maxRetries: 1,
      timeoutMs: 30_000,
    });

    try {
      const publisher = new OutboxPublisher(
        storeForJob(job.id),
        workerQueuePublisher,
        console,
      );
      await expect(publisher.publishNext()).resolves.toBe(true);
      await vi.waitFor(
        async () => {
          const deadLettered = await store.getJob(job.id);
          expect(deadLettered?.status).toBe("dead-lettered");
        },
        { timeout: 15_000 },
      );

      const result = await pool.query(
        `SELECT payload, attempt_count, error
         FROM job_dead_letters WHERE job_id = $1`,
        [job.id],
      );
      expect(result.rows[0]).toMatchObject({
        payload: job.payload,
        attempt_count: 2,
        error: "Unsupported job payload type: unsupported",
      });
    } finally {
      await (await workerQueue.getJob(job.id))?.remove();
      await pool.query("DELETE FROM job_dead_letters WHERE job_id = $1", [job.id]);
      await pool.query("DELETE FROM jobs WHERE id = $1", [job.id]);
    }
  });
});
