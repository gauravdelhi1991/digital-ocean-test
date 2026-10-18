import { randomUUID } from "node:crypto";
import type { Pool } from "pg";

export interface NewJob {
  payload: Record<string, unknown>;
  priority: number;
  maxRetries: number;
  timeoutMs: number;
}

export type JobStatus =
  | "accepted"
  | "queued"
  | "running"
  | "succeeded"
  | "failed"
  | "dead-lettered"
  | "cancelled";

export interface Job {
  id: string;
  payload: Record<string, unknown>;
  priority: number;
  maxRetries: number;
  timeoutMs: number;
  attemptCount: number;
  status: JobStatus;
  lastError: string | null;
  result: unknown | null;
  createdAt: string;
  updatedAt: string;
  startedAt: string | null;
  completedAt: string | null;
}

export interface JobStore {
  createJob(input: NewJob): Promise<Job>;
  getJob(id: string): Promise<Job | null>;
  checkConnection(): Promise<void>;
}

export interface ClaimedOutboxMessage {
  jobId: string;
  attempt: number;
  maxRetries: number;
  priority: number;
}

export interface OutboxStore {
  claimNextOutboxMessage(jobId?: string): Promise<ClaimedOutboxMessage | null>;
  markOutboxMessagePublished(jobId: string): Promise<void>;
  scheduleOutboxRetry(jobId: string, delayMs: number, error: string): Promise<void>;
}

export interface WorkerJobStore {
  startJobAttempt(jobId: string): Promise<Job | null>;
  markJobRetryable(jobId: string, error: string): Promise<void>;
  markJobSucceeded(jobId: string, result: unknown): Promise<void>;
  markJobDeadLettered(jobId: string, error: string): Promise<void>;
}

interface JobRow {
  id: string;
  payload: Record<string, unknown>;
  priority: number;
  max_retries: number;
  timeout_ms: number;
  attempt_count: number;
  status: JobStatus;
  last_error: string | null;
  result: unknown | null;
  created_at: Date;
  updated_at: Date;
  started_at: Date | null;
  completed_at: Date | null;
}

const jobColumns = `
  id, payload, priority, max_retries, timeout_ms, attempt_count, status,
  last_error, result, created_at, updated_at, started_at, completed_at
`;

function mapJob(row: JobRow): Job {
  return {
    id: row.id,
    payload: row.payload,
    priority: row.priority,
    maxRetries: row.max_retries,
    timeoutMs: row.timeout_ms,
    attemptCount: row.attempt_count,
    status: row.status,
    lastError: row.last_error,
    result: row.result,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
    startedAt: row.started_at?.toISOString() ?? null,
    completedAt: row.completed_at?.toISOString() ?? null,
  };
}

export class PostgresJobStore implements JobStore, OutboxStore, WorkerJobStore {
  constructor(private readonly pool: Pool) {}

  async createJob(input: NewJob): Promise<Job> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query<JobRow>(
        `INSERT INTO jobs (id, payload, priority, max_retries, timeout_ms)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING ${jobColumns}`,
        [randomUUID(), input.payload, input.priority, input.maxRetries, input.timeoutMs],
      );
      const row = result.rows[0];
      if (!row) {
        throw new Error("PostgreSQL did not return the inserted job");
      }

      await client.query("INSERT INTO job_outbox (job_id) VALUES ($1)", [row.id]);
      await client.query("COMMIT");
      return mapJob(row);
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async getJob(id: string): Promise<Job | null> {
    const result = await this.pool.query<JobRow>(
      `SELECT ${jobColumns} FROM jobs WHERE id = $1`,
      [id],
    );
    const row = result.rows[0];
    return row ? mapJob(row) : null;
  }

  async checkConnection(): Promise<void> {
    await this.pool.query("SELECT 1");
  }

  async claimNextOutboxMessage(jobId?: string): Promise<ClaimedOutboxMessage | null> {
    const result = await this.pool.query<{
      job_id: string;
      attempts: number;
      max_retries: number;
      priority: number;
    }>(`
      WITH next_message AS (
        SELECT outbox.job_id
        FROM job_outbox AS outbox
        JOIN jobs ON jobs.id = outbox.job_id
        WHERE outbox.published_at IS NULL
          AND ($1::uuid IS NULL OR outbox.job_id = $1)
          AND outbox.available_at <= NOW()
          AND (outbox.claimed_at IS NULL OR outbox.claimed_at < NOW() - INTERVAL '60 seconds')
        ORDER BY outbox.available_at, outbox.created_at
        FOR UPDATE OF outbox SKIP LOCKED
        LIMIT 1
      )
      UPDATE job_outbox AS outbox
      SET claimed_at = NOW(), attempts = attempts + 1
      FROM next_message
      JOIN jobs ON jobs.id = next_message.job_id
      WHERE outbox.job_id = next_message.job_id
      RETURNING outbox.job_id, outbox.attempts, jobs.max_retries, jobs.priority
    `, [jobId ?? null]);
    const row = result.rows[0];
    return row
      ? {
          jobId: row.job_id,
          attempt: row.attempts,
          maxRetries: row.max_retries,
          priority: row.priority,
        }
      : null;
  }

  async markOutboxMessagePublished(jobId: string): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query(
        `UPDATE job_outbox
         SET published_at = NOW(), claimed_at = NULL, last_error = NULL
         WHERE job_id = $1 AND published_at IS NULL`,
        [jobId],
      );
      if (result.rowCount !== 1) {
        throw new Error(`Outbox message for job ${jobId} is not pending publication`);
      }
      await client.query(
        `UPDATE jobs
         SET status = 'queued', updated_at = NOW()
         WHERE id = $1 AND status = 'accepted'`,
        [jobId],
      );
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async scheduleOutboxRetry(
    jobId: string,
    delayMs: number,
    error: string,
  ): Promise<void> {
    await this.pool.query(
      `UPDATE job_outbox
       SET claimed_at = NULL,
           available_at = NOW() + ($2 * INTERVAL '1 millisecond'),
           last_error = $3
       WHERE job_id = $1 AND published_at IS NULL`,
      [jobId, delayMs, error.slice(0, 4000)],
    );
  }

  async startJobAttempt(jobId: string): Promise<Job | null> {
    const result = await this.pool.query<JobRow>(
      `UPDATE jobs
       SET status = 'running', attempt_count = attempt_count + 1,
           started_at = COALESCE(started_at, NOW()), updated_at = NOW()
       WHERE id = $1 AND status IN ('accepted', 'queued', 'running')
       RETURNING ${jobColumns}`,
      [jobId],
    );
    const row = result.rows[0];
    if (row) {
      return mapJob(row);
    }

    const existing = await this.pool.query<{ id: string }>(
      "SELECT id FROM jobs WHERE id = $1",
      [jobId],
    );
    if (!existing.rows[0]) {
      throw new Error(`Queue references unknown job ${jobId}`);
    }
    return null;
  }

  async markJobRetryable(jobId: string, error: string): Promise<void> {
    const result = await this.pool.query(
      `UPDATE jobs
       SET status = 'queued', last_error = $2, updated_at = NOW()
       WHERE id = $1 AND status = 'running'`,
      [jobId, error.slice(0, 4000)],
    );
    if (result.rowCount !== 1) {
      throw new Error(`Running job ${jobId} could not be scheduled for retry`);
    }
  }

  async markJobSucceeded(jobId: string, result: unknown): Promise<void> {
    const update = await this.pool.query(
      `UPDATE jobs
       SET status = 'succeeded', result = $2, last_error = NULL,
           completed_at = NOW(), updated_at = NOW()
       WHERE id = $1 AND status = 'running'`,
      [jobId, result],
    );
    if (update.rowCount !== 1) {
      throw new Error(`Running job ${jobId} could not be marked succeeded`);
    }
  }

  async markJobDeadLettered(jobId: string, error: string): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query<{
        payload: Record<string, unknown>;
        attempt_count: number;
      }>(
        `UPDATE jobs
         SET status = 'dead-lettered', last_error = $2,
             completed_at = NOW(), updated_at = NOW()
         WHERE id = $1 AND status = 'running'
         RETURNING payload, attempt_count`,
        [jobId, error.slice(0, 4000)],
      );
      const row = result.rows[0];
      if (!row) {
        throw new Error(`Running job ${jobId} could not be dead-lettered`);
      }

      await client.query(
        `INSERT INTO job_dead_letters (job_id, payload, attempt_count, error)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (job_id) DO UPDATE
         SET payload = EXCLUDED.payload,
             attempt_count = EXCLUDED.attempt_count,
             error = EXCLUDED.error,
             failed_at = NOW()`,
        [jobId, row.payload, row.attempt_count, error.slice(0, 4000)],
      );
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
}
