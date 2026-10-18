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

export class PostgresJobStore implements JobStore {
  constructor(private readonly pool: Pool) {}

  async createJob(input: NewJob): Promise<Job> {
    const result = await this.pool.query<JobRow>(
      `INSERT INTO jobs (id, payload, priority, max_retries, timeout_ms)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING ${jobColumns}`,
      [randomUUID(), input.payload, input.priority, input.maxRetries, input.timeoutMs],
    );
    const row = result.rows[0];
    if (!row) {
      throw new Error("PostgreSQL did not return the inserted job");
    }

    return mapJob(row);
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
}
