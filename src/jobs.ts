import { randomUUID } from "node:crypto";
import type { Pool } from "pg";

export interface NewJob {
  payload: Record<string, unknown>;
  priority: number;
  maxRetries: number;
  timeoutMs: number;
}

export interface Job {
  id: string;
  payload: Record<string, unknown>;
  priority: number;
  maxRetries: number;
  timeoutMs: number;
  attemptCount: number;
  status: "accepted";
  createdAt: string;
}

export interface JobStore {
  createJob(input: NewJob): Promise<Job>;
  checkConnection(): Promise<void>;
}

interface JobRow {
  id: string;
  payload: Record<string, unknown>;
  priority: number;
  max_retries: number;
  timeout_ms: number;
  attempt_count: number;
  status: "accepted";
  created_at: Date;
}

export class PostgresJobStore implements JobStore {
  constructor(private readonly pool: Pool) {}

  async createJob(input: NewJob): Promise<Job> {
    const result = await this.pool.query<JobRow>(
      `INSERT INTO jobs (id, payload, priority, max_retries, timeout_ms)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id, payload, priority, max_retries, timeout_ms, attempt_count, status, created_at`,
      [randomUUID(), input.payload, input.priority, input.maxRetries, input.timeoutMs],
    );
    const row = result.rows[0];
    if (!row) {
      throw new Error("PostgreSQL did not return the inserted job");
    }

    return {
      id: row.id,
      payload: row.payload,
      priority: row.priority,
      maxRetries: row.max_retries,
      timeoutMs: row.timeout_ms,
      attemptCount: row.attempt_count,
      status: row.status,
      createdAt: row.created_at.toISOString(),
    };
  }

  async checkConnection(): Promise<void> {
    await this.pool.query("SELECT 1");
  }
}
