import { describe, expect, it, vi } from "vitest";
import { buildApp } from "../src/app";
import type { JobStore, NewJob } from "../src/jobs";

function createStore(overrides: Partial<JobStore> = {}) {
  const createJob = vi.fn(async (input: NewJob) => ({
    id: "d2719a42-75b1-4339-b4f5-c640f86aed7f",
    ...input,
    attemptCount: 0,
    status: "accepted" as const,
    lastError: null,
    result: null,
    createdAt: "2026-10-17T00:00:00.000Z",
    updatedAt: "2026-10-17T00:00:00.000Z",
    startedAt: null,
    completedAt: null,
  }));
  const getJob = vi.fn(async () => null);
  const checkConnection = vi.fn(async () => {});
  return {
    store: { createJob, getJob, checkConnection, ...overrides } as JobStore,
    createJob,
    getJob,
    checkConnection,
  };
}

describe("job submission API", () => {
  it("persists a valid job and returns accepted status with defaults", async () => {
    const { store, createJob } = createStore();
    const app = buildApp(store, { logger: false });

    const response = await app.inject({
      method: "POST",
      url: "/jobs",
      payload: { payload: { operation: "import" } },
    });

    expect(response.statusCode).toBe(201);
    expect(response.json()).toMatchObject({
      id: "d2719a42-75b1-4339-b4f5-c640f86aed7f",
      payload: { operation: "import" },
      priority: 0,
      maxRetries: 3,
      timeoutMs: 30_000,
      attemptCount: 0,
      status: "accepted",
    });
    expect(createJob).toHaveBeenCalledWith({
      payload: { operation: "import" },
      priority: 0,
      maxRetries: 3,
      timeoutMs: 30_000,
    });
    await app.close();
  });

  it("accepts explicit retry, timeout, and priority values", async () => {
    const { store, createJob } = createStore();
    const app = buildApp(store, { logger: false });

    const response = await app.inject({
      method: "POST",
      url: "/jobs",
      payload: {
        payload: { operation: "export" },
        priority: 10,
        maxRetries: 5,
        timeoutMs: 60_000,
      },
    });

    expect(response.statusCode).toBe(201);
    expect(createJob).toHaveBeenCalledWith({
      payload: { operation: "export" },
      priority: 10,
      maxRetries: 5,
      timeoutMs: 60_000,
    });
    await app.close();
  });

  it("returns the current job status and lifecycle details", async () => {
    const job = {
      id: "d2719a42-75b1-4339-b4f5-c640f86aed7f",
      payload: { operation: "import" },
      priority: 0,
      maxRetries: 3,
      timeoutMs: 30_000,
      attemptCount: 2,
      status: "failed" as const,
      lastError: "upstream unavailable",
      result: null,
      createdAt: "2026-10-17T00:00:00.000Z",
      updatedAt: "2026-10-17T00:02:00.000Z",
      startedAt: "2026-10-17T00:01:00.000Z",
      completedAt: "2026-10-17T00:02:00.000Z",
    };
    const getJob = vi.fn(async () => job);
    const { store } = createStore({ getJob });
    const app = buildApp(store, { logger: false });

    const response = await app.inject({
      method: "GET",
      url: `/jobs/${job.id}`,
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual(job);
    expect(getJob).toHaveBeenCalledWith(job.id);
    await app.close();
  });

  it("rejects malformed job IDs", async () => {
    const { store, getJob } = createStore();
    const app = buildApp(store, { logger: false });

    const response = await app.inject({ method: "GET", url: "/jobs/not-a-uuid" });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toMatchObject({ error: "Invalid job ID" });
    expect(getJob).not.toHaveBeenCalled();
    await app.close();
  });

  it("returns not found for an unknown job ID", async () => {
    const { store, getJob } = createStore();
    const app = buildApp(store, { logger: false });

    const response = await app.inject({
      method: "GET",
      url: "/jobs/d2719a42-75b1-4339-b4f5-c640f86aed7f",
    });

    expect(response.statusCode).toBe(404);
    expect(response.json()).toEqual({ error: "Job not found" });
    expect(getJob).toHaveBeenCalledWith("d2719a42-75b1-4339-b4f5-c640f86aed7f");
    await app.close();
  });

  it("returns an error when status retrieval fails", async () => {
    const { store } = createStore({
      getJob: vi.fn(async () => {
        throw new Error("database unavailable");
      }),
    });
    const app = buildApp(store, { logger: false });

    const response = await app.inject({
      method: "GET",
      url: "/jobs/d2719a42-75b1-4339-b4f5-c640f86aed7f",
    });

    expect(response.statusCode).toBe(500);
    expect(response.json()).toEqual({ error: "Job status could not be retrieved" });
    await app.close();
  });

  it.each([
    [{ payload: [] }, "payload must be an object"],
    [{ payload: { operation: "import" }, maxRetries: -1 }, "retry limit must be valid"],
    [{ payload: { operation: "import" }, timeoutMs: 99 }, "timeout must be valid"],
    [{ payload: { operation: "import" }, unexpected: true }, "unknown fields must be rejected"],
  ])("rejects invalid submissions (%s)", async (payload) => {
    const { store, createJob } = createStore();
    const app = buildApp(store, { logger: false });

    const response = await app.inject({
      method: "POST",
      url: "/jobs",
      payload,
    });

    expect(response.statusCode).toBe(400);
    expect(createJob).not.toHaveBeenCalled();
    await app.close();
  });

  it("returns an error when durable persistence fails", async () => {
    const { store } = createStore({
      createJob: vi.fn(async () => {
        throw new Error("database unavailable");
      }),
    });
    const app = buildApp(store, { logger: false });

    const response = await app.inject({
      method: "POST",
      url: "/jobs",
      payload: { payload: { operation: "import" } },
    });

    expect(response.statusCode).toBe(500);
    expect(response.json()).toEqual({ error: "Job could not be persisted" });
    await app.close();
  });

  it("reports liveness without requiring a database connection", async () => {
    const { store, checkConnection } = createStore({
      checkConnection: vi.fn(async () => {
        throw new Error("database unavailable");
      }),
    });
    const app = buildApp(store, { logger: false });

    const response = await app.inject({ method: "GET", url: "/health/live" });

    expect(response.statusCode).toBe(200);
    expect(checkConnection).not.toHaveBeenCalled();
    await app.close();
  });

  it("reports readiness failures when the database cannot be reached", async () => {
    const { store } = createStore({
      checkConnection: vi.fn(async () => {
        throw new Error("database unavailable");
      }),
    });
    const app = buildApp(store, { logger: false });

    const response = await app.inject({ method: "GET", url: "/health/ready" });

    expect(response.statusCode).toBe(503);
    expect(response.json()).toEqual({
      status: "unavailable",
      database: "unavailable",
    });
    await app.close();
  });
});
