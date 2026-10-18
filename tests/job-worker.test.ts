import { describe, expect, it, vi } from "vitest";
import {
  executeQueuedJob,
  executionRetryDelayMs,
} from "../src/job-worker";
import type { Job } from "../src/jobs";
import type { WorkerJobStore } from "../src/jobs";

const sampleJob: Job = {
  id: "d2719a42-75b1-4339-b4f5-c640f86aed7f",
  payload: { type: "example", value: 42 },
  priority: 0,
  maxRetries: 2,
  timeoutMs: 1000,
  attemptCount: 1,
  status: "running",
  lastError: null,
  result: null,
  createdAt: "2026-10-17T00:00:00.000Z",
  updatedAt: "2026-10-17T00:00:00.000Z",
  startedAt: "2026-10-17T00:00:00.000Z",
  completedAt: null,
  cancellationRequestedAt: null,
};

function createStore(job: Job | null = sampleJob) {
  const store: WorkerJobStore = {
    startJobAttempt: vi.fn(async () => job),
    markJobRetryable: vi.fn(async () => "retryable" as const),
    markJobSucceeded: vi.fn(async () => "succeeded" as const),
    markJobDeadLettered: vi.fn(async () => "dead-lettered" as const),
    isCancellationRequested: vi.fn(async () => false),
    markJobCancelled: vi.fn(async () => {}),
  };
  return store;
}

describe("job worker", () => {
  it("uses capped exponential retry delays with jitter", () => {
    expect(executionRetryDelayMs(1, () => 0)).toBe(500);
    expect(executionRetryDelayMs(2, () => 1)).toBe(2000);
    expect(executionRetryDelayMs(20, () => 1)).toBe(60_000);
    expect(() => executionRetryDelayMs(0)).toThrow(RangeError);
  });

  it("runs the example handler and persists its result", async () => {
    const store = createStore();
    const logger = { error: vi.fn() };

    await expect(
      executeQueuedJob(sampleJob.id, store, logger),
    ).resolves.toEqual({ payload: sampleJob.payload });

    expect(store.startJobAttempt).toHaveBeenCalledWith(sampleJob.id);
    expect(store.markJobSucceeded).toHaveBeenCalledWith(sampleJob.id, {
      payload: sampleJob.payload,
    });
    expect(store.markJobRetryable).not.toHaveBeenCalled();
  });

  it("persists retryable failures and rethrows them for BullMQ backoff", async () => {
    const job = { ...sampleJob, attemptCount: 1, maxRetries: 2 };
    const store = createStore(job);
    const logger = { error: vi.fn() };
    const error = new Error("temporary failure");

    await expect(
      executeQueuedJob(
        job.id,
        store,
        logger,
        { example: () => Promise.reject(error) },
      ),
    ).rejects.toBe(error);

    expect(store.markJobRetryable).toHaveBeenCalledWith(job.id, error.message);
    expect(store.markJobDeadLettered).not.toHaveBeenCalled();
  });

  it("acknowledges a retryable failure when cancellation wins the persistence race", async () => {
    const job = { ...sampleJob, attemptCount: 1, maxRetries: 2 };
    const store = createStore(job);
    vi.mocked(store.markJobRetryable).mockResolvedValue("cancelled");

    await expect(
      executeQueuedJob(
        job.id,
        store,
        { error: vi.fn() },
        { example: () => Promise.reject(new Error("temporary failure")) },
      ),
    ).resolves.toBeNull();

    expect(store.markJobRetryable).toHaveBeenCalledWith(
      job.id,
      "temporary failure",
    );
  });

  it("writes an exhausted failure to the dead-letter store", async () => {
    const job = { ...sampleJob, attemptCount: 3, maxRetries: 2 };
    const store = createStore(job);
    const logger = { error: vi.fn() };

    await expect(
      executeQueuedJob(
        job.id,
        store,
        logger,
        { example: () => Promise.reject(new Error("permanent failure")) },
      ),
    ).rejects.toThrow("permanent failure");

    expect(store.markJobDeadLettered).toHaveBeenCalledWith(
      job.id,
      "permanent failure",
    );
    expect(store.markJobRetryable).not.toHaveBeenCalled();
  });

  it("does not dead-letter a job when cancellation wins the persistence race", async () => {
    const job = { ...sampleJob, attemptCount: 3, maxRetries: 2 };
    const store = createStore(job);
    vi.mocked(store.markJobDeadLettered).mockResolvedValue("cancelled");

    await expect(
      executeQueuedJob(
        job.id,
        store,
        { error: vi.fn() },
        { example: () => Promise.reject(new Error("permanent failure")) },
      ),
    ).resolves.toBeNull();

    expect(store.markJobDeadLettered).toHaveBeenCalledWith(
      job.id,
      "permanent failure",
    );
  });

  it("fails unsupported job types rather than silently succeeding", async () => {
    const job = { ...sampleJob, payload: { type: "unknown" } };
    const store = createStore(job);
    const logger = { error: vi.fn() };

    await expect(executeQueuedJob(job.id, store, logger)).rejects.toThrow(
      "Unsupported job payload type: unknown",
    );

    expect(store.markJobRetryable).toHaveBeenCalledWith(
      job.id,
      "Unsupported job payload type: unknown",
    );
  });

  it("aborts a handler and records a timeout failure", async () => {
    const job = { ...sampleJob, timeoutMs: 5 };
    const store = createStore(job);
    const logger = { error: vi.fn() };
    let signal: AbortSignal | undefined;

    await expect(
      executeQueuedJob(
        job.id,
        store,
        logger,
        {
          example: (_payload, context) => {
            signal = context.signal;
            return new Promise(() => {});
          },
        },
      ),
    ).rejects.toThrow("Job execution timed out after 5ms");

    expect(signal?.aborted).toBe(true);
    expect(store.markJobRetryable).toHaveBeenCalledWith(
      job.id,
      "Job execution timed out after 5ms",
    );
  });

  it("does not rerun jobs that are already terminal", async () => {
    const store = createStore(null);
    const logger = { error: vi.fn() };

    await expect(executeQueuedJob(sampleJob.id, store, logger)).resolves.toBeNull();
    expect(store.markJobSucceeded).not.toHaveBeenCalled();
  });

  it("aborts a running handler when cooperative cancellation is requested", async () => {
    const store = createStore();
    vi.mocked(store.isCancellationRequested)
      .mockResolvedValueOnce(false)
      .mockResolvedValue(true);
    const logger = { error: vi.fn() };
    let signal: AbortSignal | undefined;

    await expect(
      executeQueuedJob(
        sampleJob.id,
        store,
        logger,
        {
          example: (_payload, context) => {
            signal = context.signal;
            return new Promise((_, reject) => {
              context.signal.addEventListener(
                "abort",
                () => reject(new Error("handler aborted")),
                { once: true },
              );
            });
          },
        },
      ),
    ).resolves.toBeNull();

    expect(signal?.aborted).toBe(true);
    expect(store.markJobCancelled).toHaveBeenCalledWith(sampleJob.id);
    expect(store.markJobRetryable).not.toHaveBeenCalled();
  });
});
