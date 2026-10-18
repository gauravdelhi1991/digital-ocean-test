import { describe, expect, it, vi } from "vitest";
import { OutboxPublisher, retryDelayMs } from "../src/outbox-publisher";
import type { ClaimedOutboxMessage, OutboxStore } from "../src/jobs";
import type { JobQueuePublisher } from "../src/outbox-publisher";

function createStore(message: ClaimedOutboxMessage | null) {
  const store: OutboxStore = {
    claimNextOutboxMessage: vi.fn(async () => message),
    markOutboxMessagePublished: vi.fn(async () => {}),
    scheduleOutboxRetry: vi.fn(async () => {}),
  };
  return store;
}

function createQueue(
  publish: JobQueuePublisher["publish"],
): JobQueuePublisher {
  return {
    publish,
    close: vi.fn(async () => {}),
    onError: vi.fn(),
  };
}

describe("outbox publisher", () => {
  it("publishes and marks a claimed message", async () => {
    const message = {
      jobId: "job-1",
      attempt: 1,
      maxRetries: 3,
      priority: 10,
    };
    const store = createStore(message);
    const publish = vi.fn(async () => {});
    const logger = { error: vi.fn(), warn: vi.fn() };
    const publisher = new OutboxPublisher(store, createQueue(publish), logger);

    await expect(publisher.publishNext()).resolves.toBe(true);

    expect(publish).toHaveBeenCalledWith("job-1", {
      maxRetries: 3,
      priority: 10,
    });
    expect(store.markOutboxMessagePublished).toHaveBeenCalledWith("job-1");
    expect(store.scheduleOutboxRetry).not.toHaveBeenCalled();
  });

  it("schedules a retry and preserves the accepted job when publishing fails", async () => {
    const message = {
      jobId: "job-2",
      attempt: 2,
      maxRetries: 3,
      priority: 0,
    };
    const store = createStore(message);
    const logger = { error: vi.fn(), warn: vi.fn() };
    const publisher = new OutboxPublisher(
      store,
      createQueue(async () => {
        throw new Error("Redis unavailable");
      }),
      logger,
    );

    await expect(publisher.publishNext()).resolves.toBe(true);

    expect(store.scheduleOutboxRetry).toHaveBeenCalledWith(
      "job-2",
      expect.any(Number),
      "Redis unavailable",
    );
    expect(store.markOutboxMessagePublished).not.toHaveBeenCalled();
    expect(logger.warn).toHaveBeenCalledOnce();
  });

  it("uses capped exponential retry delays with jitter", () => {
    expect(retryDelayMs(1, () => 0)).toBe(500);
    expect(retryDelayMs(2, () => 0.5)).toBe(2000);
    expect(retryDelayMs(20, () => 1)).toBe(60_000);
    expect(() => retryDelayMs(0)).toThrow(RangeError);
  });
});
