import type { OutboxStore } from "./jobs";

export interface JobQueuePublisher {
  publish(jobId: string): Promise<void>;
  close(): Promise<void>;
  onError(handler: (error: Error) => void): void;
}

export interface PublisherLogger {
  error(fields: object, message: string): void;
  warn(fields: object, message: string): void;
}

const pollIntervalMs = 1000;
const maximumRetryDelayMs = 60_000;

export function retryDelayMs(attempt: number, random = Math.random): number {
  if (!Number.isInteger(attempt) || attempt < 1) {
    throw new RangeError("Outbox attempt must be a positive integer");
  }
  const baseDelay = Math.min(maximumRetryDelayMs, 1000 * 2 ** Math.min(attempt - 1, 6));
  return Math.min(
    maximumRetryDelayMs,
    Math.floor(baseDelay * (0.5 + random())),
  );
}

function delay(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }

    const timeout = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    const onAbort = () => {
      clearTimeout(timeout);
      resolve();
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

export class OutboxPublisher {
  constructor(
    private readonly store: OutboxStore,
    private readonly queue: JobQueuePublisher,
    private readonly logger: PublisherLogger,
  ) {}

  async publishNext(): Promise<boolean> {
    const message = await this.store.claimNextOutboxMessage();
    if (!message) {
      return false;
    }

    try {
      await this.queue.publish(message.jobId);
      await this.store.markOutboxMessagePublished(message.jobId);
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      try {
        await this.store.scheduleOutboxRetry(
          message.jobId,
          retryDelayMs(message.attempt),
          errorMessage,
        );
      } catch (retryError) {
        this.logger.error(
          { err: retryError, jobId: message.jobId, publishError: errorMessage },
          "Outbox publish failed and retry could not be scheduled",
        );
        throw retryError;
      }
      this.logger.warn(
        { err: error, jobId: message.jobId, attempt: message.attempt },
        "Outbox publish failed; retry scheduled",
      );
    }

    return true;
  }

  async run(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      try {
        const didPublish = await this.publishNext();
        if (!didPublish) {
          await delay(pollIntervalMs, signal);
        }
      } catch (error) {
        this.logger.error({ err: error }, "Outbox polling failed");
        await delay(pollIntervalMs, signal);
      }
    }
  }
}
