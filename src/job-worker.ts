import { Worker, type Job as BullMQJob } from "bullmq";
import type { Job, WorkerJobStore } from "./jobs";
import { JOB_QUEUE_NAME, parseRedisConnectionOptions } from "./job-queue";

interface JobQueueData {
  jobId: string;
}

export interface JobHandlerContext {
  jobId: string;
  attempt: number;
  signal: AbortSignal;
}

export type JobHandler = (
  payload: Record<string, unknown>,
  context: JobHandlerContext,
) => unknown | Promise<unknown>;

export type JobHandlerRegistry = Readonly<Record<string, JobHandler>>;

export interface WorkerLogger {
  error(fields: object, message: string): void;
}

const defaultHandlers: JobHandlerRegistry = {
  example: async (payload) => ({ payload }),
};

const maximumRetryDelayMs = 60_000;
const cancellationCheckIntervalMs = 500;

class JobCancellationRequestedError extends Error {
  constructor() {
    super("Job cancellation requested");
    this.name = "JobCancellationRequestedError";
  }
}

export function executionRetryDelayMs(
  attempt: number,
  random = Math.random,
): number {
  if (!Number.isInteger(attempt) || attempt < 1) {
    throw new RangeError("Execution attempt must be a positive integer");
  }
  const baseDelay = Math.min(
    maximumRetryDelayMs,
    1000 * 2 ** Math.min(attempt - 1, 6),
  );
  return Math.min(
    maximumRetryDelayMs,
    Math.floor(baseDelay * (0.5 + random() * 0.5)),
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function runWithTimeout(
  job: Job,
  handler: JobHandler,
  isCancellationRequested: () => Promise<boolean>,
): Promise<unknown> {
  const controller = new AbortController();
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let cancellationCheck: ReturnType<typeof setInterval> | undefined;
  let cancellationCheckInFlight = false;
  let cancellationRequested = false;
  let cancellationCheckError: unknown;
  const execution = Promise.resolve().then(() =>
    handler(job.payload, {
      jobId: job.id,
      attempt: job.attemptCount,
      signal: controller.signal,
    }),
  );
  const timeoutPromise = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => {
      controller.abort();
      reject(new Error(`Job execution timed out after ${job.timeoutMs}ms`));
    }, job.timeoutMs);
  });
  const checkCancellation = async () => {
    if (cancellationCheckInFlight || cancellationRequested || cancellationCheckError) {
      return;
    }
    cancellationCheckInFlight = true;
    try {
      if (await isCancellationRequested()) {
        cancellationRequested = true;
        controller.abort();
      }
    } catch (error) {
      cancellationCheckError = error;
      controller.abort();
    } finally {
      cancellationCheckInFlight = false;
    }
  };
  void checkCancellation();
  cancellationCheck = setInterval(
    () => void checkCancellation(),
    cancellationCheckIntervalMs,
  );

  try {
    const result = await Promise.race([execution, timeoutPromise]);
    if (cancellationCheckError) {
      throw cancellationCheckError;
    }
    if (cancellationRequested) {
      throw new JobCancellationRequestedError();
    }
    return result;
  } catch (error) {
    if (cancellationRequested) {
      throw new JobCancellationRequestedError();
    }
    if (cancellationCheckError) {
      throw cancellationCheckError;
    }
    throw error;
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
    if (cancellationCheck) {
      clearInterval(cancellationCheck);
    }
  }
}

export async function executeQueuedJob(
  jobId: string,
  store: WorkerJobStore,
  logger: WorkerLogger,
  handlers: JobHandlerRegistry = defaultHandlers,
): Promise<unknown> {
  const job = await store.startJobAttempt(jobId);
  if (!job) {
    return null;
  }

  const handlerType = job.payload.type;
  const handler =
    typeof handlerType === "string" ? handlers[handlerType] : undefined;
  if (!handler) {
    const error = new Error(
      `Unsupported job payload type: ${String(handlerType ?? "undefined")}`,
    );
    const cancelled = await persistFailure(job, error, store, logger);
    if (cancelled) {
      return null;
    }
    throw error;
  }

  try {
    const result = await runWithTimeout(
      job,
      handler,
      () => store.isCancellationRequested(job.id),
    );
    const completion = await store.markJobSucceeded(job.id, result);
    return completion === "cancelled" ? null : result;
  } catch (error) {
    if (error instanceof JobCancellationRequestedError) {
      await store.markJobCancelled(job.id);
      return null;
    }
    const cancelled = await persistFailure(job, error, store, logger);
    if (cancelled) {
      return null;
    }
    throw error;
  }
}

async function persistFailure(
  job: Job,
  error: unknown,
  store: WorkerJobStore,
  logger: WorkerLogger,
): Promise<boolean> {
  const message = errorMessage(error).slice(0, 4000);
  try {
    if (await store.isCancellationRequested(job.id)) {
      await store.markJobCancelled(job.id);
      return true;
    }
    if (job.attemptCount <= job.maxRetries) {
      return (await store.markJobRetryable(job.id, message)) === "cancelled";
    }
    return (await store.markJobDeadLettered(job.id, message)) === "cancelled";
  } catch (persistenceError) {
    logger.error(
      { err: persistenceError, jobId: job.id, executionError: message },
      "Job failure could not be recorded",
    );
    throw persistenceError;
  }
}

export function createJobWorker(
  connectionString: string,
  store: WorkerJobStore,
  logger: WorkerLogger,
  queueName = JOB_QUEUE_NAME,
  concurrency = 5,
): Worker<JobQueueData> {
  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new RangeError("Worker concurrency must be a positive integer");
  }

  const connection = parseRedisConnectionOptions(connectionString);
  connection.maxRetriesPerRequest = null;
  const worker = new Worker<JobQueueData>(
    queueName,
    async (queueJob: BullMQJob<JobQueueData>) =>
      executeQueuedJob(queueJob.data.jobId, store, logger),
    {
      connection,
      concurrency,
      settings: {
        backoffStrategy: (attemptsMade, type) =>
          type === "custom" ? executionRetryDelayMs(attemptsMade) : -1,
      },
    },
  );
  worker.on("error", (error) => {
    logger.error({ err: error }, "Job worker connection error");
  });
  worker.on("failed", (queueJob, error) => {
    logger.error(
      {
        err: error,
        jobId: queueJob?.data.jobId,
        attemptsMade: queueJob?.attemptsMade,
      },
      "Queue job attempt failed",
    );
  });
  return worker;
}
