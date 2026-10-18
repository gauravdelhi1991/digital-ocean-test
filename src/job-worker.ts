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
): Promise<unknown> {
  const controller = new AbortController();
  let timeout: ReturnType<typeof setTimeout> | undefined;
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

  try {
    return await Promise.race([execution, timeoutPromise]);
  } finally {
    if (timeout) {
      clearTimeout(timeout);
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
    await persistFailure(job, error, store, logger);
    throw error;
  }

  try {
    const result = await runWithTimeout(job, handler);
    await store.markJobSucceeded(job.id, result);
    return result;
  } catch (error) {
    await persistFailure(job, error, store, logger);
    throw error;
  }
}

async function persistFailure(
  job: Job,
  error: unknown,
  store: WorkerJobStore,
  logger: WorkerLogger,
): Promise<void> {
  const message = errorMessage(error).slice(0, 4000);
  try {
    if (job.attemptCount <= job.maxRetries) {
      await store.markJobRetryable(job.id, message);
    } else {
      await store.markJobDeadLettered(job.id, message);
    }
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
