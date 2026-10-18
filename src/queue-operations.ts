import { Queue } from "bullmq";
import type {
  JobCancellationResult,
  JobStore,
  QueueDatabaseMetrics,
} from "./jobs";
import { JOB_QUEUE_NAME, parseRedisConnectionOptions } from "./job-queue";

export interface QueueDepth {
  accepted: number;
  queued: number;
  running: number;
  delayed: number;
  deadLettered: number;
  backlogLagSeconds: number | null;
  draining: boolean;
}

export interface QueueOperationsLogger {
  warn(fields: object, message: string): void;
}

export interface QueueOperations {
  getQueueDepth(): Promise<QueueDepth>;
  drain(): Promise<void>;
  resume(): Promise<void>;
  cancelJob(id: string): Promise<
    | { outcome: "cancelled"; queueCleanup: "removed" | "not-enqueued" | "deferred" }
    | { outcome: "requested" }
    | { outcome: "not-cancellable"; status: string }
    | { outcome: "not-found" }
  >;
  close(): Promise<void>;
}

export function queueDepthFromMetrics(
  database: QueueDatabaseMetrics,
  queueCounts: Record<string, number>,
  draining: boolean,
): QueueDepth {
  return {
    accepted: database.accepted,
    queued:
      (queueCounts.waiting ?? 0) +
      (queueCounts.paused ?? 0) +
      (queueCounts.prioritized ?? 0),
    running: Math.max(database.running, queueCounts.active ?? 0),
    delayed: database.delayed + (queueCounts.delayed ?? 0),
    deadLettered: database.deadLettered,
    backlogLagSeconds: database.backlogLagSeconds,
    draining,
  };
}

export function createQueueOperations(
  connectionString: string,
  store: JobStore,
  logger: QueueOperationsLogger,
  queueName = JOB_QUEUE_NAME,
): QueueOperations {
  const queue = new Queue<{ jobId: string }>(queueName, {
    connection: parseRedisConnectionOptions(connectionString),
  });

  return {
    async getQueueDepth() {
      const [databaseMetrics, queueCounts, draining] = await Promise.all([
        store.getQueueDatabaseMetrics(),
        queue.getJobCounts(
          "waiting",
          "active",
          "delayed",
          "prioritized",
        ),
        queue.isPaused(),
      ]);
      const pausedCount = await (await queue.getBackend().client).llen(
        queue.toKey("paused"),
      );
      return queueDepthFromMetrics(
        databaseMetrics,
        { ...queueCounts, paused: pausedCount },
        draining,
      );
    },
    async drain() {
      await queue.pause();
    },
    async resume() {
      await queue.resume();
    },
    async cancelJob(id) {
      const cancellation: JobCancellationResult =
        await store.requestJobCancellation(id);
      if (cancellation.outcome !== "cancelled") {
        return cancellation;
      }

      try {
        const queueJob = await queue.getJob(id);
        if (!queueJob) {
          return { outcome: "cancelled", queueCleanup: "not-enqueued" };
        }
        if ((await queueJob.getState()) === "active") {
          return { outcome: "cancelled", queueCleanup: "deferred" };
        }
        await queueJob.remove();
        return { outcome: "cancelled", queueCleanup: "removed" };
      } catch (error) {
        logger.warn(
          { err: error, jobId: id },
          "Cancelled job remains in the queue for worker acknowledgement",
        );
        return { outcome: "cancelled", queueCleanup: "deferred" };
      }
    },
    close: () => queue.close(),
  };
}
