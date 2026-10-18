import { Queue, type RedisOptions } from "bullmq";
import type { JobQueuePublisher } from "./outbox-publisher";

export const JOB_QUEUE_NAME = "jobs";

interface JobQueueData {
  jobId: string;
}

export function parseRedisConnectionOptions(connectionString: string): RedisOptions {
  const url = new URL(connectionString);
  if (url.protocol !== "redis:" && url.protocol !== "rediss:") {
    throw new Error("REDIS_URL must use redis:// or rediss://");
  }
  if (!url.hostname) {
    throw new Error("REDIS_URL must include a hostname");
  }

  const pathname = url.pathname.replace(/^\//, "");
  const database = pathname ? Number(pathname) : 0;
  if (!Number.isInteger(database) || database < 0) {
    throw new Error("REDIS_URL path must be a non-negative database number");
  }

  const useTls =
    url.protocol === "rediss:" ||
    url.searchParams.get("sslmode")?.toLowerCase() === "require";

  return {
    host: url.hostname,
    port: url.port ? Number(url.port) : 6379,
    username: url.username ? decodeURIComponent(url.username) : undefined,
    password: url.password ? decodeURIComponent(url.password) : undefined,
    db: database,
    tls: useTls ? {} : undefined,
    connectTimeout: 5000,
    maxRetriesPerRequest: 1,
  };
}

export function createJobQueuePublisher(
  connectionString: string,
  queueName = JOB_QUEUE_NAME,
): JobQueuePublisher {
  const queue = new Queue<JobQueueData>(queueName, {
    connection: parseRedisConnectionOptions(connectionString),
  });

  return {
    async publish(jobId) {
      await queue.add("execute", { jobId }, { jobId });
    },
    close: () => queue.close(),
    onError(handler) {
      queue.on("error", handler);
    },
  };
}
