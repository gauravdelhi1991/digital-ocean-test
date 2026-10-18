import { createDatabasePool } from "./database";
import { createJobWorker } from "./job-worker";
import { PostgresJobStore } from "./jobs";

async function main(): Promise<void> {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error("DATABASE_URL must be configured");
  }
  const redisConnectionString = process.env.REDIS_URL;
  if (!redisConnectionString) {
    throw new Error("REDIS_URL must be configured");
  }

  const parsedConcurrency = Number(process.env.WORKER_CONCURRENCY ?? "5");
  if (!Number.isInteger(parsedConcurrency) || parsedConcurrency < 1) {
    throw new Error("WORKER_CONCURRENCY must be a positive integer");
  }

  const pool = createDatabasePool(connectionString);
  const worker = createJobWorker(
    redisConnectionString,
    new PostgresJobStore(pool),
    {
      error(fields, message) {
        console.error(message, fields);
      },
    },
    undefined,
    parsedConcurrency,
  );
  worker.on("ready", () => {
    console.info("Job worker is ready");
  });

  let closing: Promise<void> | undefined;
  const shutdown = () => {
    closing ??= worker.close().then(() => pool.end());
    return closing;
  };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
  await worker.waitUntilReady();
}

main().catch((error: unknown) => {
  console.error("Worker startup failed", error);
  process.exitCode = 1;
});
