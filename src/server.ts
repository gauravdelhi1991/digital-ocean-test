import { buildApp } from "./app";
import { createDatabasePool } from "./database";
import { createJobQueuePublisher } from "./job-queue";
import { PostgresJobStore } from "./jobs";
import { runMigrations } from "./migration-runner";
import { OutboxPublisher } from "./outbox-publisher";
import { createQueueOperations } from "./queue-operations";

async function main(): Promise<void> {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error("DATABASE_URL must be configured");
  }
  const redisConnectionString = process.env.REDIS_URL;
  if (!redisConnectionString) {
    throw new Error("REDIS_URL must be configured");
  }
  const operatorToken = process.env.OPERATOR_TOKEN;
  if (!operatorToken || operatorToken.length < 32) {
    throw new Error("OPERATOR_TOKEN must contain at least 32 characters");
  }

  const pool = createDatabasePool(connectionString);
  try {
    await runMigrations(pool);
  } catch (error) {
    await pool.end();
    throw error;
  }
  const store = new PostgresJobStore(pool);
  const queueOperations = createQueueOperations(redisConnectionString, store, {
    warn(fields, message) {
      console.warn(message, fields);
    },
  });
  const app = buildApp(store, {
    logger: { level: process.env.LOG_LEVEL ?? "info" },
    operatorToken,
    queueOperations,
  });
  const queue = createJobQueuePublisher(redisConnectionString);
  const publisher = new OutboxPublisher(store, queue, app.log);
  const abortController = new AbortController();
  let publisherTask: Promise<void> | undefined;

  queue.onError((error) => {
    app.log.error({ err: error }, "Queue connection error");
  });

  app.addHook("onClose", async () => {
    abortController.abort();
    await publisherTask;
    await queue.close();
    await queueOperations.close();
    await pool.end();
  });

  const port = Number(process.env.PORT ?? "3000");
  const host = process.env.HOST ?? "0.0.0.0";
  await app.listen({ port, host });
  publisherTask = publisher.run(abortController.signal);

  const shutdown = async () => {
    await app.close();
  };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
}

main().catch((error: unknown) => {
  console.error("Service startup failed", error);
  process.exitCode = 1;
});
