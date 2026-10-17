import { Pool } from "pg";
import { buildApp } from "./app";
import { PostgresJobStore } from "./jobs";

async function main(): Promise<void> {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error("DATABASE_URL must be configured");
  }

  const pool = new Pool({ connectionString });
  const app = buildApp(new PostgresJobStore(pool), {
    logger: { level: process.env.LOG_LEVEL ?? "info" },
  });
  app.addHook("onClose", async () => {
    await pool.end();
  });

  const port = Number(process.env.PORT ?? "3000");
  const host = process.env.HOST ?? "0.0.0.0";
  await app.listen({ port, host });

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
