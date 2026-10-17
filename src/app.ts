import Fastify, { type FastifyServerOptions } from "fastify";
import { z } from "zod";
import type { JobStore } from "./jobs";

const submitJobSchema = z
  .object({
    payload: z.record(z.string(), z.unknown()),
    priority: z.number().int().min(-100).max(100).default(0),
    maxRetries: z.number().int().min(0).max(20).default(3),
    timeoutMs: z.number().int().min(100).max(3_600_000).default(30_000),
  })
  .strict();

export function buildApp(
  store: JobStore,
  options: { logger?: FastifyServerOptions["logger"] } = {},
) {
  const app = Fastify({
    logger: options.logger ?? true,
    bodyLimit: 1_048_576,
  });

  app.get("/health/live", async () => ({ status: "ok" }));

  app.get("/health/ready", async (request, reply) => {
    try {
      await store.checkConnection();
      return { status: "ok", database: "available" };
    } catch (error) {
      request.log.error({ err: error }, "Database readiness check failed");
      return reply.code(503).send({ status: "unavailable", database: "unavailable" });
    }
  });

  app.post("/jobs", async (request, reply) => {
    const parsed = submitJobSchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.code(400).send({
        error: "Invalid job submission",
        details: parsed.error.issues.map((issue) => ({
          path: issue.path.join("."),
          message: issue.message,
        })),
      });
    }

    try {
      const job = await store.createJob(parsed.data);
      return reply.code(201).send(job);
    } catch (error) {
      request.log.error({ err: error }, "Job could not be persisted");
      return reply.code(500).send({ error: "Job could not be persisted" });
    }
  });

  return app;
}
