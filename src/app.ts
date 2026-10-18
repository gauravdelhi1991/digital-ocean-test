import { timingSafeEqual } from "node:crypto";
import Fastify, {
  type FastifyReply,
  type FastifyRequest,
  type FastifyServerOptions,
} from "fastify";
import { z } from "zod";
import type { JobStore } from "./jobs";
import type { QueueOperations } from "./queue-operations";

const submitJobSchema = z
  .object({
    payload: z.record(z.string(), z.unknown()),
    priority: z.number().int().min(-100).max(100).default(0),
    maxRetries: z.number().int().min(0).max(20).default(3),
    timeoutMs: z.number().int().min(100).max(3_600_000).default(30_000),
    scheduledAt: z
      .string()
      .datetime({ offset: true })
      .transform((value) => new Date(value))
      .optional()
      .transform((value) => value ?? null),
  })
  .strict();

const jobParamsSchema = z.object({ id: z.string().uuid() });

export function buildApp(
  store: JobStore,
  options: {
    logger?: FastifyServerOptions["logger"];
    operatorToken?: string;
    queueOperations?: QueueOperations;
  } = {},
) {
  const app = Fastify({
    logger: options.logger ?? true,
    bodyLimit: 1_048_576,
  });
  const authorizeOperator = async (
    request: FastifyRequest,
    reply: FastifyReply,
  ) => {
    const expectedToken = options.operatorToken;
    if (!expectedToken) {
      return reply.code(503).send({ error: "Operator access is not configured" });
    }

    const authorization = request.headers.authorization;
    const match = authorization?.match(/^Bearer ([^\s]+)$/i);
    if (!match?.[1]) {
      return reply.code(401).send({ error: "Operator authorization required" });
    }

    const provided = Buffer.from(match[1]);
    const expected = Buffer.from(expectedToken);
    if (
      provided.length !== expected.length ||
      !timingSafeEqual(provided, expected)
    ) {
      return reply.code(401).send({ error: "Operator authorization required" });
    }
  };

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

  app.get("/jobs/:id", async (request, reply) => {
    const parsed = jobParamsSchema.safeParse(request.params);
    if (!parsed.success) {
      return reply.code(400).send({
        error: "Invalid job ID",
        details: parsed.error.issues.map((issue) => ({
          path: issue.path.join("."),
          message: issue.message,
        })),
      });
    }

    try {
      const job = await store.getJob(parsed.data.id);
      if (!job) {
        return reply.code(404).send({ error: "Job not found" });
      }
      return job;
    } catch (error) {
      request.log.error({ err: error }, "Job status could not be retrieved");
      return reply.code(500).send({ error: "Job status could not be retrieved" });
    }
  });

  app.get(
    "/ops/queue-depth",
    { preHandler: authorizeOperator },
    async (request, reply) => {
      if (!options.queueOperations) {
        return reply.code(503).send({ error: "Queue operations are unavailable" });
      }
      try {
        return await options.queueOperations.getQueueDepth();
      } catch (error) {
        request.log.error({ err: error }, "Queue depth could not be retrieved");
        return reply.code(503).send({ error: "Queue depth is unavailable" });
      }
    },
  );

  app.post(
    "/ops/drain",
    { preHandler: authorizeOperator },
    async (request, reply) => {
      if (!options.queueOperations) {
        return reply.code(503).send({ error: "Queue operations are unavailable" });
      }
      try {
        await options.queueOperations.drain();
        return { status: "draining" };
      } catch (error) {
        request.log.error({ err: error }, "Queue drain could not be started");
        return reply.code(503).send({ error: "Queue drain could not be started" });
      }
    },
  );

  app.post(
    "/ops/resume",
    { preHandler: authorizeOperator },
    async (request, reply) => {
      if (!options.queueOperations) {
        return reply.code(503).send({ error: "Queue operations are unavailable" });
      }
      try {
        await options.queueOperations.resume();
        return { status: "resumed" };
      } catch (error) {
        request.log.error({ err: error }, "Queue could not be resumed");
        return reply.code(503).send({ error: "Queue could not be resumed" });
      }
    },
  );

  app.post(
    "/ops/jobs/:id/cancel",
    { preHandler: authorizeOperator },
    async (request, reply) => {
      if (!options.queueOperations) {
        return reply.code(503).send({ error: "Queue operations are unavailable" });
      }
      const parsed = jobParamsSchema.safeParse(request.params);
      if (!parsed.success) {
        return reply.code(400).send({ error: "Invalid job ID" });
      }

      try {
        const result = await options.queueOperations.cancelJob(parsed.data.id);
        if (result.outcome === "not-found") {
          return reply.code(404).send({ error: "Job not found" });
        }
        if (result.outcome === "not-cancellable") {
          return reply.code(409).send({
            error: "Job cannot be cancelled",
            status: result.status,
          });
        }
        if (result.outcome === "requested") {
          return reply.code(202).send({ status: "cancellation-requested" });
        }
        return {
          status: "cancelled",
          queueCleanup: result.queueCleanup,
        };
      } catch (error) {
        request.log.error({ err: error }, "Job cancellation could not be completed");
        return reply.code(500).send({ error: "Job cancellation could not be completed" });
      }
    },
  );

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
