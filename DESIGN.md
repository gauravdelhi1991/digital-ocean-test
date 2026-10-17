# Asynchronous Job Service: Design and Implementation Plan

## Goal

Build a production-ready REST API for accepting jobs, executing them asynchronously with retry and timeout policies, and exposing lifecycle status, results, and operational controls. Deliver in small, deployable increments.

## Architecture

Use TypeScript with Fastify for the HTTP API, PostgreSQL as the durable job registry and result store, Redis with BullMQ for asynchronous delivery, and separate worker and scheduler processes. Deploy the API and managed PostgreSQL on DigitalOcean first; add Redis and independently scalable workers in later increments. Keep the App Platform app specification in source control; provision the managed database cluster through DigitalOcean and attach it to the app. Use GitHub Actions for code validation and gated deployments.

```text
Client
  |
  v
Fastify API ---- transaction ----> PostgreSQL job registry
                                      |
                                      | transactional outbox (later)
                                      v
                                  Publisher
                                      |
                                      v
                              Redis / BullMQ queue
                                      |
                                      v
                              Independent workers
                               |              |
                         success/result   retry with capped
                           in PostgreSQL   exponential backoff
                                              |
                                  retries exhausted / permanent
                                              v
                                  Dead-lettered job in PostgreSQL

Scheduler (later) ---> PostgreSQL schedules ---> Redis delayed/recurring jobs
```

## Job lifecycle and delivery semantics

- At the first milestone, `POST /jobs` validates and persists a job as `accepted`; it does not claim to enqueue or execute it.
- A later transactional-outbox milestone atomically records a job and its publish intent. A publisher retries outbox records until Redis accepts them.
- Workers consume BullMQ jobs and persist attempt and completion state. Queue delivery and execution are at least once, not exactly once: a crash between performing a side effect and recording completion can cause a redelivery.
- Bound duplicate work using stable job IDs, conditional state transitions, and handler-level idempotency keys. Handlers that perform external side effects must make those effects idempotent where possible.
- Retry transient errors with capped exponential backoff and jitter. Persist the final error and dead-letter state when retries are exhausted; do not silently discard failed work.

## API and data

Initial API:

- `POST /jobs`: validate a JSON object payload and optional `priority`, `maxRetries`, and `timeoutMs`; persist it and return its ID with `status: accepted`.
- `GET /health/live` and `GET /health/ready`: process liveness and database readiness.

Later API:

- `GET /jobs/{id}`: state, attempts, last error, result, and lifecycle timestamps.
- `GET /jobs/queue-depth`: queued, running, delayed, and dead-letter counts plus backlog lag.
- Cancellation and drain/resume operations for pending work and graceful worker shutdown.

The PostgreSQL `jobs` table stores payload, scheduling/execution options, status, attempt count, last error, result, and timestamps. The initial migration includes the future lifecycle states; jobs remain `accepted` until the publisher and workers are implemented. A transactional outbox, schedules, and a dead-letter audit store are added in their corresponding milestones.

## Retry, timeout, scheduling, and operations

- `maxRetries` counts retries after the initial attempt. Retry delay is capped exponential backoff with jitter.
- A timeout applies to each individual execution attempt.
- Delayed jobs become eligible no earlier than their scheduled time. A separate scheduler process reads durable schedule definitions and publishes due work; cron/recurring definitions and execution history live in PostgreSQL.
- Drain stops workers claiming new jobs while in-flight jobs finish. Cancellation is supported for queued jobs, with cooperative cancellation for running handlers.
- Structured metrics cover queue depth, worker utilization, job latency (p50/p95), retries, timeouts, and dead-letter rate. Logs include job ID, handler type, attempt, duration, and outcome.

## DigitalOcean and delivery pipeline

- Run a stateless API service and managed PostgreSQL on DigitalOcean. Keep credentials out of source control and inject them using DigitalOcean secrets.
- Scale the API and worker pool independently. Add managed Redis before enabling asynchronous execution.
- Track the DigitalOcean App Platform specification in source control. GitHub Actions validates, tests, and builds on pushes and pull requests. Production deployment is a separate, gated workflow requiring GitHub and DigitalOcean credentials.
- Configure database backups, connection limits/pooling, health checks, migrations, and alerting before production traffic.

## High-load approach

Scale worker replicas independently according to backlog and utilization. Apply API request limits, database connection pooling, queue backpressure, per-handler concurrency limits, and bounded payload sizes. Store large payload artifacts in object storage and retain only references in PostgreSQL. Partition queues by workload/priority only when measurements justify it; monitor backlog lag and p95 job latency during load tests.

## Sequential implementation tasks

1. **Build and deploy the database-backed submission service.** Implement the TypeScript API, validation, PostgreSQL schema/migrations, readiness/liveness checks, tests, container setup, DigitalOcean infrastructure configuration, and deployment documentation. Submit jobs as `accepted`; do not imply they are queued or processed yet.
2. **Add code-managed CI and gated deployment.** Run lint, unit/integration tests, and builds in GitHub Actions. Add explicit protected deployment steps and secrets for DigitalOcean.
3. **Add status and result APIs.** Return complete lifecycle state, attempt count, errors, results, and timestamps with correct transitions and tests.
4. **Add Redis, reliable publication, and transactional outbox.** Provision managed Redis; atomically persist jobs and outbox intents; retry publishing safely and test Redis outages/restarts.
5. **Implement independently deployable workers.** Add pluggable typed handlers, job claiming, durable attempt state, acknowledgements, and crash/redelivery integration tests.
6. **Add timeouts, retry/backoff, and dead-letter handling.** Test retry classification, attempt limits, timeouts, permanent failures, and dead-letter retention.
7. **Add queue visibility and operational controls.** Report backlog depth/lag, support cancellation and drain/resume, and secure operator endpoints.
8. **Add delayed and recurring scheduling.** Persist schedules, run a recoverable scheduler, publish jobs no earlier than their due time, and define missed/overlapping run behavior.
9. **Add observability and production hardening.** Emit structured logs and required metrics; add alerts, retention, backup/recovery, load, and failure tests.
10. **Scale and validate the DigitalOcean deployment.** Independently scale API and workers, exercise autoscaling/backpressure, and document deployment, configuration, operations, and delivery guarantees.
