# Asynchronous Job Service: Design and Implementation Plan

## Goal

Build a production-ready REST API for accepting jobs, executing them asynchronously with retry and timeout policies, and exposing lifecycle status, results, and operational controls. Deliver in small, deployable increments.

## Architecture

Use TypeScript with Fastify for the HTTP API, PostgreSQL as the durable job registry, result store, and dead-letter audit, Valkey (Redis-compatible) with BullMQ for asynchronous delivery, and separate worker and scheduler processes. Host the API as an App Platform service and execute queued work in a separate App Platform worker component. Keep infrastructure in Terraform and use GitHub Actions for CI, remote-state bootstrap, and manually triggered Terraform plans/applies. Store Terraform state in a private DigitalOcean Spaces bucket. DigitalOcean Functions are not part of this design.

```text
Client
  |
  v
Fastify API ---- transaction ----> PostgreSQL job registry
                                      |
                                      | transactional outbox
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

- `POST /jobs` validates and transactionally persists a job and its outbox intent as `accepted`.
- The API's outbox publisher claims records with expiring leases, retries failures with capped exponential backoff and jitter, and marks a job `queued` only after BullMQ/Redis accepts it. Stable job IDs make ambiguous publish retries deduplicate while the queue record exists.
- Workers consume BullMQ jobs and persist attempt and completion state. Queue delivery and execution are at least once, not exactly once: a crash between performing a side effect and recording completion can cause a redelivery.
- Bound duplicate work using stable job IDs, conditional state transitions, and handler-level idempotency keys. Handlers that perform external side effects must make those effects idempotent where possible.
- Retry execution failures with capped exponential backoff and jitter. Persist the final error and dead-letter state transactionally when retries are exhausted; do not silently discard failed work.

## API and data

Initial API:

- `POST /jobs`: validate a JSON object payload and optional `priority`, `maxRetries`, and `timeoutMs`; persist it and return its ID with `status: accepted`.
- `GET /jobs/{id}`: return the stored job, including its current state, attempt count, last error, result, and lifecycle timestamps; return `404` for an unknown UUID.
- `GET /health/live` and `GET /health/ready`: process liveness and database readiness.

The initial worker dispatches on `payload.type` and supports only `example`, which returns the submitted payload. Unsupported types fail, follow the configured retry policy, and enter the PostgreSQL dead-letter table when retries are exhausted.

Operational API (operator bearer token required):

- `GET /ops/queue-depth`: accepted, queued, running, delayed, and dead-letter counts, oldest backlog lag, and drain state.
- `POST /ops/drain` and `POST /ops/resume`: globally stop/resume new worker claims while active jobs finish.
- `POST /ops/jobs/{id}/cancel`: cancel pending work or request cooperative cancellation of a running handler.

The PostgreSQL `jobs` table stores payload, scheduling/execution options, status, attempt count, last error, result, and timestamps. Jobs remain `accepted` until publication succeeds, then remain `queued` until a worker processes them. Exhausted failures are stored transactionally in `job_dead_letters`; schedule tables are added in the scheduling milestone.

## Retry, timeout, scheduling, and operations

- `maxRetries` counts retries after the initial attempt. Retry delay is capped exponential backoff with jitter.
- A timeout applies to each individual execution attempt.
- Delayed jobs become eligible no earlier than their scheduled time. A separate scheduler process reads durable schedule definitions and publishes due work; cron/recurring definitions and execution history live in PostgreSQL.
- Drain stops workers claiming new jobs while in-flight jobs finish. Cancellation is supported for queued jobs, with cooperative cancellation for running handlers.
- Structured metrics cover queue depth, worker utilization, job latency (p50/p95), retries, timeouts, and dead-letter rate. Logs include job ID, handler type, attempt, duration, and outcome.

## DigitalOcean and delivery pipeline

- Run a stateless API service and separate worker, with managed PostgreSQL and Valkey, on DigitalOcean, grouped in a dedicated DigitalOcean Project. Keep credentials out of source control and inject them using DigitalOcean secrets.
- Scale API and worker independently using App Platform dedicated-CPU autoscaling with a 70% CPU target and a one-to-three instance range. App Platform currently uses the same target for scaling up and down and does not expose separate 40%/five-minute scale-down settings.
- Track DigitalOcean infrastructure in Terraform. GitHub Actions validates, tests, and builds on pushes and pull requests. A manually triggered workflow creates a Spaces state bucket and runs Terraform plan/apply; applying requires an explicitly protected GitHub environment and DigitalOcean/Spaces secrets. Managed PostgreSQL and Valkey are firewall-restricted to the App Platform API.
- Configure database backups, connection limits/pooling, health checks, migrations, and alerting before production traffic.

## High-load approach

Scale worker replicas independently according to backlog and utilization. Apply API request limits, database connection pooling, queue backpressure, per-handler concurrency limits, and bounded payload sizes. Store large payload artifacts in object storage and retain only references in PostgreSQL. Partition queues by workload/priority only when measurements justify it; monitor backlog lag and p95 job latency during load tests.

## Sequential implementation tasks

1. **Build and provision the database-backed submission service.** Implement the TypeScript API, validation, PostgreSQL schema/migrations, readiness/liveness checks, tests, container setup, Terraform for DigitalOcean Managed PostgreSQL and App Platform, a Spaces remote-state bootstrap workflow, a gated Terraform deployment workflow, and deployment documentation. Submit jobs as `accepted`; do not imply they are queued or processed yet.
2. **Expand CI and release safeguards.** Add broader PostgreSQL-backed integration coverage, branch protection, deployment verification, and release promotion practices to the existing GitHub Actions CI and gated infrastructure workflow.
3. **Add status and result APIs.** Return complete lifecycle state, attempt count, errors, results, and timestamps with correct transitions and tests.
4. **Add Redis, reliable publication, and transactional outbox.** Provision managed Valkey (Redis-compatible); atomically persist jobs and outbox intents; retry publishing safely and test Redis outages/restarts.
5. **Implement independently deployable App Platform workers.** Complete for the initial handler: the separate worker consumes BullMQ messages, persists execution state and results, and has PostgreSQL/Redis integration coverage.
6. **Add timeouts, retry/backoff, and dead-letter handling.** Complete for the current handler contract: per-attempt timeouts, exponential queue backoff, and durable dead-letter persistence are implemented.
7. **Add queue visibility and operational controls.** Report backlog depth/lag, support cancellation and drain/resume, and secure operator endpoints.
8. **Add delayed and recurring scheduling.** Persist schedules, run a recoverable scheduler, publish jobs no earlier than their due time, and define missed/overlapping run behavior.
9. **Add observability and production hardening.** Emit structured logs and required metrics; add alerts, retention, backup/recovery, load, and failure tests.
10. **Scale and validate the DigitalOcean deployment.** Independently scale API and workers, exercise autoscaling/backpressure, and document deployment, configuration, operations, and delivery guarantees.
