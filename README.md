# Digital Ocean Test

A TypeScript REST API that durably accepts job submissions in PostgreSQL. This first milestone stores jobs as `accepted`; it does not enqueue or execute them yet. Redis, workers, retries, scheduling, and status/result lookup are later milestones described in [DESIGN.md](./DESIGN.md).

## Requirements

- Node.js 22 or newer
- PostgreSQL 16 or newer

## Configure and run locally

1. Create a PostgreSQL database named `jobs`.
2. Copy `.env.example` to `.env` and set `DATABASE_URL` to your PostgreSQL connection string.
3. Install dependencies and apply the schema:

   ```powershell
   npm ci
   npm run migrate
   ```

4. Start the API:

   ```powershell
   npm run dev
   ```

The API listens on `http://localhost:3000`. `GET /health/live` checks process liveness; `GET /health/ready` checks database connectivity.

## Submit a job

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:3000/jobs `
  -ContentType 'application/json' `
  -Body '{"payload":{"type":"example","value":42},"priority":0,"maxRetries":3,"timeoutMs":30000}'
```

`payload` must be a JSON object. `priority` defaults to `0` and must be between `-100` and `100`; `maxRetries` defaults to `3` and must be between `0` and `20`; `timeoutMs` defaults to `30000` and must be between `100` and `3600000`. A successful request returns HTTP `201` and a generated job ID with status `accepted`, only after the database insert succeeds. This milestone intentionally does not claim queue delivery or execution.

## Migrations, tests, and build

```powershell
npm run migrate
npm test
npm run build
```

Unit tests run without external services. To run the PostgreSQL integration test, set `TEST_DATABASE_URL` to a disposable test database and run:

```powershell
npm run test:integration
```

Never point integration tests at production: the migration creates tables and the test inserts records.

## DigitalOcean deployment

The App Platform specification at `.do/app.yaml` defines the API service and attaches it to the production managed PostgreSQL cluster named `digital-ocean-test-db`. Creating a managed cluster and running an App Platform service incur ongoing charges; check the current price shown in your DigitalOcean account and approve those costs before provisioning. Install `doctl`, authenticate with your DigitalOcean account, and create a PostgreSQL 16 cluster in `nyc3` using a supported single-node plan (for example, `db-s-1vcpu-1gb`):

```powershell
doctl databases create digital-ocean-test-db --engine pg --version 16 --region nyc3 --size db-s-1vcpu-1gb --num-nodes 1
```

Use the returned cluster ID to create the database and user:

```powershell
$clusterId = '<cluster-id-returned-above>'
doctl databases db create $clusterId jobs
doctl databases user create $clusterId jobs
```

Connect the GitHub repository to DigitalOcean App Platform, then create the app from its tracked specification:

```powershell
doctl apps create --spec .do/app.yaml
```

For an existing app, update the spec with `doctl apps update <app-id> --spec .do/app.yaml`. When the managed cluster is attached, verify that the app is listed as a trusted source. The managed database URL is supplied to the service as a secret environment variable; do not commit database credentials. The container runs the idempotent database migration before starting the API, with a PostgreSQL advisory lock to serialize simultaneous service starts. Verify `/health/ready` after deployment. The first deployment requires an authorized DigitalOcean account and GitHub repository integration; this specification does not create resources until explicitly applied.

## Configuration

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `DATABASE_URL` | Yes | — | PostgreSQL connection URI |
| `HOST` | No | `0.0.0.0` | HTTP bind address |
| `PORT` | No | `3000` | HTTP port |
| `LOG_LEVEL` | No | `info` | Structured service log level |

## Worker configuration and high load

There are no workers or queue settings in this first milestone. Later milestones add Redis/BullMQ, separately scalable worker processes, handler concurrency, retry/backoff and timeout policies, and dead-letter tracking. For high load, scale API instances independently from workers, use connection pooling and queue backpressure, cap request/payload sizes, and monitor queue lag and p95 job latency. Large job data should be stored as object-storage references rather than embedded in the request.
