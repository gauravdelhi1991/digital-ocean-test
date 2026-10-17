# Digital Ocean Test

A TypeScript REST API, deployed as an App Platform service, that durably accepts job submissions in PostgreSQL. This first milestone stores jobs as `accepted`; it does not enqueue or execute them yet. Later, a separate App Platform worker will consume Redis jobs and execute the handlers itself. DigitalOcean Functions are not used. Redis, workers, retries, scheduling, and status/result lookup are later milestones described in [DESIGN.md](./DESIGN.md).

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

Unit tests run without external services. The integration test starts the Fastify API on an ephemeral localhost port, sends a real HTTP `POST /jobs` request, and verifies the response and persisted row against PostgreSQL. GitHub Actions starts a disposable PostgreSQL 16 service for the full test run. To run the integration test locally, set `TEST_DATABASE_URL` to a disposable test database and run:

```powershell
npm run test:integration
```

Never point integration tests at production: the migration creates tables and the test inserts records.

## DigitalOcean deployment

Infrastructure is code-managed in `terraform/`. The GitHub Actions workflows create a private DigitalOcean Spaces bucket for Terraform state, then Terraform provisions a managed PostgreSQL cluster, database, firewall rule, and App Platform API service. No DigitalOcean resources are created just by pushing code.

### Estimated monthly cost

The initial configuration uses a single-node PostgreSQL plan (`db-s-1vcpu-1gb`, currently listed at **$15/month**), one shared App Platform API instance (`apps-s-1vcpu-0.5gb`, currently **$5/month**), and the Spaces Standard Storage base subscription (**$5/month**, including 250 GiB storage and 1,024 GiB outbound transfer across buckets). That is approximately **$25/month** before taxes, additional storage/transfer, or other usage. If a Spaces subscription is already active on the account, the state bucket may use that existing subscription instead of adding another base charge. The single-node database is not highly available and is intended for an initial development deployment; check the current prices and plan details in the DigitalOcean account before applying.

- [Managed PostgreSQL pricing](https://docs.digitalocean.com/products/databases/postgresql/details/pricing/)
- [App Platform pricing](https://docs.digitalocean.com/products/app-platform/details/pricing/)
- [Spaces pricing](https://docs.digitalocean.com/products/spaces/details/pricing/)

### One-time GitHub and DigitalOcean setup

1. Connect `gauravdelhi1991/digital-ocean-test` to DigitalOcean App Platform in the DigitalOcean control panel. This authorizes App Platform to build the private GitHub repository. Terraform creates a DigitalOcean project named `Digital Ocean Test` and assigns the API and database to it.
2. Create a persistent DigitalOcean API token and a Spaces access key in the control panel. Add them as GitHub Actions repository secrets named `DIGITALOCEAN_TOKEN`, `DO_SPACES_ACCESS_KEY_ID`, and `DO_SPACES_SECRET_ACCESS_KEY`. Do not paste or commit credentials.
3. In GitHub **Settings → Environments**, create `digitalocean-production` and restrict deployments to the `main` branch. This is a branch restriction, not an approval gate. GitHub Free does not offer required reviewers for private repositories. The workflow therefore requires a separate, manually dispatched apply run with explicit confirmation; this is a deliberate operator action, not an independent reviewer approval.

### Provision

1. Run **Actions → Bootstrap Terraform State Storage → Run workflow**. It creates the private `digital-ocean-test-tfstate-821117` Spaces bucket in `nyc3` if it does not already exist. This starts the Spaces subscription if the account does not already have one.
2. Run **Actions → Deploy DigitalOcean Infrastructure → Run workflow** with `operation: plan`. By default, `deployment_branch` is `main`; to test a feature branch, select it here while launching the workflow from `main`. Review the app source branch and commit along with the resource changes in the completed run’s summary, and compare the plan with the approximate monthly estimate above and current DigitalOcean prices. Note the plan run’s ID. The plan artifact expires after one day; apply is rejected if either the infrastructure commit or selected application branch commit changes.
3. Only after reviewing, run the same workflow from the same `main` commit with `operation: apply`, enter the plan run ID, and set `confirm_plan: yes`. The workflow verifies that the earlier plan run succeeded on the same commit and applies that exact saved plan; it does not silently generate and apply a new plan. The database cluster has `prevent_destroy` protection against accidental removal.
4. After the app deployment completes, verify `https://<app-domain>/health/ready`.

If an App Platform deployment fails, dispatch the same workflow with `operation: inspect` and the app and deployment UUIDs from the failed run. This retrieves that deployment’s status and the API component’s crash logs; it does not change infrastructure.

The API uses the managed cluster’s default database account for its first migration and runtime connection. Restrict database access to the App Platform app using the managed database firewall. Before handling sensitive production workloads, introduce a least-privilege application database role and a separate migration credential.

The API container applies the idempotent migration before starting, using a PostgreSQL advisory lock so simultaneous service starts do not race. Terraform state is stored in a private Spaces bucket; deploy runs are serialized by GitHub Actions.

## Configuration

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `DATABASE_URL` | Yes | — | PostgreSQL connection URI |
| `HOST` | No | `0.0.0.0` | HTTP bind address |
| `PORT` | No | `3000` | HTTP port |
| `LOG_LEVEL` | No | `info` | Structured service log level |

## Worker configuration and high load

There are no workers or queue settings in this first milestone. Later milestones add Redis/BullMQ and a separate App Platform worker component that consumes jobs from Redis and executes handlers itself. Scale that worker pool independently from the API service. Add handler concurrency, retry/backoff and timeout policies, and dead-letter tracking. For high load, use connection pooling and queue backpressure, cap request/payload sizes, and monitor queue lag and p95 job latency. Large job data should be stored as object-storage references rather than embedded in the request.
