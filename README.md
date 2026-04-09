# STRANTAS QUANTA V1

Headless underwriting intake and quote-preparation service for group insurance RFP submissions.

## Milestone

- Milestone: `V1`
- Branch checkpoint: `master`
- Recommended Git tag: `v1`
- Current validation baseline: `46 passed` integration tests

## What V1 Includes

- FastAPI API for email intake, parsing, extraction, normalization, and output generation
- Canonical quote, LOB, census, evidence, and carrier-aligned domain models
- Postgres-backed persistence for submissions, quotes, LOBs, census, outputs, jobs, alerts, mailbox configs, connector cursors, idempotency keys, and replay audits
- S3-compatible object storage support via MinIO or AWS S3
- JSON, multipart, provider-email, and SMTP webhook intake paths
- Gmail and Microsoft Graph connector execution
- Tenant-aware inbound mailbox configuration for Gmail and Microsoft Graph
- Durable inbound email queue with connector poll and connector ingest jobs
- Multi-tenant background worker that refreshes readiness, polls connectors, drains jobs, and monitors queue lag
- Gmail history-based incremental fetch for push-driven ingestion
- ZIP attachment expansion with provenance tracking for extracted child files
- OCR fallback for scanned PDFs when `tesseract` is installed
- Field-level extraction results with value, confidence, evidence, and warnings
- Carrier-aligned output including richer dental and vision structures
- Dead-letter handling, replay endpoints, bulk replay, replay audit persistence, queue lag metrics, and queue lag alerting

## Production-Shaped Intake Flow

1. Customer routes RFP mail to a configured mailbox.
2. Provider event or scheduled polling detects a new message.
3. QUANTA enqueues a durable inbound job.
4. Worker drains the queue.
5. Connector fetches the message and attachments.
6. Intake persists raw provider payload and normalized submission envelope.
7. Reader, extractor, normalizer, and output pipeline run.
8. Queue, replay, lag, and alert state remain visible through admin APIs.

## Repository Layout

- `apps/api/quanta_api/main.py`: FastAPI app entrypoint
- `apps/api/quanta_api/worker_main.py`: dedicated worker process entrypoint
- `apps/api/quanta_api/api/routes.py`: API routes
- `apps/api/quanta_api/bootstrap.py`: service container wiring
- `apps/api/quanta_api/services/`: ingestion, orchestration, queue, worker, extraction, normalization
- `apps/api/quanta_api/storage/`: Postgres and in-memory adapters
- `alembic/`: schema migrations
- `tests/integration/`: end-to-end integration coverage
- `scripts/`: local bootstrap, fixture generation, worker helper
- `samples/`: sample census and RFP fixtures
- `docs/`: architecture notes, examples, and local verification guidance
- `postman/`: Postman collection for API testing

## Local Run

### API

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
uvicorn quanta_api.main:app --app-dir apps/api --host 127.0.0.1 --port 8000
```

### Worker

Run the worker as a separate process:

```bash
source .venv/bin/activate
python scripts/run_worker.py
```

Recommended process split:

- API process: `QUANTA_CONNECTOR_WORKER_ENABLED=false`
- Worker process: `QUANTA_CONNECTOR_WORKER_ENABLED=true`

## Local Dev Stack

```bash
./scripts/bootstrap_dev.sh
set -a; source .env; set +a
source .venv/bin/activate
uvicorn quanta_api.main:app --app-dir apps/api --host 127.0.0.1 --port 8000
```

This starts:

- Postgres on `127.0.0.1:5432`
- MinIO API on `127.0.0.1:9100`
- MinIO console on `127.0.0.1:9101`
- Alembic schema at `head`
- generated local sample fixtures

## Migrations

```bash
source .venv/bin/activate
set -a; source .env; set +a
alembic upgrade head
```

Reset locally:

```bash
source .venv/bin/activate
python scripts/reset_dev_db.py
alembic upgrade head
```

## API Docs

- Swagger UI: `/docs`
- ReDoc: `/redoc`
- OpenAPI schema: `/openapi.json`
- Postman collection: [postman/STRANTAS_QUANTA_v1.postman_collection.json](/Users/dineshsivakumar/Documents/Projects/strantas-quanta/postman/STRANTAS_QUANTA_v1.postman_collection.json)

## Core API Surfaces

### Intake

- `POST /v1/listener/email`
- `POST /v1/listener/email-multipart`
- `POST /v1/listener/provider-email`
- `POST /v1/listener/smtp-webhook`

### Provider / Connector Operations

- `GET /v1/listener/providers`
- `GET /v1/connectors/state`
- `PUT /v1/inbound-mailboxes/{provider}`
- `GET /v1/inbound-mailboxes`
- `DELETE /v1/inbound-mailboxes/{provider}`
- `POST /v1/inbound-emails/enqueue`
- `POST /v1/connectors/microsoft-graph/subscriptions/refresh`
- `POST /v1/connectors/gmail/watch/refresh`
- `POST /v1/connectors/microsoft-graph/poll`
- `POST /v1/connectors/gmail/poll`
- `GET /v1/connectors/microsoft-graph/webhook`
- `POST /v1/connectors/microsoft-graph/webhook`
- `POST /v1/connectors/gmail/events`

### Queue / Worker / Ops

- `GET /v1/jobs`
- `POST /v1/jobs/run-next`
- `POST /v1/jobs/{job_id}/replay`
- `GET /v1/inbound-email-jobs`
- `GET /v1/inbound-email-jobs/dead-letter`
- `GET /v1/inbound-email-jobs/dashboard`
- `GET /v1/inbound-email-jobs/replay-audit`
- `POST /v1/inbound-email-jobs/{job_id}/replay`
- `POST /v1/inbound-email-jobs/replay`
- `GET /v1/alerts`
- `GET /v1/worker/status`
- `POST /v1/worker/tick`
- `GET /metrics`
- `GET /metrics/prometheus`

### Pipeline

- `POST /v1/reader/parse?submission_id=SUB-...`
- `POST /v1/extractor/run?submission_id=SUB-...`
- `POST /v1/normalizer/run?submission_id=SUB-...`
- `GET /v1/output/{case_id}`
- `GET /v1/output/{case_id}/carrier-rfp`

## Configuration Notes

Important env vars:

- `QUANTA_REPOSITORY_BACKEND`
- `QUANTA_OBJECT_STORE_BACKEND`
- `QUANTA_DATABASE_URL`
- `QUANTA_S3_BUCKET_NAME`
- `QUANTA_S3_ENDPOINT_URL`
- `QUANTA_CONNECTOR_ADMIN_SECRET`
- `QUANTA_SMTP_WEBHOOK_SECRET`
- `QUANTA_PROVIDER_WEBHOOK_SECRET`
- `QUANTA_GRAPH_ACCESS_TOKEN`
- `QUANTA_GRAPH_MAILBOX_USER`
- `QUANTA_GRAPH_CLIENT_STATE`
- `QUANTA_GMAIL_ACCESS_TOKEN`
- `QUANTA_GMAIL_USER_ID`
- `QUANTA_CONNECTOR_WORKER_ENABLED`
- `QUANTA_CONNECTOR_POLL_INTERVAL_SECONDS`
- `QUANTA_CONNECTOR_JOB_BATCH_SIZE`
- `QUANTA_INBOUND_QUEUE_LAG_ALERT_THRESHOLD_SECONDS`
- `QUANTA_ALERT_WEBHOOK_URL`
- `QUANTA_TRACE_SINK_URL`
- `QUANTA_METRICS_OTLP_ENDPOINT`

## V1 Delivery Notes

### Intake / Extraction

- CSV, XLSX, PDF, image, and ZIP inputs are supported
- ZIP archives are expanded safely with provenance preserved on child files
- OCR fallback is used for scanned PDFs when available
- Email body vs attachment precedence is handled with normalization warnings
- Dental and vision extraction is materially richer than the initial scaffold

### Connector / Queue / Worker

- Gmail and Graph support tenant-specific mailbox credentials
- Gmail push events enqueue durable poll jobs
- Gmail poll jobs can use `historyId` incremental fetch
- Graph webhook notifications are validated by client-state and stored subscription id
- Queue supports connector poll, connector ingest, and submission pipeline jobs
- Dead-letter replay and bulk replay are supported
- Replay actions are now persisted as replay audit records
- Queue lag is exposed via dashboard, metrics, and alerts

### Output

- Quote request includes explicit broker agency vs broker contact split
- Employer and broker contact evidence is preserved at field level
- Carrier output includes dental and vision plan detail structures and field results

## Validation Baseline

Current expected verification flow:

```bash
source .venv/bin/activate
python -m compileall apps/api tests scripts
pytest tests/integration -q
```

Current result at this milestone:

- `46 passed in 17.43s`

## Manual Validation Checklist

When resuming manual validation, test:

1. provider-email ingestion with ZIP attachments
2. Gmail connector ingest with mixed attachment bundles
3. Graph webhook notification enqueue behavior
4. Gmail push event enqueue behavior
5. worker process draining queued jobs continuously
6. dead-letter replay and bulk replay
7. replay audit listing
8. queue lag dashboard, metrics, and alerts
9. dental / vision carrier output regression

## Git Milestone Guidance

Recommended milestone checkpoint:

- Commit message: `milestone(v1): production-shaped inbound email intake and worker pipeline`
- Annotated tag: `v1`

Suggested tag annotation:

- `STRANTAS QUANTA V1 milestone: inbound email queue, worker split, connector orchestration, ZIP intake, replay audit, lag monitoring, and carrier-aligned output.`

## Supporting Docs

- Architecture notes: [docs/architecture.md](/Users/dineshsivakumar/Documents/Projects/strantas-quanta/docs/architecture.md)
- Local verification notes: [docs/local-verification.md](/Users/dineshsivakumar/Documents/Projects/strantas-quanta/docs/local-verification.md)
- Example BQM output: [docs/examples/bqm-output-example.json](/Users/dineshsivakumar/Documents/Projects/strantas-quanta/docs/examples/bqm-output-example.json)
