# STRANTAS QUANTA MVP Scaffold

Phase 1 scaffold for QUANTA, a headless underwriting intake and quote-preparation service for group insurance submissions.

## Included

- FastAPI application scaffold
- Canonical domain models for submissions, quote requests, LOB requests, census, evidence, and BQM output
- Repository interfaces with in-memory and Postgres-backed adapters
- Local filesystem and S3-compatible object storage adapters
- Case, submission, attachment, and census ID generation
- Listener, reader, extractor, normalizer, output, submission status, and case summary endpoints
- JSON and multipart email intake paths
- provider-adapter email intake abstraction for Microsoft Graph, Gmail, SMTP webhook, and manual payloads
- provider connector scaffold endpoint for integration planning
- CSV/XLSX/PDF census extraction with evidence references
- scanned-PDF OCR fallback when `tesseract` is available
- Reader pass for submission intent classification, document inventory, and attachment tagging

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
uvicorn quanta_api.main:app --reload
```

Or with the package layout explicitly:

```bash
source .venv/bin/activate
uvicorn quanta_api.main:app --app-dir apps/api --host 127.0.0.1 --port 8000
```

## Storage backends

- Default repository backend: in-memory
- Default object store backend: local filesystem under `.data/object_store`
- Optional Postgres adapter is available through `settings.repository_backend = "postgres"`
- Optional S3-compatible object storage adapter is available through `settings.object_store_backend = "s3"`

## Local dev stack

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
- generated sample PDF/XLSX files
- generated scanned-PDF OCR fixture

## Migrations

```bash
source .venv/bin/activate
set -a; source .env; set +a
alembic upgrade head
```

For a clean local reset:

```bash
source .venv/bin/activate
python scripts/reset_dev_db.py
alembic upgrade head
```

## First API flow

1. `POST /v1/listener/email`
2. `POST /v1/listener/email-multipart` as an alternative to inline base64 attachments
3. `POST /v1/listener/provider-email` for normalized provider events
4. `GET /v1/listener/providers` for connector scaffolding metadata
5. `POST /v1/reader/parse?submission_id=SUB-...`
6. `POST /v1/extractor/run?submission_id=SUB-...`
7. `POST /v1/normalizer/run?submission_id=SUB-...`
8. `GET /v1/output/{case_id}`

`POST /v1/reader/parse` now returns:

- `submission_intent`
- classified `document_inventory`
- attachment `document_type` tags
- reader warnings and confidence

## Notes

- The default repository backend is still in-memory, so local state resets on restart unless Postgres is enabled.
- Attachment content can now be sent inline as base64 for CSV/XLSX extraction.
- Attachment content can also be uploaded through multipart form data.
- LOB and plan extraction are still heuristic; census extraction is now implemented for CSV/XLSX/PDF.
- Scanned PDF fallback uses OCR when `tesseract` is installed locally; otherwise QUANTA returns a warning instead of failing.
- Attachment-backed extraction now contributes LOB evidence and first-pass plan designs for life, disability, dental, vision, and supplemental products when matching terms are present in files.
- Field-level precedence rules currently prefer email-body values over conflicting attachment values and emit explicit warnings.
- BQM output is now validated before persistence/response.
- A verified sample census file is included at `samples/acme_census.csv`.
- A generated PDF sample census is included at `samples/acme_census.pdf`.
- A generated XLSX sample census is included at `samples/acme_census.xlsx`.
- A generated scanned PDF fixture is included at `samples/acme_census_scanned.pdf`.

## CI

GitHub Actions integration coverage lives in [.github/workflows/integration.yml](/Users/dineshsivakumar/Documents/Projects/strantas-quanta/.github/workflows/integration.yml) and runs:

- `scripts/bootstrap_dev.sh`
- `pytest tests/integration -q`

The CI runner installs `tesseract-ocr`, so the scanned-PDF OCR test is exercised there even if it is skipped locally.
