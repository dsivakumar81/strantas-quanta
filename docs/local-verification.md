# Local Verification

## Start

```bash
bash scripts/bootstrap_dev.sh
set -a; source .env; set +a
source .venv/bin/activate
uvicorn quanta_api.main:app --app-dir apps/api --host 127.0.0.1 --port 8000
```

Readiness:

```bash
curl http://127.0.0.1:8000/readiness
```

OpenAPI:

```bash
curl http://127.0.0.1:8000/openapi.json
```

## Sample flow

Use `samples/acme_census.csv` encoded as base64 in the listener request, then call parse, extract, normalize, and output.

Use `samples/acme_census.pdf` with `POST /v1/listener/email-multipart` to exercise the multipart and PDF extraction path.
Use `samples/acme_census.xlsx` with `POST /v1/listener/email` to exercise the XLSX extraction path.
Use `samples/acme_census_scanned.pdf` to exercise the OCR fallback path when `tesseract` is installed.

This exercises:

- local object storage persistence
- submission and case creation
- heuristic LOB detection
- CSV census extraction
- evidence references in normalized output

Verified locally on March 12, 2026 with:

- `SUB-2026-000001`
- `QNT-2026-000001`
- detected LOBs: `group_life`, `group_ltd`, `dental`
- census summary: 4 employees, 6 dependents, average age `39.2`, median salary `80000.0`

Verified again on March 12, 2026 against the real adapters:

- repository backend: Postgres on `127.0.0.1:5432`
- object store backend: MinIO on `127.0.0.1:9100`
- intake path: `POST /v1/listener/email-multipart`
- attachment: `samples/acme_census.pdf`
- persisted rows:
  - `submissions`: `1`
  - `quotes`: `1`
  - `lobs`: `3`
  - `census`: `1`
  - `outputs`: `1`
- stored object:
  - `submissions/ACME_Manufacturing_Life_LTD_Dental_submission_PDF/ATT-2026-000001-acme_census.pdf`

Integration tests verified on March 12, 2026:

- command: `pytest tests/integration -q`
- result: `6 passed` locally after installing `tesseract`
- coverage in suite:
  - provider connector scaffold endpoint
  - reader intent classification and document tagging
  - provider email adapter normalization route
  - multipart PDF intake against Postgres/MinIO
  - JSON/base64 CSV intake against Postgres/MinIO
  - JSON/base64 XLSX intake against Postgres/MinIO
  - Alembic downgrade/upgrade round-trip
  - scanned-PDF OCR path with local `tesseract`
  - email-over-attachment precedence conflict handling
