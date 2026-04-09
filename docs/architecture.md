# QUANTA MVP v1 Architecture

## Service shape

- `apps/api`: FastAPI control plane and headless API surface
- `apps/api/quanta_api/domain`: canonical domain objects and request/response contracts
- `apps/api/quanta_api/services`: ID generation, intake, and pipeline orchestration
- `apps/api/quanta_api/storage`: repository adapters and object storage implementations

## Phase 1 boundaries

This scaffold intentionally covers:

- canonical model definitions
- stable API surface
- case and submission identity generation
- repository interfaces with in-memory and Postgres adapters
- local filesystem and S3-compatible object stores
- CSV/XLSX census extraction with evidence references
- PDF table extraction with optional OCR fallback for scanned pages
- explicit Alembic-managed Postgres schema
- Reader pass with intent classification and document tagging
- attachment-backed LOB/plan extraction with field precedence warnings
- provider event normalization for future Microsoft Graph / Gmail / SMTP ingress
- connector subscription refresh and polling orchestration for Graph/Gmail
- durable job queue, dead-letter replay, and alert feed backed by Postgres
- persisted idempotency fingerprints and tenant-aware artifact partitioning
- field-level extraction result objects carried on normalized quote and LOB records
- BQM validation before output persistence
- mock normalization into BQM-compatible JSON

This scaffold intentionally defers:

- async job queue
- OCR and document extraction engines
- production email adapters

## Recommended next implementation slices

1. Extend provider orchestration from API-triggered poll/refresh into scheduled workers.
2. Improve OCR table reconstruction for noisier scanned census PDFs.
3. Add field-level extraction depth for class structure, eligibility, participation minimums, and contribution splits.
4. Expand contradiction resolution and confidence scoring by source/document type.
5. Add richer alert sinks and external observability integrations.
