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
- connector scaffolding metadata for provider rollout planning
- BQM validation before output persistence
- mock normalization into BQM-compatible JSON

This scaffold intentionally defers:

- async job queue
- OCR and document extraction engines
- production email adapters

## Recommended next implementation slices

1. Extend object storage to persist raw email bodies and parsed artifacts, not only attachment binaries.
2. Improve OCR table reconstruction for noisier scanned census PDFs.
3. Move parse/extract/normalize steps to async jobs while keeping current endpoints as orchestration triggers.
4. Add evidence precedence rules and contradiction resolution into normalized output.
5. Add CI automation that boots the Docker dev stack and runs the integration suite.
