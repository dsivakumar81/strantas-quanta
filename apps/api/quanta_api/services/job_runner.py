from __future__ import annotations

import time
from dataclasses import dataclass

from quanta_api.domain.models import CensusDataset, LOBRequest, QuoteRequest
from quanta_api.services.metrics import MetricsService
from quanta_api.services.pipeline import PipelineService
from quanta_api.services.reader import ReaderService
from quanta_api.services.retry import RetryService


@dataclass
class SubmissionJobResult:
    quote: QuoteRequest
    lobs: list[LOBRequest]
    census: CensusDataset


class SubmissionJobRunner:
    def __init__(
        self,
        reader_service: ReaderService,
        pipeline_service: PipelineService,
        retry_service: RetryService,
        metrics: MetricsService,
    ) -> None:
        self.reader_service = reader_service
        self.pipeline_service = pipeline_service
        self.retry_service = retry_service
        self.metrics = metrics

    def run_submission(self, submission_id: str, tenant_id: str = "default") -> SubmissionJobResult:
        started = time.perf_counter()

        def operation() -> SubmissionJobResult:
            self.reader_service.parse_submission(submission_id, tenant_id=tenant_id)
            quote, lobs, census = self.pipeline_service.run_normalization(submission_id, tenant_id=tenant_id)
            return SubmissionJobResult(quote=quote, lobs=lobs, census=census)

        try:
            result = self.retry_service.run(
                operation,
                on_retry=lambda attempt, _exc: self.metrics.increment("job.submission.retry"),
            )
            self.metrics.increment("job.submission.success")
            return result
        except Exception:
            self.metrics.increment("job.submission.failure")
            raise
        finally:
            self.metrics.record_timing("job.submission.runtime_ms", (time.perf_counter() - started) * 1000)
