from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from quanta_api.core.config import Settings
from quanta_api.domain.enums import AlertSeverity, JobStatus, JobType
from quanta_api.domain.models import JobRecord, ReplayAuditRecord
from quanta_api.domain.repositories import OperationsRepository
from quanta_api.services.alerts import AlertService
from quanta_api.services.id_factory import IdFactory
from quanta_api.services.job_runner import SubmissionJobRunner
from quanta_api.services.metrics import MetricsService
from quanta_api.services.connector_execution import GmailConnectorExecutionService, GraphConnectorExecutionService

if TYPE_CHECKING:
    from quanta_api.services.connector_orchestration import ConnectorOrchestrationService


class JobQueueService:
    def __init__(
        self,
        operations_repository: OperationsRepository,
        ids: IdFactory,
        graph_connector: GraphConnectorExecutionService,
        gmail_connector: GmailConnectorExecutionService,
        submission_job_runner: SubmissionJobRunner,
        metrics: MetricsService,
        alert_service: AlertService,
        settings: Settings,
    ) -> None:
        self.operations_repository = operations_repository
        self.ids = ids
        self.graph_connector = graph_connector
        self.gmail_connector = gmail_connector
        self.submission_job_runner = submission_job_runner
        self.metrics = metrics
        self.alert_service = alert_service
        self.settings = settings
        self.connector_orchestration: ConnectorOrchestrationService | None = None

    def bind_connector_orchestration(self, connector_orchestration: ConnectorOrchestrationService) -> None:
        self.connector_orchestration = connector_orchestration

    def enqueue(self, job_type: JobType, payload: dict, max_attempts: int = 3, tenant_id: str = "default") -> JobRecord:
        dedupe_key = payload.get("dedupe_key")
        if dedupe_key:
            existing = self.operations_repository.get_job_by_dedupe_key(dedupe_key, tenant_id=tenant_id)
            if existing is not None and existing.status != JobStatus.dead_letter:
                return existing
        job = JobRecord(job_id=self.ids.next_job_id(), tenant_id=tenant_id, job_type=job_type, payload=payload, max_attempts=max_attempts, dedupe_key=dedupe_key)
        return self.operations_repository.enqueue_job(job)

    def enqueue_connector_ingest(
        self,
        *,
        provider,
        message_id: str,
        tenant_id: str = "default",
        run_pipeline: bool = True,
        event_id: str | None = None,
    ) -> JobRecord:
        provider_value = provider.value if hasattr(provider, "value") else str(provider)
        dedupe_key = f"connector_ingest:{tenant_id}:{provider_value}:{message_id}"
        payload = {
            "provider": provider_value,
            "message_id": message_id,
            "run_pipeline": run_pipeline,
            "tenant_id": tenant_id,
            "event_id": event_id,
            "dedupe_key": dedupe_key,
        }
        return self.enqueue(JobType.connector_ingest, payload=payload, tenant_id=tenant_id)

    def enqueue_connector_poll(
        self,
        *,
        provider,
        tenant_id: str = "default",
        event_id: str | None = None,
        history_id: str | None = None,
        source: str | None = None,
    ) -> JobRecord:
        provider_value = provider.value if hasattr(provider, "value") else str(provider)
        dedupe_source = history_id or event_id or self.ids.next_job_id()
        dedupe_key = f"connector_poll:{tenant_id}:{provider_value}:{dedupe_source}"
        payload = {
            "provider": provider_value,
            "tenant_id": tenant_id,
            "event_id": event_id,
            "history_id": history_id,
            "source": source,
            "dedupe_key": dedupe_key,
        }
        return self.enqueue(JobType.connector_poll, payload=payload, tenant_id=tenant_id)

    def list_jobs(self, status: str | None = None, tenant_id: str | None = None) -> list[JobRecord]:
        return self.operations_repository.list_jobs(status=status, tenant_id=tenant_id)

    def run_next(self, tenant_id: str | None = None) -> JobRecord | None:
        job = self.operations_repository.next_available_job(tenant_id=tenant_id)
        if job is None:
            return None
        job.status = JobStatus.running
        job.started_at = datetime.now(timezone.utc)
        job.attempts += 1
        self.operations_repository.update_job(job)
        try:
            self._execute(job)
            job.status = JobStatus.succeeded
            job.completed_at = datetime.now(timezone.utc)
            job.error_message = None
            job.dead_letter_reason = None
            self.metrics.increment("job.queue.succeeded")
        except Exception as exc:  # noqa: BLE001
            job.error_message = str(exc)
            if job.attempts >= job.max_attempts:
                job.status = JobStatus.dead_letter
                job.dead_letter_reason = str(exc)
                self.metrics.increment("job.queue.dead_letter")
                self.alert_service.emit(
                    AlertSeverity.error,
                    source="job_queue",
                    message=f"Job {job.job_id} moved to dead letter",
                    tenant_id=job.tenant_id,
                    context={"jobType": job.job_type.value, "error": str(exc)},
                )
            else:
                job.status = JobStatus.queued
                job.available_at = datetime.now(timezone.utc) + timedelta(seconds=job.attempts * 5)
                self.metrics.increment("job.queue.retry_scheduled")
        return self.operations_repository.update_job(job)

    def replay(self, job_id: str) -> JobRecord:
        job = self.operations_repository.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        previous_status = job.status.value
        job.status = JobStatus.queued
        job.attempts = 0
        job.available_at = datetime.now(timezone.utc)
        job.started_at = None
        job.completed_at = None
        job.dead_letter_reason = None
        job.error_message = None
        updated = self.operations_repository.update_job(job)
        self.operations_repository.save_replay_audit(
            ReplayAuditRecord(
                audit_id=self.ids.next_alert_id(),
                tenant_id=job.tenant_id,
                job_id=job.job_id,
                job_type=job.job_type,
                provider=str(job.payload.get("provider")) if job.payload.get("provider") else None,
                previous_status=previous_status,
                replay_scope="single",
                metadata={"dedupeKey": job.dedupe_key},
            )
        )
        self.alert_service.emit(
            AlertSeverity.info,
            source="job_queue.replay",
            message=f"Job {job.job_id} replayed",
            tenant_id=job.tenant_id,
            context={"jobType": job.job_type.value, "previousStatus": previous_status},
        )
        return updated

    def list_inbound_email_jobs(self, status: str | None = None, tenant_id: str | None = None) -> list[JobRecord]:
        return [
            job
            for job in self.list_jobs(status=status, tenant_id=tenant_id)
            if job.job_type in {JobType.connector_ingest, JobType.connector_poll}
        ]

    def filter_inbound_email_jobs(
        self,
        *,
        status: str | None = None,
        tenant_id: str | None = None,
        provider: str | None = None,
    ) -> list[JobRecord]:
        jobs = self.list_inbound_email_jobs(status=status, tenant_id=tenant_id)
        if provider is not None:
            jobs = [job for job in jobs if str(job.payload.get("provider")) == provider]
        return jobs

    def inbound_email_dashboard(self, tenant_id: str | None = None, lag_threshold_seconds: int | None = None) -> dict[str, object]:
        jobs = self.list_inbound_email_jobs(tenant_id=tenant_id)
        by_status: dict[str, int] = {}
        by_provider: dict[str, int] = {}
        per_tenant: dict[str, dict[str, object]] = {}
        oldest_queued_at = None
        last_processed: dict[str, object] | None = None
        now = datetime.now(timezone.utc)
        for job in jobs:
            by_status[job.status.value] = by_status.get(job.status.value, 0) + 1
            provider = str(job.payload.get("provider", "unknown"))
            by_provider[provider] = by_provider.get(provider, 0) + 1
            tenant_bucket = per_tenant.setdefault(
                job.tenant_id,
                {
                    "total": 0,
                    "byStatus": {},
                    "byProvider": {},
                    "lastProcessedMessage": None,
                    "oldestQueuedAt": None,
                    "oldestQueuedAgeSeconds": None,
                    "lagThresholdBreached": False,
                },
            )
            tenant_bucket["total"] = int(tenant_bucket["total"]) + 1
            tenant_status = tenant_bucket["byStatus"]
            tenant_status[job.status.value] = tenant_status.get(job.status.value, 0) + 1
            tenant_provider = tenant_bucket["byProvider"]
            tenant_provider[provider] = tenant_provider.get(provider, 0) + 1
            if job.status == JobStatus.queued and (oldest_queued_at is None or job.available_at < oldest_queued_at):
                oldest_queued_at = job.available_at
            if job.status == JobStatus.queued:
                current_oldest = tenant_bucket["oldestQueuedAt"]
                if current_oldest is None or job.available_at < current_oldest:
                    tenant_bucket["oldestQueuedAt"] = job.available_at
            if job.status == JobStatus.succeeded and job.completed_at is not None:
                candidate = {
                    "tenantId": job.tenant_id,
                    "provider": provider,
                    "messageId": job.payload.get("message_id"),
                    "completedAt": job.completed_at,
                }
                current_tenant_last = tenant_bucket["lastProcessedMessage"]
                if current_tenant_last is None or candidate["completedAt"] > current_tenant_last["completedAt"]:
                    tenant_bucket["lastProcessedMessage"] = candidate
                if last_processed is None or candidate["completedAt"] > last_processed["completedAt"]:
                    last_processed = candidate
        replay_audit_trail = [audit.model_dump(mode="json") for audit in self.operations_repository.list_replay_audits(limit=25, tenant_id=tenant_id)]
        oldest_queued_age_seconds = None
        if oldest_queued_at is not None:
            oldest_queued_age_seconds = max(0, int((now - oldest_queued_at).total_seconds()))
        for tenant_bucket in per_tenant.values():
            tenant_oldest = tenant_bucket["oldestQueuedAt"]
            if tenant_oldest is not None:
                age = max(0, int((now - tenant_oldest).total_seconds()))
                tenant_bucket["oldestQueuedAgeSeconds"] = age
                tenant_bucket["lagThresholdBreached"] = lag_threshold_seconds is not None and age >= lag_threshold_seconds
        return {
            "tenantId": tenant_id,
            "total": len(jobs),
            "byStatus": by_status,
            "byProvider": by_provider,
            "deadLetterCount": by_status.get(JobStatus.dead_letter.value, 0),
            "oldestQueuedAt": oldest_queued_at,
            "oldestQueuedAgeSeconds": oldest_queued_age_seconds,
            "lagThresholdSeconds": lag_threshold_seconds,
            "lagThresholdBreached": lag_threshold_seconds is not None and oldest_queued_age_seconds is not None and oldest_queued_age_seconds >= lag_threshold_seconds,
            "lastProcessedMessage": last_processed,
            "perTenant": per_tenant,
            "replayAuditTrail": replay_audit_trail,
        }

    def replay_inbound_email_job(self, job_id: str) -> JobRecord:
        job = self.operations_repository.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        if job.job_type not in {JobType.connector_ingest, JobType.connector_poll}:
            raise ValueError("Job is not an inbound email job")
        if job.status != JobStatus.dead_letter:
            raise ValueError("Only dead-letter inbound email jobs can be replayed from this endpoint")
        return self.replay(job_id)

    def replay_inbound_email_jobs(
        self,
        *,
        tenant_id: str | None = None,
        provider: str | None = None,
        status: str = JobStatus.dead_letter.value,
    ) -> list[JobRecord]:
        jobs = self.filter_inbound_email_jobs(status=status, tenant_id=tenant_id, provider=provider)
        replayed = [self.replay(job.job_id) for job in jobs]
        if replayed:
            scope_tenant = tenant_id or "all"
            self.alert_service.emit(
                AlertSeverity.info,
                source="job_queue.bulk_replay",
                message=f"Bulk replayed {len(replayed)} inbound email jobs",
                tenant_id=tenant_id or "default",
                context={"provider": provider, "status": status, "tenantScope": scope_tenant},
            )
        return replayed

    def monitor_inbound_email_queue(self, tenant_id: str | None = None) -> dict[str, object]:
        dashboard = self.inbound_email_dashboard(tenant_id=tenant_id, lag_threshold_seconds=self.settings.inbound_queue_lag_alert_threshold_seconds)
        suffix = tenant_id if tenant_id is not None else "all"
        self.metrics.set_gauge(f"inbound.email.queue.total.{suffix}", float(dashboard["total"]))
        self.metrics.set_gauge(f"inbound.email.queue.dead_letter.{suffix}", float(dashboard["deadLetterCount"]))
        self.metrics.set_gauge(
            f"inbound.email.queue.oldest_age_seconds.{suffix}",
            float(dashboard["oldestQueuedAgeSeconds"] or 0),
        )
        if dashboard["lagThresholdBreached"]:
            self.alert_service.emit(
                AlertSeverity.warning,
                source="job_queue.lag",
                message="Inbound email queue lag threshold breached",
                tenant_id=tenant_id or "default",
                context={
                    "oldestQueuedAgeSeconds": dashboard["oldestQueuedAgeSeconds"],
                    "lagThresholdSeconds": dashboard["lagThresholdSeconds"],
                    "total": dashboard["total"],
                },
            )
        return dashboard

    def _execute(self, job: JobRecord) -> None:
        if job.job_type == JobType.connector_poll:
            provider = job.payload["provider"]
            tenant_id = job.payload.get("tenant_id", job.tenant_id)
            history_id = job.payload.get("history_id")
            if self.connector_orchestration is None:
                raise ValueError("Connector orchestration is not bound")
            if provider == "microsoft_graph":
                self.connector_orchestration.poll_graph_messages(tenant_id=tenant_id)
                return
            if provider == "gmail":
                self.connector_orchestration.poll_gmail_messages(tenant_id=tenant_id, history_id=history_id)
                return
            raise ValueError(f"Unsupported provider '{provider}'")
        if job.job_type == JobType.connector_ingest:
            provider = job.payload["provider"]
            message_id = job.payload["message_id"]
            run_pipeline = bool(job.payload.get("run_pipeline", True))
            tenant_id = job.payload.get("tenant_id", job.tenant_id)
            if provider == "microsoft_graph":
                result = self.graph_connector.ingest_message(message_id, tenant_id=tenant_id)
            elif provider == "gmail":
                result = self.gmail_connector.ingest_message(message_id, tenant_id=tenant_id)
            else:
                raise ValueError(f"Unsupported provider '{provider}'")
            if run_pipeline:
                self.submission_job_runner.run_submission(result.submission.submission_id, tenant_id=tenant_id)
            return
        if job.job_type == JobType.submission_pipeline:
            self.submission_job_runner.run_submission(job.payload["submission_id"], tenant_id=job.payload.get("tenant_id", job.tenant_id))
            return
        raise ValueError(f"Unsupported job type '{job.job_type.value}'")
