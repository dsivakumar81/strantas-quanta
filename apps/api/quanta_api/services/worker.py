from __future__ import annotations

import logging
import threading
import time

from quanta_api.core.config import Settings
from quanta_api.domain.enums import EmailProvider, JobStatus
from quanta_api.services.connector_orchestration import ConnectorOrchestrationService
from quanta_api.services.job_queue import JobQueueService

logger = logging.getLogger("quanta.worker")


class ConnectorWorkerService:
    def __init__(
        self,
        settings: Settings,
        connector_orchestration: ConnectorOrchestrationService,
        job_queue: JobQueueService,
    ) -> None:
        self.settings = settings
        self.connector_orchestration = connector_orchestration
        self.job_queue = job_queue
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if not self.settings.connector_worker_enabled or self.is_running():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="quanta-connector-worker")
        self._thread.start()
        logger.info("Connector worker started")

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._thread = None
        logger.info("Connector worker stopped")

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def status(self) -> dict[str, object]:
        return {
            "enabled": self.settings.connector_worker_enabled,
            "running": self.is_running(),
            "pollIntervalSeconds": self.settings.connector_poll_interval_seconds,
            "jobBatchSize": self.settings.connector_job_batch_size,
            "managedTenants": self._managed_tenant_ids(),
        }

    def tick(self) -> dict[str, object]:
        refreshed = []
        graph_results = []
        gmail_results = []
        tenant_results: list[dict[str, object]] = []
        for tenant_id in self._managed_tenant_ids():
            tenant_result: dict[str, object] = {"tenantId": tenant_id, "refreshed": [], "graph": None, "gmail": None}
            try:
                tenant_refreshed = [item.model_dump(mode="json") for item in self.connector_orchestration.ensure_connector_readiness(tenant_id=tenant_id)]
                refreshed.extend(tenant_refreshed)
                tenant_result["refreshed"] = tenant_refreshed
            except Exception as exc:  # noqa: BLE001
                logger.warning("Connector readiness check failed for tenant %s: %s", tenant_id, exc)
            try:
                if self._should_poll_provider(EmailProvider.microsoft_graph, tenant_id):
                    graph_result = self.connector_orchestration.poll_graph_messages(tenant_id=tenant_id)
                    graph_results.append(graph_result)
                    tenant_result["graph"] = graph_result
            except Exception as exc:  # noqa: BLE001
                logger.warning("Graph poll tick failed for tenant %s: %s", tenant_id, exc)
            try:
                if self._should_poll_provider(EmailProvider.gmail, tenant_id):
                    gmail_result = self.connector_orchestration.poll_gmail_messages(tenant_id=tenant_id)
                    gmail_results.append(gmail_result)
                    tenant_result["gmail"] = gmail_result
            except Exception as exc:  # noqa: BLE001
                logger.warning("Gmail poll tick failed for tenant %s: %s", tenant_id, exc)
            tenant_results.append(tenant_result)
        processed = 0
        for _ in range(self.settings.connector_job_batch_size):
            job = self.job_queue.run_next()
            if job is None:
                break
            processed += 1
        dashboards = [self.job_queue.monitor_inbound_email_queue(tenant_id=tenant_id) for tenant_id in self._managed_tenant_ids()]
        overall_dashboard = self.job_queue.monitor_inbound_email_queue(tenant_id=None)
        return {
            "refreshed": refreshed,
            "graph": graph_results[0] if graph_results else None,
            "gmail": gmail_results[0] if gmail_results else None,
            "jobsProcessed": processed,
            "tenants": tenant_results,
            "queueDashboards": dashboards,
            "queueDashboard": overall_dashboard,
        }

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self.tick()
            self._stop_event.wait(self.settings.connector_poll_interval_seconds)

    def _managed_tenant_ids(self) -> list[str]:
        tenant_ids: set[str] = set()
        for mailbox in self.connector_orchestration.operations_repository.list_mailboxes():
            if mailbox.enabled:
                tenant_ids.add(mailbox.tenant_id)
        for cursor in self.connector_orchestration.operations_repository.list_cursors():
            tenant_ids.add(cursor.tenant_id)
        for job in self.job_queue.list_inbound_email_jobs(status=JobStatus.queued.value):
            tenant_ids.add(job.tenant_id)
        if self.settings.graph_access_token or self.settings.gmail_access_token:
            tenant_ids.add("default")
        if not tenant_ids:
            tenant_ids.add("default")
        return sorted(tenant_ids)

    def _should_poll_provider(self, provider: EmailProvider, tenant_id: str) -> bool:
        mailbox = self.connector_orchestration.operations_repository.get_mailbox(provider, tenant_id=tenant_id)
        if mailbox is not None:
            return mailbox.enabled and mailbox.mode == "polling"
        if provider == EmailProvider.microsoft_graph:
            return bool(self.settings.graph_access_token and self.settings.graph_mailbox_user)
        if provider == EmailProvider.gmail:
            return bool(self.settings.gmail_access_token)
        return False
