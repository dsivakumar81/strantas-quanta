from __future__ import annotations

import json
import time
from dataclasses import dataclass

import httpx

from quanta_api.core.config import Settings
from quanta_api.domain.enums import EmailProvider
from quanta_api.domain.models import InboundMailboxConfig, SubmissionEnvelope
from quanta_api.domain.repositories import ObjectStore, OperationsRepository
from quanta_api.services.email_adapters import EmailAdapterService
from quanta_api.services.intake import IntakeService
from quanta_api.services.metrics import MetricsService
from quanta_api.services.retry import RetryService


@dataclass
class ConnectorIngestResult:
    submission: SubmissionEnvelope
    raw_event_storage_key: str


class GraphConnectorExecutionService:
    def __init__(
        self,
        settings: Settings,
        object_store: ObjectStore,
        email_adapter: EmailAdapterService,
        intake_service: IntakeService,
        operations_repository: OperationsRepository,
        retry_service: RetryService,
        metrics: MetricsService,
        client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings
        self.object_store = object_store
        self.email_adapter = email_adapter
        self.intake_service = intake_service
        self.operations_repository = operations_repository
        self.retry_service = retry_service
        self.metrics = metrics
        self.client = client or httpx.Client(timeout=20.0)

    def ingest_message(self, message_id: str, tenant_id: str = "default") -> ConnectorIngestResult:
        started = time.perf_counter()
        mailbox = self.operations_repository.get_mailbox(EmailProvider.microsoft_graph, tenant_id=tenant_id)
        if mailbox is not None and not mailbox.enabled:
            mailbox = None
        try:
            payload = self.retry_service.run(
                lambda: self._fetch_message(message_id, mailbox),
                on_retry=lambda attempt, _exc: self.metrics.increment("connector.graph.retry"),
            )
            raw_key = self._persist_raw_event(message_id, payload, tenant_id=tenant_id)
            normalized = self.email_adapter.parse(EmailProvider.microsoft_graph, payload)
            submission = self.intake_service.ingest_email(
                normalized,
                tenant_id=tenant_id,
                source_provider=EmailProvider.microsoft_graph,
                raw_event_storage_key=raw_key,
                connector_message_id=message_id,
            )
            self.metrics.increment("connector.graph.success")
            return ConnectorIngestResult(submission=submission, raw_event_storage_key=raw_key)
        except Exception:
            self.metrics.increment("connector.graph.failure")
            raise
        finally:
            self.metrics.record_timing("connector.graph.ingest_ms", (time.perf_counter() - started) * 1000)

    def _fetch_message(self, message_id: str, mailbox: InboundMailboxConfig | None) -> dict:
        access_token = mailbox.access_token if mailbox else self.settings.graph_access_token
        mailbox_user = mailbox.provider_user_id if mailbox else self.settings.graph_mailbox_user
        if not access_token or not mailbox_user:
            raise ValueError("Microsoft Graph connector is not configured")
        response = self.client.get(
            f"{self.settings.graph_base_url}/users/{mailbox_user}/messages/{message_id}",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        response.raise_for_status()
        payload = response.json()
        attachments_response = self.client.get(
            f"{self.settings.graph_base_url}/users/{mailbox_user}/messages/{message_id}/attachments",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        attachments_response.raise_for_status()
        payload["attachments"] = attachments_response.json().get("value", [])
        return payload

    def _persist_raw_event(self, message_id: str, payload: dict, tenant_id: str) -> str:
        storage_key = f"tenants/{tenant_id}/raw-events/graph/{message_id}.json"
        self.object_store.put_bytes(storage_key, json.dumps(payload).encode("utf-8"), "application/json")
        return storage_key


class GmailConnectorExecutionService:
    def __init__(
        self,
        settings: Settings,
        object_store: ObjectStore,
        email_adapter: EmailAdapterService,
        intake_service: IntakeService,
        operations_repository: OperationsRepository,
        retry_service: RetryService,
        metrics: MetricsService,
        client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings
        self.object_store = object_store
        self.email_adapter = email_adapter
        self.intake_service = intake_service
        self.operations_repository = operations_repository
        self.retry_service = retry_service
        self.metrics = metrics
        self.client = client or httpx.Client(timeout=20.0)

    def ingest_message(self, message_id: str, tenant_id: str = "default") -> ConnectorIngestResult:
        started = time.perf_counter()
        mailbox = self.operations_repository.get_mailbox(EmailProvider.gmail, tenant_id=tenant_id)
        if mailbox is not None and not mailbox.enabled:
            mailbox = None
        try:
            payload = self.retry_service.run(
                lambda: self._fetch_message(message_id, mailbox),
                on_retry=lambda attempt, _exc: self.metrics.increment("connector.gmail.retry"),
            )
            raw_key = self._persist_raw_event(message_id, payload, tenant_id=tenant_id)
            normalized = self.email_adapter.parse(EmailProvider.gmail, payload)
            submission = self.intake_service.ingest_email(
                normalized,
                tenant_id=tenant_id,
                source_provider=EmailProvider.gmail,
                raw_event_storage_key=raw_key,
                connector_message_id=message_id,
            )
            self.metrics.increment("connector.gmail.success")
            return ConnectorIngestResult(submission=submission, raw_event_storage_key=raw_key)
        except Exception:
            self.metrics.increment("connector.gmail.failure")
            raise
        finally:
            self.metrics.record_timing("connector.gmail.ingest_ms", (time.perf_counter() - started) * 1000)

    def _fetch_message(self, message_id: str, mailbox: InboundMailboxConfig | None) -> dict:
        access_token = mailbox.access_token if mailbox else self.settings.gmail_access_token
        user_id = mailbox.provider_user_id if mailbox else self.settings.gmail_user_id
        if not access_token:
            raise ValueError("Gmail connector is not configured")
        response = self.client.get(
            f"{self.settings.gmail_base_url}/users/{user_id}/messages/{message_id}",
            params={"format": "full"},
            headers={"Authorization": f"Bearer {access_token}"},
        )
        response.raise_for_status()
        message = response.json()
        self._hydrate_attachment_parts(message_id, message.get("payload", {}), access_token=access_token, user_id=user_id)
        return message

    def _persist_raw_event(self, message_id: str, payload: dict, tenant_id: str) -> str:
        storage_key = f"tenants/{tenant_id}/raw-events/gmail/{message_id}.json"
        self.object_store.put_bytes(storage_key, json.dumps(payload).encode("utf-8"), "application/json")
        return storage_key

    def _hydrate_attachment_parts(self, message_id: str, payload: dict, *, access_token: str, user_id: str) -> None:
        queue = list(payload.get("parts", []))
        while queue:
            part = queue.pop(0)
            queue[0:0] = part.get("parts", [])
            filename = part.get("filename")
            if not filename:
                continue
            body = part.setdefault("body", {})
            if body.get("data") or not body.get("attachmentId"):
                continue
            attachment_response = self.client.get(
                f"{self.settings.gmail_base_url}/users/{user_id}/messages/{message_id}/attachments/{body['attachmentId']}",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            attachment_response.raise_for_status()
            attachment_payload = attachment_response.json()
            body["data"] = attachment_payload.get("data", "")
