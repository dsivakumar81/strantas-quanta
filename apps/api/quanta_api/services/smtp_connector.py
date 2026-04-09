from __future__ import annotations

import json
import re
from dataclasses import dataclass

from quanta_api.core.config import Settings
from quanta_api.domain.contracts import InboundEmailPayload, SMTPWebhookPayload
from quanta_api.domain.enums import EmailProvider
from quanta_api.domain.models import SubmissionEnvelope
from quanta_api.domain.repositories import ObjectStore
from quanta_api.services.email_adapters import EmailAdapterService
from quanta_api.services.intake import IntakeService


class SMTPWebhookAuthError(ValueError):
    pass


@dataclass
class SMTPWebhookResult:
    submission: SubmissionEnvelope
    raw_event_storage_key: str


class SMTPWebhookConnectorService:
    def __init__(
        self,
        settings: Settings,
        object_store: ObjectStore,
        email_adapter: EmailAdapterService,
        intake_service: IntakeService,
    ) -> None:
        self.settings = settings
        self.object_store = object_store
        self.email_adapter = email_adapter
        self.intake_service = intake_service

    def ingest(self, payload: SMTPWebhookPayload, provided_secret: str | None, tenant_id: str = "default") -> SMTPWebhookResult:
        expected_secret = self.settings.smtp_webhook_secret
        if expected_secret and provided_secret != expected_secret:
            raise SMTPWebhookAuthError("Invalid SMTP webhook secret")

        raw_key = self._persist_raw_event(payload, tenant_id=tenant_id)
        normalized_payload = self.email_adapter.parse(
            provider=EmailProvider.smtp_webhook,
            payload=payload.model_dump(),
        )
        submission = self.intake_service.ingest_email(
            normalized_payload,
            tenant_id=tenant_id,
            source_provider=EmailProvider.smtp_webhook,
            raw_event_storage_key=raw_key,
        )
        return SMTPWebhookResult(submission=submission, raw_event_storage_key=raw_key)

    def _persist_raw_event(self, payload: SMTPWebhookPayload, tenant_id: str) -> str:
        safe_subject = re.sub(r"[^A-Za-z0-9._-]+", "_", payload.subject).strip("_") or "smtp_event"
        storage_key = f"tenants/{tenant_id}/raw-events/smtp/{safe_subject}.json"
        self.object_store.put_bytes(storage_key, json.dumps(payload.model_dump()).encode("utf-8"), "application/json")
        return storage_key
