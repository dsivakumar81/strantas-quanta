from __future__ import annotations

import base64
import re

from quanta_api.domain.contracts import EmailAttachmentInput, InboundEmailPayload
from quanta_api.domain.enums import ProcessingStatus
from quanta_api.domain.models import Attachment, SubmissionEnvelope
from quanta_api.domain.repositories import ObjectStore, SubmissionRepository
from quanta_api.services.id_factory import IdFactory


class IntakeService:
    def __init__(self, submission_repository: SubmissionRepository, object_store: ObjectStore, ids: IdFactory) -> None:
        self.submission_repository = submission_repository
        self.object_store = object_store
        self.ids = ids

    def ingest_email(self, payload: InboundEmailPayload) -> SubmissionEnvelope:
        attachments = self._store_attachments(payload.subject, payload.attachments)
        submission = SubmissionEnvelope(
            submission_id=self.ids.next_submission_id(),
            sender=payload.sender,
            recipients=payload.recipients,
            subject=payload.subject,
            email_body_raw=payload.body_raw,
            email_body_text=payload.body_text,
            attachments=attachments,
            processing_status=ProcessingStatus.received,
        )
        return self.submission_repository.create(submission)

    def ingest_email_with_files(
        self,
        sender: str,
        recipients: list[str],
        subject: str,
        body_raw: str,
        body_text: str,
        files: list[tuple[str, str | None, bytes]],
    ) -> SubmissionEnvelope:
        attachments = self._store_attachments(
            subject,
            [
                EmailAttachmentInput(
                    file_name=file_name,
                    content_type=content_type,
                    size_bytes=len(content),
                    content_base64=base64.b64encode(content).decode(),
                )
                for file_name, content_type, content in files
            ],
        )
        submission = SubmissionEnvelope(
            submission_id=self.ids.next_submission_id(),
            sender=sender,
            recipients=recipients,
            subject=subject,
            email_body_raw=body_raw,
            email_body_text=body_text,
            attachments=attachments,
            processing_status=ProcessingStatus.received,
        )
        return self.submission_repository.create(submission)

    def _store_attachments(self, subject: str, attachment_inputs: list[EmailAttachmentInput]) -> list[Attachment]:
        attachments = []
        safe_subject = re.sub(r"[^A-Za-z0-9._-]+", "_", subject).strip("_") or "submission"
        for item in attachment_inputs:
            attachment_id = self.ids.next_attachment_id()
            storage_key = f"submissions/{safe_subject}/{attachment_id}-{item.file_name}"
            if item.content_base64:
                content = base64.b64decode(item.content_base64)
                self.object_store.put_bytes(storage_key, content, item.content_type)
                size_bytes = len(content)
            else:
                size_bytes = item.size_bytes
                storage_key = None
            attachments.append(
                Attachment(
                    attachment_id=attachment_id,
                    file_name=item.file_name,
                    content_type=item.content_type,
                    size_bytes=size_bytes,
                    storage_key=storage_key,
                )
            )
        return attachments
