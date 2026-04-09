from __future__ import annotations

import base64
import hashlib
import io
from pathlib import PurePosixPath
import re
import zipfile

from quanta_api.core.config import Settings
from quanta_api.domain.contracts import EmailAttachmentInput, InboundEmailPayload
from quanta_api.domain.enums import EmailProvider, ProcessingStatus, SourceType
from quanta_api.domain.models import Attachment, EvidenceReference, IdempotencyRecord, SubmissionEnvelope
from quanta_api.domain.repositories import ObjectStore, SubmissionRepository
from quanta_api.errors import ConflictError, InputValidationError
from quanta_api.services.file_sniffer import FileSniffer
from quanta_api.services.id_factory import IdFactory
from quanta_api.services.tracing import TraceSinkService


class IntakeService:
    MAX_ARCHIVE_MEMBERS = 50
    MAX_ARCHIVE_DEPTH = 2

    def __init__(
        self,
        submission_repository: SubmissionRepository,
        object_store: ObjectStore,
        ids: IdFactory,
        settings: Settings,
        file_sniffer: FileSniffer,
        trace_sink: TraceSinkService,
    ) -> None:
        self.submission_repository = submission_repository
        self.object_store = object_store
        self.ids = ids
        self.settings = settings
        self.file_sniffer = file_sniffer
        self.trace_sink = trace_sink

    def ingest_email(
        self,
        payload: InboundEmailPayload,
        tenant_id: str = "default",
        source_provider: EmailProvider | None = None,
        raw_event_storage_key: str | None = None,
        connector_message_id: str | None = None,
    ) -> SubmissionEnvelope:
        self._validate_payload(payload.sender, payload.subject, tenant_id)
        fingerprint = self._fingerprint_for(payload, tenant_id)
        self._reject_duplicate(tenant_id, fingerprint)
        attachments = self._store_attachments(tenant_id, payload.subject, payload.attachments)
        submission_id = self.ids.next_submission_id()
        submission = SubmissionEnvelope(
            submission_id=submission_id,
            tenant_id=tenant_id,
            sender=payload.sender,
            recipients=payload.recipients,
            subject=payload.subject,
            email_body_raw=payload.body_raw,
            email_body_text=payload.body_text,
            attachments=attachments,
            processing_status=ProcessingStatus.received,
            source_provider=source_provider,
            raw_event_storage_key=raw_event_storage_key,
            connector_message_id=connector_message_id,
        )
        created = self.submission_repository.create(submission)
        self.submission_repository.save_fingerprint(
            IdempotencyRecord(tenant_id=tenant_id, fingerprint=fingerprint, submission_id=submission_id)
        )
        self.trace_sink.emit(
            "submission.ingested",
            {
                "submission_id": submission_id,
                "tenant_id": tenant_id,
                "attachment_count": len(attachments),
                "source_provider": source_provider.value if source_provider else None,
            },
        )
        return created

    def ingest_email_with_files(
        self,
        sender: str,
        recipients: list[str],
        subject: str,
        body_raw: str,
        body_text: str,
        files: list[tuple[str, str | None, bytes]],
        tenant_id: str = "default",
    ) -> SubmissionEnvelope:
        self._validate_payload(sender, subject, tenant_id)
        payload = InboundEmailPayload(
            sender=sender,
            recipients=recipients,
            subject=subject,
            body_raw=body_raw,
            body_text=body_text,
            attachments=[
                EmailAttachmentInput(
                    file_name=file_name,
                    content_type=content_type,
                    size_bytes=len(content),
                    content_base64=base64.b64encode(content).decode(),
                )
                for file_name, content_type, content in files
            ],
        )
        fingerprint = self._fingerprint_for(payload, tenant_id)
        self._reject_duplicate(tenant_id, fingerprint)
        attachments = self._store_attachments(tenant_id, subject, payload.attachments)
        submission_id = self.ids.next_submission_id()
        submission = SubmissionEnvelope(
            submission_id=submission_id,
            tenant_id=tenant_id,
            sender=sender,
            recipients=recipients,
            subject=subject,
            email_body_raw=body_raw,
            email_body_text=body_text,
            attachments=attachments,
            processing_status=ProcessingStatus.received,
        )
        created = self.submission_repository.create(submission)
        self.submission_repository.save_fingerprint(
            IdempotencyRecord(tenant_id=tenant_id, fingerprint=fingerprint, submission_id=submission_id)
        )
        self.trace_sink.emit(
            "submission.ingested",
            {"submission_id": submission_id, "tenant_id": tenant_id, "attachment_count": len(attachments)},
        )
        return created

    def _store_attachments(self, tenant_id: str, subject: str, attachment_inputs: list[EmailAttachmentInput]) -> list[Attachment]:
        attachments = []
        safe_subject = re.sub(r"[^A-Za-z0-9._-]+", "_", subject).strip("_") or "submission"
        expanded_inputs = self._expand_attachment_inputs(attachment_inputs)
        for item in expanded_inputs:
            safe_file_name = self._sanitize_file_name(item.file_name)
            content = base64.b64decode(item.content_base64) if item.content_base64 else b""
            detection = self._validate_attachment(item, safe_file_name, content)
            attachment_id = self.ids.next_attachment_id()
            storage_key = f"tenants/{tenant_id}/submissions/{safe_subject}/{attachment_id}-{safe_file_name}"
            if item.content_base64:
                self.object_store.put_bytes(storage_key, content, detection.media_type)
                size_bytes = len(content)
            else:
                size_bytes = item.size_bytes
                storage_key = None
            attachments.append(
                Attachment(
                    attachment_id=attachment_id,
                    file_name=safe_file_name,
                    content_type=item.content_type,
                    detected_content_type=detection.media_type,
                    size_bytes=size_bytes,
                    storage_key=storage_key,
                    archive_file_name=item.archive_file_name,
                    archive_member_path=item.archive_member_path,
                    evidence_references=self._archive_evidence_for(
                        safe_file_name=safe_file_name,
                        archive_file_name=item.archive_file_name,
                        archive_member_path=item.archive_member_path,
                    ),
                )
            )
        return attachments

    def _expand_attachment_inputs(
        self,
        attachment_inputs: list[EmailAttachmentInput],
        *,
        archive_file_name: str | None = None,
        depth: int = 0,
    ) -> list[EmailAttachmentInput]:
        expanded: list[EmailAttachmentInput] = []
        for item in attachment_inputs:
            safe_file_name = self._sanitize_file_name(item.file_name)
            content = base64.b64decode(item.content_base64) if item.content_base64 else b""
            if self._is_zip_attachment(safe_file_name, content):
                if depth >= self.MAX_ARCHIVE_DEPTH:
                    raise InputValidationError(
                        "Archive nesting depth exceeded",
                        details={"fileName": safe_file_name, "maxArchiveDepth": self.MAX_ARCHIVE_DEPTH},
                    )
                expanded.extend(self._expand_archive(archive_file_name=safe_file_name, content=content, depth=depth + 1))
                continue
            expanded.append(
                EmailAttachmentInput(
                    file_name=safe_file_name,
                    content_type=item.content_type,
                    size_bytes=item.size_bytes,
                    content_base64=item.content_base64,
                    archive_file_name=item.archive_file_name or archive_file_name,
                    archive_member_path=item.archive_member_path,
                )
            )
        return expanded

    def _expand_archive(self, archive_file_name: str, content: bytes, depth: int) -> list[EmailAttachmentInput]:
        try:
            archive = zipfile.ZipFile(io.BytesIO(content))
        except zipfile.BadZipFile as exc:
            raise InputValidationError(
                "Attachment content is not supported",
                details={"fileName": archive_file_name, "detectedContentType": "application/octet-stream"},
            ) from exc
        expanded: list[EmailAttachmentInput] = []
        names = archive.infolist()
        if len(names) > self.MAX_ARCHIVE_MEMBERS:
            raise InputValidationError(
                "Archive contains too many files",
                details={"fileName": archive_file_name, "maxArchiveMembers": self.MAX_ARCHIVE_MEMBERS},
            )
        with archive:
            for member in names:
                if member.is_dir():
                    continue
                member_path = PurePosixPath(member.filename)
                if member_path.is_absolute() or ".." in member_path.parts:
                    raise InputValidationError(
                        "Archive contains unsafe file paths",
                        details={"fileName": archive_file_name, "memberPath": member.filename},
                    )
                member_bytes = archive.read(member)
                child_file_name = self._sanitize_file_name(member_path.name)
                if len(member_bytes) > self.settings.max_attachment_size_bytes:
                    raise InputValidationError(
                        "Attachment exceeds maximum allowed size",
                        details={"fileName": child_file_name, "maxAttachmentSizeBytes": self.settings.max_attachment_size_bytes},
                    )
                child_content_type = self.file_sniffer.detect(child_file_name, member_bytes).media_type
                child = EmailAttachmentInput(
                    file_name=child_file_name,
                    content_type=child_content_type,
                    size_bytes=len(member_bytes),
                    content_base64=base64.b64encode(member_bytes).decode(),
                    archive_file_name=archive_file_name,
                    archive_member_path=member.filename,
                )
                expanded.extend(self._expand_attachment_inputs([child], archive_file_name=archive_file_name, depth=depth))
        if not expanded:
            raise InputValidationError(
                "Archive did not contain any supported files",
                details={"fileName": archive_file_name},
            )
        return expanded

    def _is_zip_attachment(self, safe_file_name: str, content: bytes) -> bool:
        if not safe_file_name.lower().endswith(".zip"):
            return False
        return self.file_sniffer.detect(safe_file_name, content).media_type == "application/zip"

    def _archive_evidence_for(
        self,
        *,
        safe_file_name: str,
        archive_file_name: str | None,
        archive_member_path: str | None,
    ) -> list[EvidenceReference]:
        if not archive_file_name or not archive_member_path:
            return []
        return [
            EvidenceReference(
                source_type=SourceType.attachment,
                file_name=safe_file_name,
                snippet=f"Expanded from archive '{archive_file_name}' member '{archive_member_path}'",
                confidence=0.99,
            )
        ]

    def _sanitize_file_name(self, file_name: str) -> str:
        safe_name = PurePosixPath(file_name).name
        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", safe_name).strip("_")
        if not safe_name:
            raise InputValidationError("Attachment filename is invalid")
        return safe_name

    def _validate_attachment(self, item: EmailAttachmentInput, safe_file_name: str, content: bytes):
        extension = f".{safe_file_name.rsplit('.', 1)[-1].lower()}" if "." in safe_file_name else ""
        if extension not in self.settings.allowed_attachment_extensions:
            raise InputValidationError(
                "Unsupported attachment type",
                details={"fileName": safe_file_name, "allowedExtensions": list(self.settings.allowed_attachment_extensions)},
            )
        size_bytes = item.size_bytes
        if item.content_base64 and size_bytes is None:
            size_bytes = len(content)
        if size_bytes and size_bytes > self.settings.max_attachment_size_bytes:
            raise InputValidationError(
                "Attachment exceeds maximum allowed size",
                details={"fileName": safe_file_name, "maxAttachmentSizeBytes": self.settings.max_attachment_size_bytes},
            )
        detection = self.file_sniffer.detect(safe_file_name, content)
        if detection.media_type not in self.settings.supported_media_types:
            raise InputValidationError(
                "Attachment content is not supported",
                details={"fileName": safe_file_name, "detectedContentType": detection.media_type},
            )
        if item.content_type and not self.file_sniffer.media_type_matches(item.content_type, detection.media_type):
            raise InputValidationError(
                "Attachment MIME type does not match file content",
                details={
                    "fileName": safe_file_name,
                    "declaredContentType": item.content_type,
                    "detectedContentType": detection.media_type,
                },
            )
        return detection

    def _validate_payload(self, sender: str, subject: str, tenant_id: str) -> None:
        if "@" not in sender:
            raise InputValidationError("Sender must be a valid email address")
        if not subject.strip():
            raise InputValidationError("Submission subject is required")
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", tenant_id):
            raise InputValidationError("Tenant id must contain only letters, numbers, dot, underscore, and hyphen")

    def _fingerprint_for(self, payload: InboundEmailPayload, tenant_id: str) -> str:
        return hashlib.sha256(
            "|".join(
                [
                    tenant_id,
                    payload.sender.lower().strip(),
                    payload.subject.strip().lower(),
                    payload.body_text.strip().lower(),
                    ",".join(sorted(item.file_name.lower() for item in payload.attachments)),
                ]
            ).encode("utf-8")
        ).hexdigest()

    def _reject_duplicate(self, tenant_id: str, fingerprint: str) -> None:
        existing = self.submission_repository.get_by_fingerprint(tenant_id, fingerprint)
        if existing is not None:
            raise ConflictError(
                "Duplicate submission detected",
                details={"fingerprint": fingerprint[:12], "submissionId": existing.submission_id},
            )
