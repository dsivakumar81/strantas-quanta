from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field

from quanta_api.domain.enums import EmailProvider, ProcessingStatus, QuoteType
from quanta_api.domain.models import Attachment, BQMOutput, CensusDataset, LOBRequest, QuoteRequest, ReaderInventory


class EmailAttachmentInput(BaseModel):
    file_name: str
    content_type: str | None = None
    size_bytes: int | None = None
    content_base64: str | None = None


class InboundEmailPayload(BaseModel):
    sender: str
    recipients: list[str] = Field(default_factory=list)
    subject: str
    body_raw: str
    body_text: str
    attachments: list[EmailAttachmentInput] = Field(default_factory=list)


class ProviderEmailPayload(BaseModel):
    provider: EmailProvider
    payload: dict


class SMTPWebhookPayload(BaseModel):
    from_email: str
    to_emails: list[str] = Field(default_factory=list)
    subject: str
    text: str = ""
    html: str = ""
    attachments: list[EmailAttachmentInput] = Field(default_factory=list)


class ListenerResponse(BaseModel):
    submission_id: str
    status: ProcessingStatus
    attachment_count: int


class ParseResponse(ReaderInventory):
    status: ProcessingStatus


class ExtractResponse(BaseModel):
    submission_id: str
    case_id: str
    detected_lobs: list[str]
    status: ProcessingStatus
    warnings: list[str] = Field(default_factory=list)
    evidence_references: list[dict] = Field(default_factory=list)


class NormalizeResponse(BaseModel):
    case_id: str
    status: ProcessingStatus
    quote_request: QuoteRequest
    lob_requests: list[LOBRequest]
    census: CensusDataset


class SubmissionStatusResponse(BaseModel):
    submission_id: str
    status: ProcessingStatus
    subject: str
    sender: str
    received_at: str
    case_id: str | None = None


class CaseResponse(BaseModel):
    quote_request: QuoteRequest
    lob_requests: list[LOBRequest]
    census: CensusDataset | None = None


class BQMMockResponse(BQMOutput):
    generated_at: str


class QuoteSeedInput(BaseModel):
    employer_name: str | None = None
    broker_name: str | None = None
    effective_date: date | None = None
    situs_state: str | None = None
    quote_type: QuoteType = QuoteType.new_business
    requested_lobs: list[str] = Field(default_factory=list)
