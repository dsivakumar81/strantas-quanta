from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, Field

from quanta_api.domain.enums import EmailProvider, ProcessingStatus, QuoteType
from quanta_api.domain.models import Attachment, BQMOutput, CarrierGroupRfp, CensusDataset, LOBRequest, QuoteRequest, ReaderInventory
from quanta_api.domain.models import InboundMailboxConfig


class EmailAttachmentInput(BaseModel):
    file_name: str
    content_type: str | None = None
    size_bytes: int | None = None
    content_base64: str | None = None
    archive_file_name: str | None = None
    archive_member_path: str | None = None


class InboundEmailPayload(BaseModel):
    sender: str = Field(examples=["broker@example.com"])
    recipients: list[str] = Field(default_factory=list, examples=[["quotes@strantas.ai"]])
    subject: str = Field(examples=["ACME Manufacturing RFP - Life, LTD, Dental"])
    body_raw: str = Field(examples=["<p>Employer: ACME Manufacturing</p>"])
    body_text: str = Field(examples=["Employer: ACME Manufacturing; Broker: Northstar Benefits; Effective Date: 2026-07-01"])
    attachments: list[EmailAttachmentInput] = Field(default_factory=list)


class ProviderEmailPayload(BaseModel):
    provider: EmailProvider
    payload: dict[str, Any]


class InboundMailboxUpsertRequest(BaseModel):
    provider: EmailProvider
    mailbox_address: str
    provider_user_id: str
    access_token: str
    mode: str = "polling"
    enabled: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


class InboundEmailEnqueueRequest(BaseModel):
    provider: EmailProvider
    message_id: str
    run_pipeline: bool = True
    received_at: str | None = None
    event_id: str | None = None


class InboundMailboxResponse(InboundMailboxConfig):
    pass


class GmailPushMessage(BaseModel):
    data: str
    messageId: str | None = None
    publishTime: str | None = None
    attributes: dict[str, str] = Field(default_factory=dict)


class GmailPushEnvelope(BaseModel):
    message: GmailPushMessage
    subscription: str | None = None


class GraphWebhookNotification(BaseModel):
    subscriptionId: str | None = None
    tenantId: str | None = None
    changeType: str | None = None
    resource: str | None = None
    resourceData: dict[str, Any] = Field(default_factory=dict)
    clientState: str | None = None


class GraphWebhookPayload(BaseModel):
    value: list[GraphWebhookNotification] = Field(default_factory=list)


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


class ConnectorIngestResponse(BaseModel):
    submission_id: str
    case_id: str | None = None
    status: ProcessingStatus
    attachment_count: int
    raw_event_storage_key: str


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
    tenant_id: str
    status: ProcessingStatus
    subject: str
    sender: str
    received_at: str
    case_id: str | None = None
    source_provider: str | None = None
    raw_event_storage_key: str | None = None
    connector_message_id: str | None = None


class CaseResponse(BaseModel):
    quote_request: QuoteRequest
    lob_requests: list[LOBRequest]
    census: CensusDataset | None = None


class BQMMockResponse(BQMOutput):
    generated_at: str


class CarrierRfpResponse(CarrierGroupRfp):
    generated_at: str


class QuoteSeedInput(BaseModel):
    employer_name: str | None = None
    broker_name: str | None = None
    effective_date: date | None = None
    situs_state: str | None = None
    quote_type: QuoteType = QuoteType.new_business
    requested_lobs: list[str] = Field(default_factory=list)


class ErrorResponse(BaseModel):
    error: str
    message: str
    request_id: str | None = None
    details: dict | list | None = None
