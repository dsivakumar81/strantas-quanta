from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from quanta_api.domain.enums import (
    AttachmentType,
    DocumentType,
    ModuleStatus,
    ProcessingStatus,
    QuoteType,
    SourceChannel,
    SourceType,
    SubmissionIntent,
)


class Attachment(BaseModel):
    attachment_id: str
    file_name: str
    content_type: str | None = None
    size_bytes: int | None = None
    storage_key: str | None = None
    attachment_type: AttachmentType = AttachmentType.email_attachment
    document_type: DocumentType = DocumentType.unknown
    tags: list[str] = Field(default_factory=list)
    evidence_references: list["EvidenceReference"] = Field(default_factory=list)


class EvidenceReference(BaseModel):
    source_type: SourceType
    file_name: str | None = None
    page_number: int | None = None
    sheet_name: str | None = None
    cell_range: str | None = None
    snippet: str | None = None
    confidence: float = Field(ge=0, le=1)


class ProcessingOutcome(BaseModel):
    status: ModuleStatus
    confidence: float = Field(ge=0, le=1)
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    evidence_references: list[EvidenceReference] = Field(default_factory=list)


class SubmissionEnvelope(BaseModel):
    submission_id: str
    source_channel: SourceChannel = SourceChannel.email
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    sender: str
    recipients: list[str] = Field(default_factory=list)
    subject: str
    email_body_raw: str
    email_body_text: str
    attachments: list[Attachment] = Field(default_factory=list)
    processing_status: ProcessingStatus = ProcessingStatus.received
    submission_intent: SubmissionIntent | None = None
    document_warnings: list[str] = Field(default_factory=list)


class ReaderInventory(BaseModel):
    submission_id: str
    submission_intent: SubmissionIntent
    document_inventory: list[Attachment] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0, le=1)


class Employer(BaseModel):
    name: str | None = None
    effective_date: date | None = None
    situs_state: str | None = None


class Broker(BaseModel):
    name: str | None = None


class BQMLobOutput(BaseModel):
    lobCaseId: str
    lobType: str
    planDesigns: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    evidenceReferences: list[dict[str, Any]] = Field(default_factory=list)


class BQMCensusOutput(BaseModel):
    employeeCount: int = 0
    dependentCount: int = 0
    classesDetected: list[str] = Field(default_factory=list)
    statesDetected: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    summary: dict[str, Any] = Field(default_factory=dict)
    evidenceReferences: list[dict[str, Any]] = Field(default_factory=list)


class PlanDesign(BaseModel):
    plan_type: str | None = None
    benefit_basis: str | None = None
    benefit_percent: float | None = None
    elimination_period_days: int | None = None
    max_benefit: float | None = None
    max_monthly_benefit: float | None = None
    max_weekly_benefit: float | None = None
    guarantee_issue: float | None = None
    contribution_details: str | None = None
    notes: list[str] = Field(default_factory=list)


class LOBRequest(BaseModel):
    lob_case_id: str
    parent_case_id: str
    lob_type: str
    requested_plan_designs: list[PlanDesign] = Field(default_factory=list)
    benefit_details: dict[str, Any] = Field(default_factory=dict)
    class_structure: list[str] = Field(default_factory=list)
    eligibility_rules: list[str] = Field(default_factory=list)
    contribution_details: dict[str, Any] = Field(default_factory=dict)
    waiting_periods: list[str] = Field(default_factory=list)
    max_benefits: dict[str, Any] = Field(default_factory=dict)
    notes: list[str] = Field(default_factory=list)
    extraction_confidence: float = Field(default=0.0, ge=0, le=1)
    warnings: list[str] = Field(default_factory=list)
    evidence_references: list[EvidenceReference] = Field(default_factory=list)


class CensusSummary(BaseModel):
    avg_age: float | None = None
    median_salary: float | None = None


class CensusDataset(BaseModel):
    census_id: str
    parent_case_id: str
    source_files: list[str] = Field(default_factory=list)
    employee_count: int = 0
    dependent_count: int = 0
    classes_detected: list[str] = Field(default_factory=list)
    states_detected: list[str] = Field(default_factory=list)
    census_columns_detected: list[str] = Field(default_factory=list)
    census_rows: list[dict[str, Any]] = Field(default_factory=list)
    summary_statistics: CensusSummary = Field(default_factory=CensusSummary)
    anomalies: list[str] = Field(default_factory=list)
    extraction_confidence: float = Field(default=0.0, ge=0, le=1)
    evidence_references: list[EvidenceReference] = Field(default_factory=list)


class QuoteRequest(BaseModel):
    case_id: str
    submission_id: str
    quote_type: QuoteType = QuoteType.new_business
    employer_name: str | None = None
    broker_name: str | None = None
    effective_date: date | None = None
    situs_state: str | None = None
    market_segment: str | None = None
    requested_lobs: list[str] = Field(default_factory=list)
    overall_status: ProcessingStatus = ProcessingStatus.received
    extraction_confidence: float = Field(default=0.0, ge=0, le=1)
    warnings: list[str] = Field(default_factory=list)
    evidence_references: list[EvidenceReference] = Field(default_factory=list)


class BQMConfidence(BaseModel):
    overall: float = Field(ge=0, le=1)
    census: float = Field(ge=0, le=1)
    plan_design: float = Field(ge=0, le=1)


class BQMOutput(BaseModel):
    caseId: str
    submissionId: str
    quoteType: str
    employer: Employer
    broker: Broker
    lobs: list[BQMLobOutput] = Field(default_factory=list)
    census: BQMCensusOutput
    warnings: list[str] = Field(default_factory=list)
    confidence: BQMConfidence

    model_config = {"extra": "forbid"}
