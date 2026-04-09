from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from quanta_api.domain.enums import (
    AlertSeverity,
    AttachmentType,
    DocumentType,
    EmailProvider,
    JobStatus,
    JobType,
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
    detected_content_type: str | None = None
    size_bytes: int | None = None
    storage_key: str | None = None
    archive_file_name: str | None = None
    archive_member_path: str | None = None
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


class FieldExtractionResult(BaseModel):
    value: Any | None = None
    confidence: float = Field(ge=0, le=1)
    evidence: list[EvidenceReference] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class ProcessingOutcome(BaseModel):
    status: ModuleStatus
    confidence: float = Field(ge=0, le=1)
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    evidence_references: list[EvidenceReference] = Field(default_factory=list)


class SubmissionEnvelope(BaseModel):
    submission_id: str
    tenant_id: str = "default"
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
    source_provider: EmailProvider | None = None
    raw_event_storage_key: str | None = None
    connector_message_id: str | None = None


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
    fieldResults: dict[str, FieldExtractionResult] = Field(default_factory=dict)


class BQMCensusOutput(BaseModel):
    employeeCount: int = 0
    dependentCount: int = 0
    classesDetected: list[str] = Field(default_factory=list)
    statesDetected: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    summary: dict[str, Any] = Field(default_factory=dict)
    evidenceReferences: list[dict[str, Any]] = Field(default_factory=list)
    fieldResults: dict[str, FieldExtractionResult] = Field(default_factory=dict)


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
    attributes: dict[str, Any] = Field(default_factory=dict)
    notes: list[str] = Field(default_factory=list)
    field_results: dict[str, FieldExtractionResult] = Field(default_factory=dict)


class LOBRequest(BaseModel):
    lob_case_id: str
    tenant_id: str = "default"
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
    field_results: dict[str, FieldExtractionResult] = Field(default_factory=dict)


class CensusSummary(BaseModel):
    avg_age: float | None = None
    median_salary: float | None = None


class CensusDataset(BaseModel):
    census_id: str
    tenant_id: str = "default"
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
    field_results: dict[str, FieldExtractionResult] = Field(default_factory=dict)


class QuoteRequest(BaseModel):
    case_id: str
    tenant_id: str = "default"
    submission_id: str
    quote_type: QuoteType = QuoteType.new_business
    employer_name: str | None = None
    broker_name: str | None = None
    broker_agency_name: str | None = None
    broker_contact_name: str | None = None
    broker_contact_email: str | None = None
    employer_contact_name: str | None = None
    employer_contact_email: str | None = None
    effective_date: date | None = None
    response_due_date: date | None = None
    situs_state: str | None = None
    market_segment: str | None = None
    incumbent_carrier: str | None = None
    requested_lobs: list[str] = Field(default_factory=list)
    overall_status: ProcessingStatus = ProcessingStatus.received
    extraction_confidence: float = Field(default=0.0, ge=0, le=1)
    warnings: list[str] = Field(default_factory=list)
    evidence_references: list[EvidenceReference] = Field(default_factory=list)
    field_results: dict[str, FieldExtractionResult] = Field(default_factory=dict)


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


class CarrierContact(BaseModel):
    fullName: str | None = None
    workEmail: str | None = None
    fieldResults: dict[str, FieldExtractionResult] = Field(default_factory=dict)


class CarrierLocation(BaseModel):
    addressLine1: str | None = None
    city: str | None = None
    state: str | None = None
    postalCode: str | None = None
    fieldResults: dict[str, FieldExtractionResult] = Field(default_factory=dict)


class CarrierBroker(BaseModel):
    agencyName: str | None = None
    contact: CarrierContact | None = None
    fieldResults: dict[str, FieldExtractionResult] = Field(default_factory=dict)


class CarrierProducer(BaseModel):
    type: str = "brokerOfRecord"
    broker: CarrierBroker | None = None
    fieldResults: dict[str, FieldExtractionResult] = Field(default_factory=dict)


class CarrierBenefitClass(BaseModel):
    identifier: str
    name: str
    minWeeklyEligibleHours: str | None = None


class CarrierCoverage(BaseModel):
    coverageType: str
    benefitPlanName: str | None = None
    contributionType: str | None = None
    planDetails: dict[str, Any] = Field(default_factory=dict)
    dentalPlanDetails: "CarrierDentalPlanDetails | None" = None
    visionPlanDetails: "CarrierVisionPlanDetails | None" = None
    fieldResults: dict[str, FieldExtractionResult] = Field(default_factory=dict)


class CarrierDentalPlanDetails(BaseModel):
    coverageTiers: str | None = None
    preventivePercent: int | None = None
    basicPercent: int | None = None
    majorPercent: int | None = None
    orthodontiaPercent: int | None = None
    orthodontiaAgeLimit: int | None = None
    deductible: int | None = None
    annualMaximum: float | None = None
    officeVisitCopay: int | None = None
    serviceWaitingPeriods: str | None = None
    fieldResults: dict[str, FieldExtractionResult] = Field(default_factory=dict)


class CarrierVisionPlanDetails(BaseModel):
    examCopay: int | None = None
    materialsCopay: int | None = None
    lensCopay: int | None = None
    frameAllowance: float | None = None
    contactAllowance: float | None = None
    frequencyMonths: int | None = None
    laserCorrectionAllowance: float | None = None
    fieldResults: dict[str, FieldExtractionResult] = Field(default_factory=dict)


class CarrierGroupConfiguration(BaseModel):
    benefitClasses: list[CarrierBenefitClass] = Field(default_factory=list)
    coverages: list[CarrierCoverage] = Field(default_factory=list)
    producers: list[CarrierProducer] = Field(default_factory=list)
    numberOfEligibleEmployees: int | None = None


class CarrierGroupMember(BaseModel):
    employeeCode: str
    dependentRelationship: str = "employee"
    employmentType: str | None = None
    gender: str | None = None
    birthDate: str | None = None
    postalCode: str | None = None
    employmentStatus: str | None = None
    employeeJobTitle: str | None = None
    benefitClassName: str | None = None
    annualIncome: float | None = None
    coverage: list[dict[str, Any]] = Field(default_factory=list)


class CarrierFile(BaseModel):
    fileName: str
    mediaType: str | None = None
    documentType: str | None = None
    storageKey: str | None = None
    fieldResults: dict[str, FieldExtractionResult] = Field(default_factory=dict)


class CarrierEmployer(BaseModel):
    name: str
    contacts: list[CarrierContact] = Field(default_factory=list)
    locations: list[CarrierLocation] = Field(default_factory=list)
    fieldResults: dict[str, FieldExtractionResult] = Field(default_factory=dict)


class CarrierGroupRfp(BaseModel):
    identifier: str
    effectiveDate: str | None = None
    dueDate: str | None = None
    notes: str | None = None
    status: str = "pending"
    employer: CarrierEmployer
    hasEmployeeOutsideUS: bool = False
    groupConfiguration: CarrierGroupConfiguration = Field(default_factory=CarrierGroupConfiguration)
    census: list[CarrierGroupMember] = Field(default_factory=list)
    marketingStrategy: str | None = None
    files: list[CarrierFile] = Field(default_factory=list)
    bpRfpUrl: str | None = None
    fieldResults: dict[str, FieldExtractionResult] = Field(default_factory=dict)


class ConnectorCursor(BaseModel):
    tenant_id: str = "default"
    provider: EmailProvider
    cursor: str | None = None
    subscription_id: str | None = None
    subscription_expires_at: datetime | None = None
    last_polled_at: datetime | None = None
    last_message_id: str | None = None
    status: str = "idle"
    warnings: list[str] = Field(default_factory=list)


class InboundMailboxConfig(BaseModel):
    tenant_id: str = "default"
    provider: EmailProvider
    mailbox_address: str
    provider_user_id: str
    access_token: str
    mode: str = "polling"
    enabled: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = Field(default_factory=dict)


class JobRecord(BaseModel):
    job_id: str
    tenant_id: str = "default"
    job_type: JobType
    status: JobStatus = JobStatus.queued
    payload: dict[str, Any] = Field(default_factory=dict)
    dedupe_key: str | None = None
    attempts: int = 0
    max_attempts: int = 3
    available_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str | None = None
    dead_letter_reason: str | None = None


class AlertEvent(BaseModel):
    alert_id: str
    tenant_id: str = "default"
    severity: AlertSeverity
    source: str
    message: str
    context: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ReplayAuditRecord(BaseModel):
    audit_id: str
    tenant_id: str = "default"
    job_id: str
    job_type: JobType
    provider: str | None = None
    previous_status: str | None = None
    actor: str = "system"
    replay_scope: str = "single"
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class IdempotencyRecord(BaseModel):
    tenant_id: str
    fingerprint: str
    submission_id: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
