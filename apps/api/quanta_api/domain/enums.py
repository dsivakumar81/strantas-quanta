from enum import Enum


class ProcessingStatus(str, Enum):
    received = "received"
    parsed = "parsed"
    extracted = "extracted"
    normalized = "normalized"
    completed = "completed"
    failed = "failed"


class ModuleStatus(str, Enum):
    success = "success"
    success_with_warnings = "success_with_warnings"
    partial = "partial"
    failed = "failed"


class QuoteType(str, Enum):
    new_business = "new_business"
    renewal = "renewal"
    requote = "requote"
    amendment = "amendment"


class SourceChannel(str, Enum):
    email = "email"
    api = "api"
    upload = "upload"


class AttachmentType(str, Enum):
    email_attachment = "email_attachment"
    embedded_link = "embedded_link"


class SourceType(str, Enum):
    email_body = "email_body"
    attachment = "attachment"


class SubmissionIntent(str, Enum):
    rfp_submission = "rfp_submission"
    census_only = "census_only"
    general_email = "general_email"


class DocumentType(str, Enum):
    census = "census"
    plan_summary = "plan_summary"
    rate_exhibit = "rate_exhibit"
    narrative_rfp = "narrative_rfp"
    image_scan = "image_scan"
    unknown = "unknown"


class EmailProvider(str, Enum):
    manual = "manual"
    microsoft_graph = "microsoft_graph"
    gmail = "gmail"
    smtp_webhook = "smtp_webhook"
