from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status

from quanta_api.app_state import ServiceContainer
from quanta_api.dependencies import get_container
from quanta_api.domain.contracts import (
    BQMMockResponse,
    CaseResponse,
    ExtractResponse,
    InboundEmailPayload,
    ListenerResponse,
    NormalizeResponse,
    ParseResponse,
    ProviderEmailPayload,
    SMTPWebhookPayload,
    SubmissionStatusResponse,
)
from quanta_api.domain.enums import EmailProvider, ProcessingStatus

router = APIRouter()


@router.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/v1/listener/providers")
def list_provider_scaffolds(
    container: ServiceContainer = Depends(get_container),
) -> list[dict[str, object]]:
    return [
        {
            "provider": item.provider.value,
            "enabled": item.enabled,
            "mode": item.mode,
            "description": item.description,
        }
        for item in container.provider_connector.scaffold_configs()
    ]


@router.post("/v1/listener/email", response_model=ListenerResponse, status_code=status.HTTP_202_ACCEPTED)
def listener_email(
    payload: InboundEmailPayload,
    container: ServiceContainer = Depends(get_container),
) -> ListenerResponse:
    submission = container.intake_service.ingest_email(payload)
    return ListenerResponse(
        submission_id=submission.submission_id,
        status=submission.processing_status,
        attachment_count=len(submission.attachments),
    )


@router.post("/v1/listener/provider-email", response_model=ListenerResponse, status_code=status.HTTP_202_ACCEPTED)
def listener_provider_email(
    payload: ProviderEmailPayload,
    container: ServiceContainer = Depends(get_container),
) -> ListenerResponse:
    normalized_payload = container.email_adapter.parse(payload.provider, payload.payload)
    submission = container.intake_service.ingest_email(normalized_payload)
    return ListenerResponse(
        submission_id=submission.submission_id,
        status=submission.processing_status,
        attachment_count=len(submission.attachments),
    )


@router.post("/v1/listener/smtp-webhook", response_model=ListenerResponse, status_code=status.HTTP_202_ACCEPTED)
def listener_smtp_webhook(
    payload: SMTPWebhookPayload,
    container: ServiceContainer = Depends(get_container),
) -> ListenerResponse:
    normalized_payload = container.email_adapter.parse(
        provider=EmailProvider.smtp_webhook,
        payload=payload.model_dump(),
    )
    submission = container.intake_service.ingest_email(normalized_payload)
    return ListenerResponse(
        submission_id=submission.submission_id,
        status=submission.processing_status,
        attachment_count=len(submission.attachments),
    )


@router.post("/v1/listener/email-multipart", response_model=ListenerResponse, status_code=status.HTTP_202_ACCEPTED)
async def listener_email_multipart(
    sender: str = Form(...),
    recipients: str = Form(""),
    subject: str = Form(...),
    body_raw: str = Form(""),
    body_text: str = Form(""),
    files: list[UploadFile] = File(default_factory=list),
    container: ServiceContainer = Depends(get_container),
) -> ListenerResponse:
    parsed_recipients = [item.strip() for item in recipients.split(",") if item.strip()]
    materialized_files: list[tuple[str, str | None, bytes]] = []
    for upload in files:
        materialized_files.append((upload.filename, upload.content_type, await upload.read()))
    submission = container.intake_service.ingest_email_with_files(
        sender=sender,
        recipients=parsed_recipients,
        subject=subject,
        body_raw=body_raw,
        body_text=body_text,
        files=materialized_files,
    )
    return ListenerResponse(
        submission_id=submission.submission_id,
        status=submission.processing_status,
        attachment_count=len(submission.attachments),
    )


@router.post("/v1/reader/parse", response_model=ParseResponse)
def reader_parse(
    submission_id: str,
    container: ServiceContainer = Depends(get_container),
) -> ParseResponse:
    if container.submission_repository.get(submission_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission not found")
    inventory = container.reader_service.parse_submission(submission_id)
    return ParseResponse(
        submission_id=inventory.submission_id,
        submission_intent=inventory.submission_intent,
        document_inventory=inventory.document_inventory,
        warnings=inventory.warnings,
        confidence=inventory.confidence,
        status=ProcessingStatus.parsed,
    )


@router.post("/v1/extractor/run", response_model=ExtractResponse)
def extractor_run(
    submission_id: str,
    container: ServiceContainer = Depends(get_container),
) -> ExtractResponse:
    if container.submission_repository.get(submission_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission not found")
    quote, _, _ = container.pipeline_service.run_extraction(submission_id)
    return ExtractResponse(
        submission_id=submission_id,
        case_id=quote.case_id,
        detected_lobs=quote.requested_lobs,
        status=ProcessingStatus.extracted,
        warnings=quote.warnings,
        evidence_references=[ref.model_dump(exclude_none=True) for ref in quote.evidence_references],
    )


@router.post("/v1/normalizer/run", response_model=NormalizeResponse)
def normalizer_run(
    submission_id: str,
    container: ServiceContainer = Depends(get_container),
) -> NormalizeResponse:
    if container.submission_repository.get(submission_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission not found")
    quote, lob_requests, census = container.pipeline_service.run_normalization(submission_id)
    return NormalizeResponse(
        case_id=quote.case_id,
        status=ProcessingStatus.normalized,
        quote_request=quote,
        lob_requests=lob_requests,
        census=census,
    )


@router.get("/v1/output/{case_id}", response_model=BQMMockResponse)
def get_output(
    case_id: str,
    container: ServiceContainer = Depends(get_container),
) -> BQMMockResponse:
    if container.case_repository.get_quote(case_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Case not found")
    output = container.pipeline_service.get_output(case_id)
    return BQMMockResponse(**output.model_dump(), generated_at=datetime.now(timezone.utc).isoformat())


@router.get("/v1/submissions/{submission_id}", response_model=SubmissionStatusResponse)
def get_submission_status(
    submission_id: str,
    container: ServiceContainer = Depends(get_container),
) -> SubmissionStatusResponse:
    submission = container.submission_repository.get(submission_id)
    if submission is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission not found")
    return SubmissionStatusResponse(
        submission_id=submission.submission_id,
        status=submission.processing_status,
        subject=submission.subject,
        sender=submission.sender,
        received_at=submission.received_at.isoformat(),
        case_id=container.submission_repository.get_case_id(submission.submission_id),
    )


@router.get("/v1/cases/{case_id}", response_model=CaseResponse)
def get_case(
    case_id: str,
    container: ServiceContainer = Depends(get_container),
) -> CaseResponse:
    quote_request = container.case_repository.get_quote(case_id)
    if quote_request is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Case not found")
    return CaseResponse(
        quote_request=quote_request,
        lob_requests=container.case_repository.get_lobs(case_id),
        census=container.case_repository.get_census(case_id),
    )
