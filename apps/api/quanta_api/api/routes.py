from __future__ import annotations

import base64
import json
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Query, UploadFile, status
from fastapi.responses import PlainTextResponse

from quanta_api.app_state import ServiceContainer
from quanta_api.dependencies import get_container
from quanta_api.domain.contracts import (
    BQMMockResponse,
    CaseResponse,
    CarrierRfpResponse,
    ConnectorIngestResponse,
    ErrorResponse,
    ExtractResponse,
    GmailPushEnvelope,
    GraphWebhookPayload,
    InboundEmailEnqueueRequest,
    InboundMailboxResponse,
    InboundMailboxUpsertRequest,
    InboundEmailPayload,
    ListenerResponse,
    NormalizeResponse,
    ParseResponse,
    ProviderEmailPayload,
    SMTPWebhookPayload,
    SubmissionStatusResponse,
)
from quanta_api.domain.enums import EmailProvider, ProcessingStatus
from quanta_api.domain.models import InboundMailboxConfig

router = APIRouter()

ERROR_RESPONSES = {
    400: {"model": ErrorResponse, "description": "Bad Request"},
    401: {"model": ErrorResponse, "description": "Unauthorized"},
    404: {"model": ErrorResponse, "description": "Not Found"},
    409: {"model": ErrorResponse, "description": "Conflict"},
    500: {"model": ErrorResponse, "description": "Internal Server Error"},
}


def _validate_admin_secret(container: ServiceContainer, provided_secret: str | None) -> None:
    expected_secret = container.smtp_connector.settings.connector_admin_secret
    if expected_secret and provided_secret != expected_secret:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid connector admin secret")


def _validate_provider_webhook_secret(container: ServiceContainer, provided_secret: str | None) -> None:
    expected_secret = container.smtp_connector.settings.provider_webhook_secret
    if expected_secret and provided_secret != expected_secret:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid provider webhook secret")


def _submission_or_404(container: ServiceContainer, submission_id: str, tenant_id: str | None = None):
    submission = container.submission_repository.get(submission_id, tenant_id=tenant_id)
    if submission is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission not found")
    return submission


def _quote_or_404(container: ServiceContainer, case_id: str, tenant_id: str | None = None):
    quote = container.case_repository.get_quote(case_id, tenant_id=tenant_id)
    if quote is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Case not found")
    return quote


@router.get("/health", summary="Liveness probe", description="Returns process liveness and top-level runtime metadata.")
def healthcheck(container: ServiceContainer = Depends(get_container)) -> dict[str, object]:
    return {
        "status": "ok",
        "providers": [item.provider.value for item in container.provider_connector.scaffold_configs()],
        "smtpWebhookSecretConfigured": bool(container.smtp_connector.settings.smtp_webhook_secret),
        "worker": container.worker_service.status(),
        "metrics": container.metrics.snapshot(),
    }


@router.get("/readiness", summary="Readiness probe", description="Returns readiness state for storage, worker, and API dependencies.")
def readiness(container: ServiceContainer = Depends(get_container)) -> dict[str, object]:
    return {
        "status": "ready",
        "repositoryBackend": container.smtp_connector.settings.repository_backend,
        "objectStoreBackend": container.smtp_connector.settings.object_store_backend,
        "worker": container.worker_service.status(),
    }


@router.get("/metrics", summary="Metrics snapshot", description="Returns in-process counters and timings.")
def get_metrics(container: ServiceContainer = Depends(get_container)) -> dict[str, object]:
    return container.metrics.snapshot()


@router.get(
    "/metrics/prometheus",
    summary="Prometheus metrics",
    response_class=PlainTextResponse,
    responses={200: {"content": {"text/plain": {"example": "# TYPE http_request_count counter\nhttp_request_count 4\n"}}}},
)
def get_metrics_prometheus(container: ServiceContainer = Depends(get_container)) -> str:
    return container.metrics.render_prometheus()


@router.get("/v1/listener/providers", summary="List connector providers")
def list_provider_scaffolds(container: ServiceContainer = Depends(get_container)) -> list[dict[str, object]]:
    return [{"provider": item.provider.value, "enabled": item.enabled, "mode": item.mode, "description": item.description} for item in container.provider_connector.scaffold_configs()]


@router.get("/v1/connectors/state", summary="List connector state")
def get_connector_state(x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> list[dict[str, object]]:
    return [item.model_dump(mode="json") for item in container.connector_orchestration.list_cursors(tenant_id=x_quanta_tenant_id)]


@router.get("/v1/inbound-mailboxes", response_model=list[InboundMailboxResponse], summary="List inbound mailbox configs")
def list_inbound_mailboxes(x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> list[InboundMailboxResponse]:
    return [InboundMailboxResponse(**item.model_dump()) for item in container.operations_repository.list_mailboxes(tenant_id=x_quanta_tenant_id)]


@router.put("/v1/inbound-mailboxes/{provider}", response_model=InboundMailboxResponse, responses=ERROR_RESPONSES, summary="Upsert inbound mailbox config")
def upsert_inbound_mailbox(
    provider: str,
    payload: InboundMailboxUpsertRequest,
    x_quanta_admin_secret: str | None = Header(default=None),
    x_quanta_tenant_id: str = Header(default="default"),
    container: ServiceContainer = Depends(get_container),
) -> InboundMailboxResponse:
    _validate_admin_secret(container, x_quanta_admin_secret)
    if provider != payload.provider.value:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provider path does not match request body")
    mailbox = InboundMailboxConfig(
        tenant_id=x_quanta_tenant_id,
        provider=payload.provider,
        mailbox_address=payload.mailbox_address,
        provider_user_id=payload.provider_user_id,
        access_token=payload.access_token,
        mode=payload.mode,
        enabled=payload.enabled,
        metadata=payload.metadata,
    )
    saved = container.operations_repository.save_mailbox(mailbox)
    return InboundMailboxResponse(**saved.model_dump())


@router.delete("/v1/inbound-mailboxes/{provider}", responses=ERROR_RESPONSES, summary="Delete inbound mailbox config")
def delete_inbound_mailbox(
    provider: str,
    x_quanta_admin_secret: str | None = Header(default=None),
    x_quanta_tenant_id: str = Header(default="default"),
    container: ServiceContainer = Depends(get_container),
) -> dict[str, object]:
    _validate_admin_secret(container, x_quanta_admin_secret)
    try:
        provider_enum = EmailProvider(provider)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported provider") from exc
    container.operations_repository.delete_mailbox(provider_enum, tenant_id=x_quanta_tenant_id)
    return {"deleted": True, "provider": provider_enum.value, "tenantId": x_quanta_tenant_id}


@router.post("/v1/inbound-emails/enqueue", responses=ERROR_RESPONSES, summary="Enqueue inbound provider email")
def enqueue_inbound_email(
    payload: InboundEmailEnqueueRequest,
    x_quanta_admin_secret: str | None = Header(default=None),
    x_quanta_tenant_id: str = Header(default="default"),
    container: ServiceContainer = Depends(get_container),
) -> dict[str, object]:
    _validate_admin_secret(container, x_quanta_admin_secret)
    job = container.job_queue.enqueue_connector_ingest(
        provider=payload.provider,
        message_id=payload.message_id,
        tenant_id=x_quanta_tenant_id,
        run_pipeline=payload.run_pipeline,
        event_id=payload.event_id,
    )
    return {"job": job.model_dump(mode="json"), "queued": True}


@router.post(
    "/v1/listener/email",
    response_model=ListenerResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses=ERROR_RESPONSES,
    summary="Submit email payload",
    description="Accepts a normalized email payload with base64 attachments and creates a submission envelope.",
    openapi_extra={"requestBody": {"content": {"application/json": {"example": {
        "sender": "broker@example.com",
        "recipients": ["quotes@strantas.ai"],
        "subject": "ACME Manufacturing RFP - Life, LTD, Dental",
        "body_raw": "<p>Employer: ACME Manufacturing</p>",
        "body_text": "Employer: ACME Manufacturing; Broker: Northstar Benefits; Effective Date: 2026-07-01",
        "attachments": [],
    }}}}},
)
def listener_email(
    payload: InboundEmailPayload,
    x_quanta_tenant_id: str = Header(default="default"),
    container: ServiceContainer = Depends(get_container),
) -> ListenerResponse:
    submission = container.intake_service.ingest_email(payload, tenant_id=x_quanta_tenant_id)
    return ListenerResponse(submission_id=submission.submission_id, status=submission.processing_status, attachment_count=len(submission.attachments))


@router.post(
    "/v1/listener/provider-email",
    response_model=ListenerResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses=ERROR_RESPONSES,
    summary="Submit provider-native email payload",
    openapi_extra={"requestBody": {"content": {"application/json": {"example": {
        "provider": "microsoft_graph",
        "payload": {"subject": "ACME Manufacturing RFP", "bodyPreview": "Employer: ACME Manufacturing"},
    }}}}},
)
def listener_provider_email(
    payload: ProviderEmailPayload,
    x_quanta_tenant_id: str = Header(default="default"),
    container: ServiceContainer = Depends(get_container),
) -> ListenerResponse:
    normalized_payload = container.email_adapter.parse(payload.provider, payload.payload)
    submission = container.intake_service.ingest_email(
        normalized_payload,
        tenant_id=x_quanta_tenant_id,
        source_provider=payload.provider,
    )
    return ListenerResponse(submission_id=submission.submission_id, status=submission.processing_status, attachment_count=len(submission.attachments))


@router.post(
    "/v1/listener/smtp-webhook",
    response_model=ListenerResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses=ERROR_RESPONSES,
    summary="Submit SMTP webhook event",
    openapi_extra={"requestBody": {"content": {"application/json": {"example": {
        "from_email": "broker@example.com",
        "to_emails": ["quotes@strantas.ai"],
        "subject": "SMTP delivered submission",
        "text": "Employer: ACME Manufacturing",
        "html": "<p>Employer: ACME Manufacturing</p>",
        "attachments": [],
    }}}}},
)
def listener_smtp_webhook(
    payload: SMTPWebhookPayload,
    x_quanta_webhook_secret: str | None = Header(default=None),
    x_quanta_tenant_id: str = Header(default="default"),
    container: ServiceContainer = Depends(get_container),
) -> ListenerResponse:
    try:
        result = container.smtp_connector.ingest(payload, x_quanta_webhook_secret, tenant_id=x_quanta_tenant_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
    submission = result.submission
    return ListenerResponse(submission_id=submission.submission_id, status=submission.processing_status, attachment_count=len(submission.attachments))


@router.post(
    "/v1/listener/email-multipart",
    response_model=ListenerResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses=ERROR_RESPONSES,
    summary="Submit multipart email payload",
    description="Accepts sender/body fields and uploaded files without requiring base64 encoding.",
)
async def listener_email_multipart(
    sender: str = Form(...),
    recipients: str = Form(""),
    subject: str = Form(...),
    body_raw: str = Form(""),
    body_text: str = Form(""),
    files: list[UploadFile] = File(default_factory=list),
    x_quanta_tenant_id: str = Header(default="default"),
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
        tenant_id=x_quanta_tenant_id,
    )
    return ListenerResponse(submission_id=submission.submission_id, status=submission.processing_status, attachment_count=len(submission.attachments))


@router.post("/v1/connectors/microsoft-graph/messages/{message_id}/ingest", response_model=ConnectorIngestResponse, responses=ERROR_RESPONSES, summary="Ingest Microsoft Graph message")
def ingest_graph_message(
    message_id: str,
    run_pipeline: bool = True,
    x_quanta_admin_secret: str | None = Header(default=None),
    x_quanta_tenant_id: str = Header(default="default"),
    container: ServiceContainer = Depends(get_container),
) -> ConnectorIngestResponse:
    _validate_admin_secret(container, x_quanta_admin_secret)
    try:
        result = container.graph_connector.ingest_message(message_id, tenant_id=x_quanta_tenant_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Graph connector request failed: {exc}") from exc
    case_id = None
    status_value = result.submission.processing_status
    if run_pipeline:
        job_result = container.job_runner.run_submission(result.submission.submission_id, tenant_id=x_quanta_tenant_id)
        case_id = job_result.quote.case_id
        status_value = ProcessingStatus.normalized
    return ConnectorIngestResponse(submission_id=result.submission.submission_id, case_id=case_id, status=status_value, attachment_count=len(result.submission.attachments), raw_event_storage_key=result.raw_event_storage_key)


@router.post("/v1/connectors/gmail/messages/{message_id}/ingest", response_model=ConnectorIngestResponse, responses=ERROR_RESPONSES, summary="Ingest Gmail message")
def ingest_gmail_message(
    message_id: str,
    run_pipeline: bool = True,
    x_quanta_admin_secret: str | None = Header(default=None),
    x_quanta_tenant_id: str = Header(default="default"),
    container: ServiceContainer = Depends(get_container),
) -> ConnectorIngestResponse:
    _validate_admin_secret(container, x_quanta_admin_secret)
    try:
        result = container.gmail_connector.ingest_message(message_id, tenant_id=x_quanta_tenant_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Gmail connector request failed: {exc}") from exc
    case_id = None
    status_value = result.submission.processing_status
    if run_pipeline:
        job_result = container.job_runner.run_submission(result.submission.submission_id, tenant_id=x_quanta_tenant_id)
        case_id = job_result.quote.case_id
        status_value = ProcessingStatus.normalized
    return ConnectorIngestResponse(submission_id=result.submission.submission_id, case_id=case_id, status=status_value, attachment_count=len(result.submission.attachments), raw_event_storage_key=result.raw_event_storage_key)


@router.post("/v1/connectors/microsoft-graph/subscriptions/refresh", responses=ERROR_RESPONSES)
def refresh_graph_subscription(x_quanta_admin_secret: str | None = Header(default=None), x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> dict[str, object]:
    _validate_admin_secret(container, x_quanta_admin_secret)
    try:
        return container.connector_orchestration.refresh_graph_subscription(tenant_id=x_quanta_tenant_id).model_dump(mode="json")
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Graph subscription refresh failed: {exc}") from exc


@router.post("/v1/connectors/gmail/watch/refresh", responses=ERROR_RESPONSES)
def refresh_gmail_watch(x_quanta_admin_secret: str | None = Header(default=None), x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> dict[str, object]:
    _validate_admin_secret(container, x_quanta_admin_secret)
    try:
        return container.connector_orchestration.refresh_gmail_watch(tenant_id=x_quanta_tenant_id).model_dump(mode="json")
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Gmail watch refresh failed: {exc}") from exc


@router.post("/v1/connectors/microsoft-graph/poll", responses=ERROR_RESPONSES)
def poll_graph_messages(x_quanta_admin_secret: str | None = Header(default=None), x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> dict[str, object]:
    _validate_admin_secret(container, x_quanta_admin_secret)
    try:
        return container.connector_orchestration.poll_graph_messages(tenant_id=x_quanta_tenant_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Graph poll failed: {exc}") from exc


@router.post("/v1/connectors/gmail/poll", responses=ERROR_RESPONSES)
def poll_gmail_messages(x_quanta_admin_secret: str | None = Header(default=None), x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> dict[str, object]:
    _validate_admin_secret(container, x_quanta_admin_secret)
    try:
        return container.connector_orchestration.poll_gmail_messages(tenant_id=x_quanta_tenant_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Gmail poll failed: {exc}") from exc


@router.get("/v1/connectors/microsoft-graph/webhook", response_class=PlainTextResponse)
def graph_webhook_validation(validationToken: str | None = Query(default=None)) -> str:
    return validationToken or ""


@router.post("/v1/connectors/microsoft-graph/webhook", responses=ERROR_RESPONSES)
def graph_webhook_notifications(
    payload: GraphWebhookPayload,
    x_quanta_webhook_secret: str | None = Header(default=None),
    x_quanta_tenant_id: str = Header(default="default"),
    container: ServiceContainer = Depends(get_container),
) -> dict[str, object]:
    _validate_provider_webhook_secret(container, x_quanta_webhook_secret)
    jobs: list[dict[str, object]] = []
    cursor = container.operations_repository.get_cursor(EmailProvider.microsoft_graph, tenant_id=x_quanta_tenant_id)
    for notification in payload.value:
        if notification.clientState and notification.clientState != container.smtp_connector.settings.graph_client_state:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid Graph client state")
        if cursor is None or not notification.subscriptionId or notification.subscriptionId != cursor.subscription_id:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unknown Graph subscription")
        message_id = notification.resourceData.get("id") if notification.resourceData else None
        if message_id is None and notification.resource:
            message_id = notification.resource.rstrip("/").split("/")[-1]
        if message_id:
            job = container.job_queue.enqueue_connector_ingest(
                provider=EmailProvider.microsoft_graph,
                message_id=message_id,
                tenant_id=x_quanta_tenant_id,
                run_pipeline=True,
                event_id=notification.subscriptionId,
            )
        else:
            job = container.job_queue.enqueue_connector_poll(
                provider=EmailProvider.microsoft_graph,
                tenant_id=x_quanta_tenant_id,
                event_id=notification.subscriptionId,
                source="graph_webhook",
            )
        jobs.append(job.model_dump(mode="json"))
    return {"queued": len(jobs), "jobs": jobs}


@router.post("/v1/connectors/gmail/events", responses=ERROR_RESPONSES)
def gmail_push_events(
    envelope: GmailPushEnvelope,
    x_quanta_webhook_secret: str | None = Header(default=None),
    x_quanta_tenant_id: str = Header(default="default"),
    container: ServiceContainer = Depends(get_container),
) -> dict[str, object]:
    _validate_provider_webhook_secret(container, x_quanta_webhook_secret)
    try:
        padding = "=" * (-len(envelope.message.data) % 4)
        decoded = base64.b64decode(envelope.message.data + padding).decode("utf-8")
        payload = json.loads(decoded)
    except (ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Gmail push payload") from exc
    history_id = payload.get("historyId")
    email_address = payload.get("emailAddress")
    job = container.job_queue.enqueue_connector_poll(
        provider=EmailProvider.gmail,
        tenant_id=x_quanta_tenant_id,
        event_id=envelope.message.messageId,
        history_id=history_id,
        source=email_address or "gmail_push",
    )
    return {"queued": 1, "historyId": history_id, "emailAddress": email_address, "job": job.model_dump(mode="json")}


@router.get("/v1/jobs")
def list_jobs(status_filter: str | None = None, x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> list[dict[str, object]]:
    return [job.model_dump(mode="json") for job in container.job_queue.list_jobs(status=status_filter, tenant_id=x_quanta_tenant_id)]


@router.get("/v1/inbound-email-jobs", responses=ERROR_RESPONSES)
def list_inbound_email_jobs(
    status_filter: str | None = None,
    provider: str | None = None,
    x_quanta_admin_secret: str | None = Header(default=None),
    x_quanta_tenant_id: str = Header(default="default"),
    container: ServiceContainer = Depends(get_container),
) -> list[dict[str, object]]:
    _validate_admin_secret(container, x_quanta_admin_secret)
    return [job.model_dump(mode="json") for job in container.job_queue.filter_inbound_email_jobs(status=status_filter, tenant_id=x_quanta_tenant_id, provider=provider)]


@router.get("/v1/inbound-email-jobs/dead-letter", responses=ERROR_RESPONSES)
def list_dead_letter_inbound_email_jobs(
    provider: str | None = None,
    x_quanta_admin_secret: str | None = Header(default=None),
    x_quanta_tenant_id: str = Header(default="default"),
    container: ServiceContainer = Depends(get_container),
) -> list[dict[str, object]]:
    _validate_admin_secret(container, x_quanta_admin_secret)
    return [job.model_dump(mode="json") for job in container.job_queue.filter_inbound_email_jobs(status="dead_letter", tenant_id=x_quanta_tenant_id, provider=provider)]


@router.get("/v1/inbound-email-jobs/dashboard", responses=ERROR_RESPONSES)
def inbound_email_jobs_dashboard(
    x_quanta_admin_secret: str | None = Header(default=None),
    x_quanta_tenant_id: str = Header(default="default"),
    all_tenants: bool = Query(default=False),
    lag_threshold_seconds: int | None = Query(default=None, ge=0),
    container: ServiceContainer = Depends(get_container),
) -> dict[str, object]:
    _validate_admin_secret(container, x_quanta_admin_secret)
    dashboard_tenant = None if all_tenants else x_quanta_tenant_id
    return container.job_queue.inbound_email_dashboard(tenant_id=dashboard_tenant, lag_threshold_seconds=lag_threshold_seconds)


@router.get("/v1/inbound-email-jobs/replay-audit", responses=ERROR_RESPONSES)
def list_replay_audit(
    x_quanta_admin_secret: str | None = Header(default=None),
    x_quanta_tenant_id: str = Header(default="default"),
    all_tenants: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=200),
    container: ServiceContainer = Depends(get_container),
) -> list[dict[str, object]]:
    _validate_admin_secret(container, x_quanta_admin_secret)
    tenant_id = None if all_tenants else x_quanta_tenant_id
    return [audit.model_dump(mode="json") for audit in container.operations_repository.list_replay_audits(limit=limit, tenant_id=tenant_id)]


@router.post("/v1/jobs/run-next", responses=ERROR_RESPONSES)
def run_next_job(x_quanta_admin_secret: str | None = Header(default=None), x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> dict[str, object]:
    _validate_admin_secret(container, x_quanta_admin_secret)
    job = container.job_queue.run_next(tenant_id=x_quanta_tenant_id)
    if job is None:
        return {"status": "idle"}
    return job.model_dump(mode="json")


@router.post("/v1/jobs/{job_id}/replay", responses=ERROR_RESPONSES)
def replay_job(job_id: str, x_quanta_admin_secret: str | None = Header(default=None), container: ServiceContainer = Depends(get_container)) -> dict[str, object]:
    _validate_admin_secret(container, x_quanta_admin_secret)
    try:
        return container.job_queue.replay(job_id).model_dump(mode="json")
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found") from exc


@router.post("/v1/inbound-email-jobs/{job_id}/replay", responses=ERROR_RESPONSES)
def replay_inbound_email_job(job_id: str, x_quanta_admin_secret: str | None = Header(default=None), container: ServiceContainer = Depends(get_container)) -> dict[str, object]:
    _validate_admin_secret(container, x_quanta_admin_secret)
    try:
        return container.job_queue.replay_inbound_email_job(job_id).model_dump(mode="json")
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/v1/inbound-email-jobs/replay", responses=ERROR_RESPONSES)
def bulk_replay_inbound_email_jobs(
    provider: str | None = None,
    status_filter: str = Query(default="dead_letter"),
    all_tenants: bool = Query(default=False),
    x_quanta_admin_secret: str | None = Header(default=None),
    x_quanta_tenant_id: str = Header(default="default"),
    container: ServiceContainer = Depends(get_container),
) -> dict[str, object]:
    _validate_admin_secret(container, x_quanta_admin_secret)
    replay_tenant = None if all_tenants else x_quanta_tenant_id
    jobs = container.job_queue.replay_inbound_email_jobs(tenant_id=replay_tenant, provider=provider, status=status_filter)
    return {"replayed": len(jobs), "jobs": [job.model_dump(mode="json") for job in jobs]}


@router.get("/v1/alerts")
def list_alerts(limit: int = 50, x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> list[dict[str, object]]:
    return [alert.model_dump(mode="json") for alert in container.alert_service.list_alerts(limit=limit, tenant_id=x_quanta_tenant_id)]


@router.get("/v1/worker/status")
def worker_status(container: ServiceContainer = Depends(get_container)) -> dict[str, object]:
    return container.worker_service.status()


@router.post("/v1/worker/tick", responses=ERROR_RESPONSES)
def worker_tick(x_quanta_admin_secret: str | None = Header(default=None), container: ServiceContainer = Depends(get_container)) -> dict[str, object]:
    _validate_admin_secret(container, x_quanta_admin_secret)
    return container.worker_service.tick()


@router.post(
    "/v1/reader/parse",
    response_model=ParseResponse,
    responses={**ERROR_RESPONSES, 200: {"description": "Reader inventory", "content": {"application/json": {"example": {
        "submission_id": "SUB-2026-000001",
        "submission_intent": "rfp_submission",
        "document_inventory": [{"file_name": "acme_census.pdf", "document_type": "census"}],
        "warnings": [],
        "confidence": 0.84,
        "status": "parsed",
    }}}}},
    summary="Parse and classify submission",
)
def reader_parse(submission_id: str, x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> ParseResponse:
    _submission_or_404(container, submission_id, tenant_id=x_quanta_tenant_id)
    inventory = container.reader_service.parse_submission(submission_id, tenant_id=x_quanta_tenant_id)
    return ParseResponse(submission_id=inventory.submission_id, submission_intent=inventory.submission_intent, document_inventory=inventory.document_inventory, warnings=inventory.warnings, confidence=inventory.confidence, status=ProcessingStatus.parsed)


@router.post(
    "/v1/extractor/run",
    response_model=ExtractResponse,
    responses={**ERROR_RESPONSES, 200: {"description": "Extraction result", "content": {"application/json": {"example": {
        "submission_id": "SUB-2026-000001",
        "case_id": "QNT-2026-000001",
        "detected_lobs": ["group_life", "group_ltd", "dental"],
        "status": "extracted",
        "warnings": [],
        "evidence_references": [],
    }}}}},
    summary="Run extraction",
)
def extractor_run(submission_id: str, x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> ExtractResponse:
    _submission_or_404(container, submission_id, tenant_id=x_quanta_tenant_id)
    quote, _, _ = container.pipeline_service.run_extraction(submission_id, tenant_id=x_quanta_tenant_id)
    return ExtractResponse(submission_id=submission_id, case_id=quote.case_id, detected_lobs=quote.requested_lobs, status=ProcessingStatus.extracted, warnings=quote.warnings, evidence_references=[ref.model_dump(exclude_none=True) for ref in quote.evidence_references])


@router.post(
    "/v1/normalizer/run",
    response_model=NormalizeResponse,
    responses={**ERROR_RESPONSES, 200: {"description": "Normalized case", "content": {"application/json": {"example": {
        "case_id": "QNT-2026-000001",
        "status": "normalized",
        "quote_request": {"employer_name": "ACME Manufacturing", "broker_name": "Northstar Benefits"},
        "lob_requests": [],
        "census": {"employee_count": 4, "dependent_count": 6},
    }}}}},
    summary="Run normalization",
    openapi_extra={"parameters": [{"name": "submission_id", "in": "query", "required": True, "schema": {"type": "string"}, "example": "SUB-2026-000001"}]},
)
def normalizer_run(submission_id: str, x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> NormalizeResponse:
    _submission_or_404(container, submission_id, tenant_id=x_quanta_tenant_id)
    quote, lob_requests, census = container.pipeline_service.run_normalization(submission_id, tenant_id=x_quanta_tenant_id)
    return NormalizeResponse(case_id=quote.case_id, status=ProcessingStatus.normalized, quote_request=quote, lob_requests=lob_requests, census=census)


@router.get(
    "/v1/output/{case_id}",
    response_model=BQMMockResponse,
    responses={**ERROR_RESPONSES, 200: {"description": "BQM output", "content": {"application/json": {"example": {
        "caseId": "QNT-2026-000001",
        "submissionId": "SUB-2026-000001",
        "quoteType": "new_business",
        "employer": {"name": "ACME Manufacturing", "effective_date": "2026-07-01", "situs_state": "TX"},
        "broker": {"name": "Northstar Benefits"},
        "lobs": [],
        "census": {"employeeCount": 4, "dependentCount": 6, "classesDetected": ["Salaried"], "statesDetected": ["TX"], "rows": [], "summary": {}, "evidenceReferences": [], "fieldResults": {}},
        "warnings": [],
        "confidence": {"overall": 0.72, "census": 0.92, "plan_design": 0.68},
        "generated_at": "2026-03-13T13:02:11.301014+00:00",
    }}}}},
    summary="Get BQM output",
)
def get_output(case_id: str, x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> BQMMockResponse:
    _quote_or_404(container, case_id, tenant_id=x_quanta_tenant_id)
    output = container.pipeline_service.get_output(case_id, tenant_id=x_quanta_tenant_id)
    return BQMMockResponse(**output.model_dump(), generated_at=datetime.now(timezone.utc).isoformat())


@router.get(
    "/v1/output/{case_id}/carrier-rfp",
    response_model=CarrierRfpResponse,
    responses={**ERROR_RESPONSES, 200: {"description": "Carrier-aligned group RFP", "content": {"application/json": {"example": {
        "identifier": "uuid",
        "effectiveDate": "2026-07-01",
        "dueDate": "2026-05-15",
        "status": "pending",
        "employer": {"name": "ACME Manufacturing", "contacts": [], "locations": []},
        "groupConfiguration": {"numberOfEligibleEmployees": 4, "benefitClasses": [], "coverages": [], "producers": []},
        "census": [],
        "files": [{"fileName": "acme_census.pdf", "mediaType": "application/pdf", "documentType": "census"}],
        "generated_at": "2026-03-13T13:02:11.301014+00:00",
    }}}}},
    summary="Get carrier-aligned RFP output",
)
def get_carrier_output(case_id: str, x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> CarrierRfpResponse:
    _quote_or_404(container, case_id, tenant_id=x_quanta_tenant_id)
    output = container.pipeline_service.get_carrier_output(case_id, tenant_id=x_quanta_tenant_id)
    return CarrierRfpResponse(**output.model_dump(), generated_at=datetime.now(timezone.utc).isoformat())


@router.get(
    "/v1/submissions/{submission_id}",
    response_model=SubmissionStatusResponse,
    responses={**ERROR_RESPONSES, 200: {"description": "Submission status", "content": {"application/json": {"example": {
        "submission_id": "SUB-2026-000001",
        "tenant_id": "tenant-alpha",
        "status": "normalized",
        "subject": "ACME Manufacturing RFP",
        "sender": "broker@example.com",
        "received_at": "2026-03-13T13:00:00+00:00",
        "case_id": "QNT-2026-000001",
    }}}}},
    summary="Get submission status",
)
def get_submission_status(submission_id: str, x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> SubmissionStatusResponse:
    submission = _submission_or_404(container, submission_id, tenant_id=x_quanta_tenant_id)
    return SubmissionStatusResponse(submission_id=submission.submission_id, tenant_id=submission.tenant_id, status=submission.processing_status, subject=submission.subject, sender=submission.sender, received_at=submission.received_at.isoformat(), case_id=container.submission_repository.get_case_id(submission.submission_id, tenant_id=submission.tenant_id), source_provider=submission.source_provider.value if submission.source_provider else None, raw_event_storage_key=submission.raw_event_storage_key, connector_message_id=submission.connector_message_id)


@router.get(
    "/v1/cases/{case_id}",
    response_model=CaseResponse,
    responses={**ERROR_RESPONSES, 200: {"description": "Normalized case", "content": {"application/json": {"example": {
        "quote_request": {"case_id": "QNT-2026-000001", "employer_name": "ACME Manufacturing"},
        "lob_requests": [],
        "census": {"employee_count": 4},
    }}}}},
    summary="Get normalized case",
)
def get_case(case_id: str, x_quanta_tenant_id: str = Header(default="default"), container: ServiceContainer = Depends(get_container)) -> CaseResponse:
    quote_request = _quote_or_404(container, case_id, tenant_id=x_quanta_tenant_id)
    return CaseResponse(quote_request=quote_request, lob_requests=container.case_repository.get_lobs(case_id, tenant_id=x_quanta_tenant_id), census=container.case_repository.get_census(case_id, tenant_id=x_quanta_tenant_id))
