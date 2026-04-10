from __future__ import annotations

import httpx

from quanta_api.app_state import ServiceContainer
from quanta_api.core.config import Settings, settings as default_settings
from quanta_api.services.alerts import AlertService
from quanta_api.services.attachment_intelligence import AttachmentIntelligenceService
from quanta_api.services.bqm_validator import BQMValidationService
from quanta_api.services.census_extractor import CensusExtractionService
from quanta_api.services.connector_execution import GmailConnectorExecutionService, GraphConnectorExecutionService
from quanta_api.services.connector_orchestration import ConnectorOrchestrationService
from quanta_api.services.email_adapters import EmailAdapterService
from quanta_api.services.file_sniffer import FileSniffer
from quanta_api.services.id_factory import IdFactory, PostgresIdFactory
from quanta_api.services.intake import IntakeService
from quanta_api.services.job_queue import JobQueueService
from quanta_api.services.job_runner import SubmissionJobRunner
from quanta_api.services.metrics import MetricsService
from quanta_api.services.normalizer import NormalizationService
from quanta_api.services.pipeline import PipelineService
from quanta_api.services.provider_connectors import ProviderConnectorService
from quanta_api.services.reader import ReaderService
from quanta_api.services.retry import RetryService
from quanta_api.services.smtp_connector import SMTPWebhookConnectorService
from quanta_api.services.tracing import TraceSinkService
from quanta_api.services.worker import ConnectorWorkerService
from quanta_api.storage.memory import InMemoryCaseRepository, InMemoryOperationsRepository, InMemorySubmissionRepository
from quanta_api.storage.object_store import LocalObjectStore, S3ObjectStore
from quanta_api.storage.postgres import PostgresCaseRepository, PostgresOperationsRepository, PostgresSubmissionRepository


def build_service_container(app_settings: Settings | None = None) -> ServiceContainer:
    active_settings = app_settings or default_settings
    ids: IdFactory = IdFactory()

    if active_settings.repository_backend == "postgres":
        ids = PostgresIdFactory(active_settings.database_url)
        submission_repository = PostgresSubmissionRepository(active_settings.database_url)
        case_repository = PostgresCaseRepository(active_settings.database_url)
        operations_repository = PostgresOperationsRepository(active_settings.database_url)
    else:
        submission_repository = InMemorySubmissionRepository()
        case_repository = InMemoryCaseRepository()
        operations_repository = InMemoryOperationsRepository()

    if active_settings.object_store_backend == "s3":
        object_store = S3ObjectStore(
            bucket_name=active_settings.s3_bucket_name,
            endpoint_url=active_settings.s3_endpoint_url,
            region_name=active_settings.s3_region_name,
            access_key_id=active_settings.s3_access_key_id,
            secret_access_key=active_settings.s3_secret_access_key,
        )
    else:
        object_store = LocalObjectStore(active_settings.object_store_root)

    provider_connector = ProviderConnectorService(active_settings)
    email_adapter = EmailAdapterService()
    metrics = MetricsService(settings=active_settings)
    file_sniffer = FileSniffer()
    retry_service = RetryService(attempts=active_settings.connector_retry_attempts, base_delay_seconds=active_settings.connector_retry_base_delay_seconds)
    trace_client = httpx.Client(timeout=active_settings.connector_timeout_seconds)
    trace_sink = TraceSinkService(settings=active_settings, client=trace_client)
    attachment_intelligence = AttachmentIntelligenceService(object_store=object_store)
    census_extractor = CensusExtractionService(object_store=object_store, ids=ids)
    bqm_validator = BQMValidationService()
    normalization_service = NormalizationService()
    reader_service = ReaderService(submission_repository=submission_repository, object_store=object_store)
    intake_service = IntakeService(submission_repository=submission_repository, object_store=object_store, ids=ids, settings=active_settings, file_sniffer=file_sniffer, trace_sink=trace_sink)
    alert_client = httpx.Client(timeout=active_settings.connector_timeout_seconds)
    alert_service = AlertService(operations_repository=operations_repository, ids=ids, settings=active_settings, client=alert_client)
    graph_client = httpx.Client(timeout=active_settings.connector_timeout_seconds)
    gmail_client = httpx.Client(timeout=active_settings.connector_timeout_seconds)
    smtp_connector = SMTPWebhookConnectorService(settings=active_settings, object_store=object_store, email_adapter=email_adapter, intake_service=intake_service)
    graph_connector = GraphConnectorExecutionService(settings=active_settings, object_store=object_store, email_adapter=email_adapter, intake_service=intake_service, operations_repository=operations_repository, retry_service=retry_service, metrics=metrics, client=graph_client)
    gmail_connector = GmailConnectorExecutionService(settings=active_settings, object_store=object_store, email_adapter=email_adapter, intake_service=intake_service, operations_repository=operations_repository, retry_service=retry_service, metrics=metrics, client=gmail_client)
    pipeline_service = PipelineService(
        submission_repository=submission_repository,
        case_repository=case_repository,
        attachment_intelligence=attachment_intelligence,
        census_extractor=census_extractor,
        bqm_validator=bqm_validator,
        normalization_service=normalization_service,
        reader_service=reader_service,
        ids=ids,
        trace_sink=trace_sink,
    )
    job_runner = SubmissionJobRunner(reader_service=reader_service, pipeline_service=pipeline_service, retry_service=retry_service, metrics=metrics)
    job_queue = JobQueueService(operations_repository=operations_repository, ids=ids, graph_connector=graph_connector, gmail_connector=gmail_connector, submission_job_runner=job_runner, metrics=metrics, alert_service=alert_service, settings=active_settings)
    connector_orchestration = ConnectorOrchestrationService(settings=active_settings, operations_repository=operations_repository, retry_service=retry_service, metrics=metrics, alert_service=alert_service, job_queue=job_queue, graph_client=graph_client, gmail_client=gmail_client)
    job_queue.bind_connector_orchestration(connector_orchestration)
    worker_service = ConnectorWorkerService(settings=active_settings, connector_orchestration=connector_orchestration, job_queue=job_queue)
    return ServiceContainer(
        ids=ids,
        submission_repository=submission_repository,
        case_repository=case_repository,
        operations_repository=operations_repository,
        provider_connector=provider_connector,
        email_adapter=email_adapter,
        smtp_connector=smtp_connector,
        graph_connector=graph_connector,
        gmail_connector=gmail_connector,
        connector_orchestration=connector_orchestration,
        object_store=object_store,
        attachment_intelligence=attachment_intelligence,
        census_extractor=census_extractor,
        bqm_validator=bqm_validator,
        normalization_service=normalization_service,
        reader_service=reader_service,
        intake_service=intake_service,
        pipeline_service=pipeline_service,
        retry_service=retry_service,
        metrics=metrics,
        trace_sink=trace_sink,
        file_sniffer=file_sniffer,
        alert_service=alert_service,
        job_runner=job_runner,
        job_queue=job_queue,
        worker_service=worker_service,
    )
