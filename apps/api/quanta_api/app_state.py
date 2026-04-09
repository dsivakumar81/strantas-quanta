from __future__ import annotations

from dataclasses import dataclass

from quanta_api.domain.repositories import CaseRepository, ObjectStore, OperationsRepository, SubmissionRepository
from quanta_api.services.alerts import AlertService
from quanta_api.services.attachment_intelligence import AttachmentIntelligenceService
from quanta_api.services.bqm_validator import BQMValidationService
from quanta_api.services.census_extractor import CensusExtractionService
from quanta_api.services.connector_execution import GmailConnectorExecutionService, GraphConnectorExecutionService
from quanta_api.services.connector_orchestration import ConnectorOrchestrationService
from quanta_api.services.email_adapters import EmailAdapterService
from quanta_api.services.file_sniffer import FileSniffer
from quanta_api.services.id_factory import IdFactory
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


@dataclass
class ServiceContainer:
    ids: IdFactory
    submission_repository: SubmissionRepository
    case_repository: CaseRepository
    operations_repository: OperationsRepository
    object_store: ObjectStore
    provider_connector: ProviderConnectorService
    email_adapter: EmailAdapterService
    smtp_connector: SMTPWebhookConnectorService
    graph_connector: GraphConnectorExecutionService
    gmail_connector: GmailConnectorExecutionService
    connector_orchestration: ConnectorOrchestrationService
    attachment_intelligence: AttachmentIntelligenceService
    census_extractor: CensusExtractionService
    bqm_validator: BQMValidationService
    normalization_service: NormalizationService
    reader_service: ReaderService
    intake_service: IntakeService
    pipeline_service: PipelineService
    retry_service: RetryService
    metrics: MetricsService
    trace_sink: TraceSinkService
    file_sniffer: FileSniffer
    alert_service: AlertService
    job_runner: SubmissionJobRunner
    job_queue: JobQueueService
    worker_service: ConnectorWorkerService
