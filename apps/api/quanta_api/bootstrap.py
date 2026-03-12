from __future__ import annotations

from quanta_api.app_state import ServiceContainer
from quanta_api.core.config import Settings, settings as default_settings
from quanta_api.services.attachment_intelligence import AttachmentIntelligenceService
from quanta_api.services.bqm_validator import BQMValidationService
from quanta_api.services.census_extractor import CensusExtractionService
from quanta_api.services.email_adapters import EmailAdapterService
from quanta_api.services.id_factory import IdFactory
from quanta_api.services.intake import IntakeService
from quanta_api.services.normalizer import NormalizationService
from quanta_api.services.pipeline import PipelineService
from quanta_api.services.provider_connectors import ProviderConnectorService
from quanta_api.services.reader import ReaderService
from quanta_api.storage.memory import InMemoryCaseRepository, InMemorySubmissionRepository
from quanta_api.storage.object_store import LocalObjectStore, S3ObjectStore
from quanta_api.storage.postgres import PostgresCaseRepository, PostgresSubmissionRepository


def build_service_container(app_settings: Settings | None = None) -> ServiceContainer:
    active_settings = app_settings or default_settings
    ids = IdFactory()

    if active_settings.repository_backend == "postgres":
        submission_repository = PostgresSubmissionRepository(active_settings.database_url)
        case_repository = PostgresCaseRepository(active_settings.database_url)
    else:
        submission_repository = InMemorySubmissionRepository()
        case_repository = InMemoryCaseRepository()

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

    provider_connector = ProviderConnectorService()
    email_adapter = EmailAdapterService()
    attachment_intelligence = AttachmentIntelligenceService(object_store=object_store)
    census_extractor = CensusExtractionService(object_store=object_store, ids=ids)
    bqm_validator = BQMValidationService()
    normalization_service = NormalizationService()
    reader_service = ReaderService(submission_repository=submission_repository, object_store=object_store)
    intake_service = IntakeService(
        submission_repository=submission_repository,
        object_store=object_store,
        ids=ids,
    )
    pipeline_service = PipelineService(
        submission_repository=submission_repository,
        case_repository=case_repository,
        attachment_intelligence=attachment_intelligence,
        census_extractor=census_extractor,
        bqm_validator=bqm_validator,
        normalization_service=normalization_service,
        ids=ids,
    )
    return ServiceContainer(
        ids=ids,
        submission_repository=submission_repository,
        case_repository=case_repository,
        provider_connector=provider_connector,
        email_adapter=email_adapter,
        object_store=object_store,
        attachment_intelligence=attachment_intelligence,
        census_extractor=census_extractor,
        bqm_validator=bqm_validator,
        normalization_service=normalization_service,
        reader_service=reader_service,
        intake_service=intake_service,
        pipeline_service=pipeline_service,
    )
