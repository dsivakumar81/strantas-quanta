from __future__ import annotations

from dataclasses import dataclass

from quanta_api.domain.repositories import CaseRepository, ObjectStore, SubmissionRepository
from quanta_api.services.attachment_intelligence import AttachmentIntelligenceService
from quanta_api.services.census_extractor import CensusExtractionService
from quanta_api.services.bqm_validator import BQMValidationService
from quanta_api.services.email_adapters import EmailAdapterService
from quanta_api.services.id_factory import IdFactory
from quanta_api.services.intake import IntakeService
from quanta_api.services.normalizer import NormalizationService
from quanta_api.services.pipeline import PipelineService
from quanta_api.services.provider_connectors import ProviderConnectorService
from quanta_api.services.reader import ReaderService


@dataclass
class ServiceContainer:
    ids: IdFactory
    submission_repository: SubmissionRepository
    case_repository: CaseRepository
    object_store: ObjectStore
    provider_connector: ProviderConnectorService
    email_adapter: EmailAdapterService
    attachment_intelligence: AttachmentIntelligenceService
    census_extractor: CensusExtractionService
    bqm_validator: BQMValidationService
    normalization_service: NormalizationService
    reader_service: ReaderService
    intake_service: IntakeService
    pipeline_service: PipelineService
