from __future__ import annotations

from abc import ABC, abstractmethod

from quanta_api.domain.enums import EmailProvider
from quanta_api.domain.models import AlertEvent, BQMOutput, CarrierGroupRfp, CensusDataset, ConnectorCursor, IdempotencyRecord, InboundMailboxConfig, JobRecord, LOBRequest, QuoteRequest, ReplayAuditRecord, SubmissionEnvelope


class SubmissionRepository(ABC):
    @abstractmethod
    def create(self, submission: SubmissionEnvelope) -> SubmissionEnvelope: ...

    @abstractmethod
    def get(self, submission_id: str, tenant_id: str | None = None) -> SubmissionEnvelope | None: ...

    @abstractmethod
    def update(self, submission: SubmissionEnvelope) -> SubmissionEnvelope: ...

    @abstractmethod
    def set_case_id(self, submission_id: str, case_id: str, tenant_id: str | None = None) -> None: ...

    @abstractmethod
    def get_case_id(self, submission_id: str, tenant_id: str | None = None) -> str | None: ...

    @abstractmethod
    def get_by_fingerprint(self, tenant_id: str, fingerprint: str) -> IdempotencyRecord | None: ...

    @abstractmethod
    def save_fingerprint(self, record: IdempotencyRecord) -> IdempotencyRecord: ...


class CaseRepository(ABC):
    @abstractmethod
    def save_quote(self, quote_request: QuoteRequest) -> QuoteRequest: ...

    @abstractmethod
    def get_quote(self, case_id: str, tenant_id: str | None = None) -> QuoteRequest | None: ...

    @abstractmethod
    def save_lobs(self, case_id: str, lobs: list[LOBRequest]) -> list[LOBRequest]: ...

    @abstractmethod
    def get_lobs(self, case_id: str, tenant_id: str | None = None) -> list[LOBRequest]: ...

    @abstractmethod
    def save_census(self, census: CensusDataset) -> CensusDataset: ...

    @abstractmethod
    def get_census(self, case_id: str, tenant_id: str | None = None) -> CensusDataset | None: ...

    @abstractmethod
    def save_output(self, output: BQMOutput) -> BQMOutput: ...

    @abstractmethod
    def get_output(self, case_id: str, tenant_id: str | None = None) -> BQMOutput | None: ...

    @abstractmethod
    def save_carrier_output(self, output: CarrierGroupRfp, case_id: str, tenant_id: str) -> CarrierGroupRfp: ...

    @abstractmethod
    def get_carrier_output(self, case_id: str, tenant_id: str | None = None) -> CarrierGroupRfp | None: ...


class ObjectStore(ABC):
    @abstractmethod
    def put_bytes(self, storage_key: str, content: bytes, content_type: str | None = None) -> str: ...

    @abstractmethod
    def get_bytes(self, storage_key: str) -> bytes: ...


class OperationsRepository(ABC):
    @abstractmethod
    def save_cursor(self, cursor: ConnectorCursor) -> ConnectorCursor: ...

    @abstractmethod
    def get_cursor(self, provider: EmailProvider, tenant_id: str = "default") -> ConnectorCursor | None: ...

    @abstractmethod
    def list_cursors(self, tenant_id: str | None = None) -> list[ConnectorCursor]: ...

    @abstractmethod
    def save_mailbox(self, mailbox: InboundMailboxConfig) -> InboundMailboxConfig: ...

    @abstractmethod
    def get_mailbox(self, provider: EmailProvider, tenant_id: str = "default") -> InboundMailboxConfig | None: ...

    @abstractmethod
    def list_mailboxes(self, tenant_id: str | None = None) -> list[InboundMailboxConfig]: ...

    @abstractmethod
    def delete_mailbox(self, provider: EmailProvider, tenant_id: str = "default") -> None: ...

    @abstractmethod
    def enqueue_job(self, job: JobRecord) -> JobRecord: ...

    @abstractmethod
    def get_job_by_dedupe_key(self, dedupe_key: str, tenant_id: str) -> JobRecord | None: ...

    @abstractmethod
    def get_job(self, job_id: str) -> JobRecord | None: ...

    @abstractmethod
    def update_job(self, job: JobRecord) -> JobRecord: ...

    @abstractmethod
    def list_jobs(self, status: str | None = None, tenant_id: str | None = None) -> list[JobRecord]: ...

    @abstractmethod
    def next_available_job(self, tenant_id: str | None = None) -> JobRecord | None: ...

    @abstractmethod
    def save_alert(self, alert: AlertEvent) -> AlertEvent: ...

    @abstractmethod
    def list_alerts(self, limit: int = 50, tenant_id: str | None = None) -> list[AlertEvent]: ...

    @abstractmethod
    def save_replay_audit(self, audit: ReplayAuditRecord) -> ReplayAuditRecord: ...

    @abstractmethod
    def list_replay_audits(self, limit: int = 50, tenant_id: str | None = None) -> list[ReplayAuditRecord]: ...
