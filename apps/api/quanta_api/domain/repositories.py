from __future__ import annotations

from abc import ABC, abstractmethod

from quanta_api.domain.models import BQMOutput, CensusDataset, LOBRequest, QuoteRequest, SubmissionEnvelope


class SubmissionRepository(ABC):
    @abstractmethod
    def create(self, submission: SubmissionEnvelope) -> SubmissionEnvelope: ...

    @abstractmethod
    def get(self, submission_id: str) -> SubmissionEnvelope | None: ...

    @abstractmethod
    def update(self, submission: SubmissionEnvelope) -> SubmissionEnvelope: ...

    @abstractmethod
    def set_case_id(self, submission_id: str, case_id: str) -> None: ...

    @abstractmethod
    def get_case_id(self, submission_id: str) -> str | None: ...


class CaseRepository(ABC):
    @abstractmethod
    def save_quote(self, quote_request: QuoteRequest) -> QuoteRequest: ...

    @abstractmethod
    def get_quote(self, case_id: str) -> QuoteRequest | None: ...

    @abstractmethod
    def save_lobs(self, case_id: str, lobs: list[LOBRequest]) -> list[LOBRequest]: ...

    @abstractmethod
    def get_lobs(self, case_id: str) -> list[LOBRequest]: ...

    @abstractmethod
    def save_census(self, census: CensusDataset) -> CensusDataset: ...

    @abstractmethod
    def get_census(self, case_id: str) -> CensusDataset | None: ...

    @abstractmethod
    def save_output(self, output: BQMOutput) -> BQMOutput: ...

    @abstractmethod
    def get_output(self, case_id: str) -> BQMOutput | None: ...


class ObjectStore(ABC):
    @abstractmethod
    def put_bytes(self, storage_key: str, content: bytes, content_type: str | None = None) -> str: ...

    @abstractmethod
    def get_bytes(self, storage_key: str) -> bytes: ...
