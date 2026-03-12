from __future__ import annotations

from quanta_api.domain.models import BQMOutput, CensusDataset, LOBRequest, QuoteRequest, SubmissionEnvelope
from quanta_api.domain.repositories import CaseRepository, SubmissionRepository


class InMemorySubmissionRepository(SubmissionRepository):
    def __init__(self) -> None:
        self.submissions: dict[str, SubmissionEnvelope] = {}
        self.case_by_submission: dict[str, str] = {}

    def create(self, submission: SubmissionEnvelope) -> SubmissionEnvelope:
        self.submissions[submission.submission_id] = submission
        return submission

    def get(self, submission_id: str) -> SubmissionEnvelope | None:
        return self.submissions.get(submission_id)

    def update(self, submission: SubmissionEnvelope) -> SubmissionEnvelope:
        self.submissions[submission.submission_id] = submission
        return submission

    def set_case_id(self, submission_id: str, case_id: str) -> None:
        self.case_by_submission[submission_id] = case_id

    def get_case_id(self, submission_id: str) -> str | None:
        return self.case_by_submission.get(submission_id)


class InMemoryCaseRepository(CaseRepository):
    def __init__(self) -> None:
        self.quotes: dict[str, QuoteRequest] = {}
        self.lobs: dict[str, list[LOBRequest]] = {}
        self.census: dict[str, CensusDataset] = {}
        self.outputs: dict[str, BQMOutput] = {}

    def save_quote(self, quote_request: QuoteRequest) -> QuoteRequest:
        self.quotes[quote_request.case_id] = quote_request
        return quote_request

    def get_quote(self, case_id: str) -> QuoteRequest | None:
        return self.quotes.get(case_id)

    def save_lobs(self, case_id: str, lobs: list[LOBRequest]) -> list[LOBRequest]:
        self.lobs[case_id] = lobs
        return lobs

    def get_lobs(self, case_id: str) -> list[LOBRequest]:
        return self.lobs.get(case_id, [])

    def save_census(self, census: CensusDataset) -> CensusDataset:
        self.census[census.parent_case_id] = census
        return census

    def get_census(self, case_id: str) -> CensusDataset | None:
        return self.census.get(case_id)

    def save_output(self, output: BQMOutput) -> BQMOutput:
        self.outputs[output.caseId] = output
        return output

    def get_output(self, case_id: str) -> BQMOutput | None:
        return self.outputs.get(case_id)
