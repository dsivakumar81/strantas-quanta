from __future__ import annotations

from quanta_api.domain.enums import EmailProvider, JobStatus
from quanta_api.domain.models import AlertEvent, BQMOutput, CarrierGroupRfp, CensusDataset, ConnectorCursor, IdempotencyRecord, InboundMailboxConfig, JobRecord, LOBRequest, QuoteRequest, ReplayAuditRecord, SubmissionEnvelope
from quanta_api.domain.repositories import CaseRepository, OperationsRepository, SubmissionRepository


class InMemorySubmissionRepository(SubmissionRepository):
    def __init__(self) -> None:
        self.submissions: dict[tuple[str, str], SubmissionEnvelope] = {}
        self.case_by_submission: dict[tuple[str, str], str] = {}
        self.fingerprints: dict[tuple[str, str], IdempotencyRecord] = {}

    def create(self, submission: SubmissionEnvelope) -> SubmissionEnvelope:
        self.submissions[(submission.tenant_id, submission.submission_id)] = submission
        return submission

    def get(self, submission_id: str, tenant_id: str | None = None) -> SubmissionEnvelope | None:
        if tenant_id is not None:
            return self.submissions.get((tenant_id, submission_id))
        for (stored_tenant, stored_submission_id), submission in self.submissions.items():
            if stored_submission_id == submission_id:
                return submission
        return None

    def update(self, submission: SubmissionEnvelope) -> SubmissionEnvelope:
        self.submissions[(submission.tenant_id, submission.submission_id)] = submission
        return submission

    def set_case_id(self, submission_id: str, case_id: str, tenant_id: str | None = None) -> None:
        tenant = tenant_id or self.get(submission_id).tenant_id
        self.case_by_submission[(tenant, submission_id)] = case_id

    def get_case_id(self, submission_id: str, tenant_id: str | None = None) -> str | None:
        if tenant_id is not None:
            return self.case_by_submission.get((tenant_id, submission_id))
        submission = self.get(submission_id)
        if submission is None:
            return None
        return self.case_by_submission.get((submission.tenant_id, submission_id))

    def get_by_fingerprint(self, tenant_id: str, fingerprint: str) -> IdempotencyRecord | None:
        return self.fingerprints.get((tenant_id, fingerprint))

    def save_fingerprint(self, record: IdempotencyRecord) -> IdempotencyRecord:
        self.fingerprints[(record.tenant_id, record.fingerprint)] = record
        return record


class InMemoryCaseRepository(CaseRepository):
    def __init__(self) -> None:
        self.quotes: dict[tuple[str, str], QuoteRequest] = {}
        self.lobs: dict[tuple[str, str], list[LOBRequest]] = {}
        self.census: dict[tuple[str, str], CensusDataset] = {}
        self.outputs: dict[tuple[str, str], BQMOutput] = {}
        self.carrier_outputs: dict[tuple[str, str], CarrierGroupRfp] = {}

    def save_quote(self, quote_request: QuoteRequest) -> QuoteRequest:
        self.quotes[(quote_request.tenant_id, quote_request.case_id)] = quote_request
        return quote_request

    def get_quote(self, case_id: str, tenant_id: str | None = None) -> QuoteRequest | None:
        if tenant_id is not None:
            return self.quotes.get((tenant_id, case_id))
        for (_tenant, stored_case_id), quote in self.quotes.items():
            if stored_case_id == case_id:
                return quote
        return None

    def save_lobs(self, case_id: str, lobs: list[LOBRequest]) -> list[LOBRequest]:
        tenant_id = lobs[0].tenant_id if lobs else "default"
        self.lobs[(tenant_id, case_id)] = lobs
        return lobs

    def get_lobs(self, case_id: str, tenant_id: str | None = None) -> list[LOBRequest]:
        if tenant_id is not None:
            return self.lobs.get((tenant_id, case_id), [])
        for (_tenant, stored_case_id), lobs in self.lobs.items():
            if stored_case_id == case_id:
                return lobs
        return []

    def save_census(self, census: CensusDataset) -> CensusDataset:
        self.census[(census.tenant_id, census.parent_case_id)] = census
        return census

    def get_census(self, case_id: str, tenant_id: str | None = None) -> CensusDataset | None:
        if tenant_id is not None:
            return self.census.get((tenant_id, case_id))
        for (_tenant, stored_case_id), census in self.census.items():
            if stored_case_id == case_id:
                return census
        return None

    def save_output(self, output: BQMOutput) -> BQMOutput:
        quote = next((quote for (_tenant, case_id), quote in self.quotes.items() if case_id == output.caseId), None)
        tenant_id = quote.tenant_id if quote else "default"
        self.outputs[(tenant_id, output.caseId)] = output
        return output

    def get_output(self, case_id: str, tenant_id: str | None = None) -> BQMOutput | None:
        if tenant_id is not None:
            return self.outputs.get((tenant_id, case_id))
        for (_tenant, stored_case_id), output in self.outputs.items():
            if stored_case_id == case_id:
                return output
        return None

    def save_carrier_output(self, output: CarrierGroupRfp, case_id: str, tenant_id: str) -> CarrierGroupRfp:
        self.carrier_outputs[(tenant_id, case_id)] = output
        return output

    def get_carrier_output(self, case_id: str, tenant_id: str | None = None) -> CarrierGroupRfp | None:
        if tenant_id is not None:
            return self.carrier_outputs.get((tenant_id, case_id))
        for (_tenant, stored_case_id), output in self.carrier_outputs.items():
            if stored_case_id == case_id:
                return output
        return None


class InMemoryOperationsRepository(OperationsRepository):
    def __init__(self) -> None:
        self.cursors: dict[tuple[str, EmailProvider], ConnectorCursor] = {}
        self.mailboxes: dict[tuple[str, EmailProvider], InboundMailboxConfig] = {}
        self.jobs: dict[str, JobRecord] = {}
        self.alerts: list[AlertEvent] = []
        self.replay_audits: list[ReplayAuditRecord] = []

    def save_cursor(self, cursor: ConnectorCursor) -> ConnectorCursor:
        self.cursors[(cursor.tenant_id, cursor.provider)] = cursor
        return cursor

    def get_cursor(self, provider: EmailProvider, tenant_id: str = "default") -> ConnectorCursor | None:
        return self.cursors.get((tenant_id, provider))

    def list_cursors(self, tenant_id: str | None = None) -> list[ConnectorCursor]:
        if tenant_id is None:
            return list(self.cursors.values())
        return [cursor for (stored_tenant, _provider), cursor in self.cursors.items() if stored_tenant == tenant_id]

    def save_mailbox(self, mailbox: InboundMailboxConfig) -> InboundMailboxConfig:
        self.mailboxes[(mailbox.tenant_id, mailbox.provider)] = mailbox
        return mailbox

    def get_mailbox(self, provider: EmailProvider, tenant_id: str = "default") -> InboundMailboxConfig | None:
        return self.mailboxes.get((tenant_id, provider))

    def list_mailboxes(self, tenant_id: str | None = None) -> list[InboundMailboxConfig]:
        if tenant_id is None:
            return list(self.mailboxes.values())
        return [mailbox for (stored_tenant, _provider), mailbox in self.mailboxes.items() if stored_tenant == tenant_id]

    def delete_mailbox(self, provider: EmailProvider, tenant_id: str = "default") -> None:
        self.mailboxes.pop((tenant_id, provider), None)

    def enqueue_job(self, job: JobRecord) -> JobRecord:
        self.jobs[job.job_id] = job
        return job

    def get_job_by_dedupe_key(self, dedupe_key: str, tenant_id: str) -> JobRecord | None:
        for job in self.jobs.values():
            if job.tenant_id == tenant_id and job.dedupe_key == dedupe_key:
                return job
        return None

    def get_job(self, job_id: str) -> JobRecord | None:
        return self.jobs.get(job_id)

    def update_job(self, job: JobRecord) -> JobRecord:
        self.jobs[job.job_id] = job
        return job

    def list_jobs(self, status: str | None = None, tenant_id: str | None = None) -> list[JobRecord]:
        jobs = list(self.jobs.values())
        if status is not None:
            jobs = [job for job in jobs if job.status.value == status]
        if tenant_id is not None:
            jobs = [job for job in jobs if job.tenant_id == tenant_id]
        return sorted(jobs, key=lambda job: job.available_at)

    def next_available_job(self, tenant_id: str | None = None) -> JobRecord | None:
        candidates = [job for job in self.jobs.values() if job.status == JobStatus.queued]
        if tenant_id is not None:
            candidates = [job for job in candidates if job.tenant_id == tenant_id]
        if not candidates:
            return None
        return sorted(candidates, key=lambda job: job.available_at)[0]

    def save_alert(self, alert: AlertEvent) -> AlertEvent:
        self.alerts.insert(0, alert)
        return alert

    def list_alerts(self, limit: int = 50, tenant_id: str | None = None) -> list[AlertEvent]:
        alerts = self.alerts
        if tenant_id is not None:
            alerts = [alert for alert in alerts if alert.tenant_id == tenant_id]
        return alerts[:limit]

    def save_replay_audit(self, audit: ReplayAuditRecord) -> ReplayAuditRecord:
        self.replay_audits.insert(0, audit)
        return audit

    def list_replay_audits(self, limit: int = 50, tenant_id: str | None = None) -> list[ReplayAuditRecord]:
        audits = self.replay_audits
        if tenant_id is not None:
            audits = [audit for audit in audits if audit.tenant_id == tenant_id]
        return audits[:limit]
