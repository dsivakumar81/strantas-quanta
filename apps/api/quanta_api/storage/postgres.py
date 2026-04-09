from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime, Index, String, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from quanta_api.domain.enums import EmailProvider, JobStatus
from quanta_api.domain.models import AlertEvent, BQMOutput, CarrierGroupRfp, CensusDataset, ConnectorCursor, IdempotencyRecord, InboundMailboxConfig, JobRecord, LOBRequest, QuoteRequest, ReplayAuditRecord, SubmissionEnvelope
from quanta_api.domain.repositories import CaseRepository, OperationsRepository, SubmissionRepository


class Base(DeclarativeBase):
    pass


class SubmissionRecord(Base):
    __tablename__ = "submissions"
    __table_args__ = (
        Index("ix_submissions_tenant_submission", "tenant_id", "submission_id", unique=True),
        Index("ix_submissions_tenant_case", "tenant_id", "case_id"),
    )

    submission_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True, default="default")
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    case_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class IdempotencyRecordTable(Base):
    __tablename__ = "idempotency_keys"

    tenant_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    fingerprint: Mapped[str] = mapped_column(String(128), primary_key=True)
    submission_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class QuoteRecord(Base):
    __tablename__ = "quotes"
    __table_args__ = (Index("ix_quotes_tenant_case", "tenant_id", "case_id", unique=True),)

    case_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True, default="default")
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class LobRecord(Base):
    __tablename__ = "lobs"
    __table_args__ = (Index("ix_lobs_tenant_case", "tenant_id", "case_id"),)

    lob_case_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True, default="default")
    case_id: Mapped[str] = mapped_column(String(32), index=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class CensusRecord(Base):
    __tablename__ = "census"
    __table_args__ = (Index("ix_census_tenant_case", "tenant_id", "case_id", unique=True),)

    census_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True, default="default")
    case_id: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class OutputRecord(Base):
    __tablename__ = "outputs"
    __table_args__ = (Index("ix_outputs_tenant_case", "tenant_id", "case_id", unique=True),)

    case_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True, default="default")
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class CarrierOutputRecord(Base):
    __tablename__ = "carrier_outputs"
    __table_args__ = (Index("ix_carrier_outputs_tenant_case", "tenant_id", "case_id", unique=True),)

    case_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True, default="default")
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class ConnectorCursorRecord(Base):
    __tablename__ = "connector_cursors"
    __table_args__ = (Index("ix_connector_cursors_tenant_provider", "tenant_id", "provider", unique=True),)

    provider: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), primary_key=True, index=True, default="default")
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class InboundMailboxRecord(Base):
    __tablename__ = "inbound_mailboxes"
    __table_args__ = (Index("ix_inbound_mailboxes_tenant_provider", "tenant_id", "provider", unique=True),)

    provider: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), primary_key=True, index=True, default="default")
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class JobRecordTable(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        Index("ix_jobs_tenant_status", "tenant_id", "status"),
        Index("ix_jobs_tenant_dedupe", "tenant_id", "dedupe_key"),
    )

    job_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True, default="default")
    status: Mapped[str] = mapped_column(String(32), index=True)
    dedupe_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class AlertRecord(Base):
    __tablename__ = "alerts"
    __table_args__ = (Index("ix_alerts_tenant_created_at", "tenant_id", "created_at"),)

    alert_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True, default="default")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class ReplayAuditTable(Base):
    __tablename__ = "replay_audits"
    __table_args__ = (Index("ix_replay_audits_tenant_created_at", "tenant_id", "created_at"),)

    audit_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True, default="default")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class PostgresSubmissionRepository(SubmissionRepository):
    def __init__(self, database_url: str) -> None:
        self.engine = create_engine(database_url, future=True)

    def create(self, submission: SubmissionEnvelope) -> SubmissionEnvelope:
        with Session(self.engine) as session:
            session.merge(
                SubmissionRecord(
                    submission_id=submission.submission_id,
                    tenant_id=submission.tenant_id,
                    payload=submission.model_dump(mode="json"),
                )
            )
            session.commit()
        return submission

    def get(self, submission_id: str, tenant_id: str | None = None) -> SubmissionEnvelope | None:
        with Session(self.engine) as session:
            statement = select(SubmissionRecord).where(SubmissionRecord.submission_id == submission_id)
            if tenant_id is not None:
                statement = statement.where(SubmissionRecord.tenant_id == tenant_id)
            record = session.scalar(statement)
            return SubmissionEnvelope.model_validate(record.payload) if record else None

    def update(self, submission: SubmissionEnvelope) -> SubmissionEnvelope:
        with Session(self.engine) as session:
            existing = session.scalar(
                select(SubmissionRecord).where(
                    SubmissionRecord.submission_id == submission.submission_id,
                    SubmissionRecord.tenant_id == submission.tenant_id,
                )
            )
            case_id = existing.case_id if existing else None
            session.merge(
                SubmissionRecord(
                    submission_id=submission.submission_id,
                    tenant_id=submission.tenant_id,
                    payload=submission.model_dump(mode="json"),
                    case_id=case_id,
                    updated_at=datetime.now(timezone.utc),
                )
            )
            session.commit()
        return submission

    def set_case_id(self, submission_id: str, case_id: str, tenant_id: str | None = None) -> None:
        with Session(self.engine) as session:
            statement = select(SubmissionRecord).where(SubmissionRecord.submission_id == submission_id)
            if tenant_id is not None:
                statement = statement.where(SubmissionRecord.tenant_id == tenant_id)
            record = session.scalar(statement)
            if record:
                record.case_id = case_id
                record.updated_at = datetime.now(timezone.utc)
                session.commit()

    def get_case_id(self, submission_id: str, tenant_id: str | None = None) -> str | None:
        with Session(self.engine) as session:
            statement = select(SubmissionRecord).where(SubmissionRecord.submission_id == submission_id)
            if tenant_id is not None:
                statement = statement.where(SubmissionRecord.tenant_id == tenant_id)
            record = session.scalar(statement)
            return record.case_id if record else None

    def get_by_fingerprint(self, tenant_id: str, fingerprint: str) -> IdempotencyRecord | None:
        with Session(self.engine) as session:
            row = session.get(IdempotencyRecordTable, {"tenant_id": tenant_id, "fingerprint": fingerprint})
            if row is None:
                return None
            return IdempotencyRecord(
                tenant_id=row.tenant_id,
                fingerprint=row.fingerprint,
                submission_id=row.submission_id,
                created_at=row.created_at,
            )

    def save_fingerprint(self, record: IdempotencyRecord) -> IdempotencyRecord:
        with Session(self.engine) as session:
            session.merge(
                IdempotencyRecordTable(
                    tenant_id=record.tenant_id,
                    fingerprint=record.fingerprint,
                    submission_id=record.submission_id,
                    created_at=record.created_at,
                )
            )
            session.commit()
        return record


class PostgresCaseRepository(CaseRepository):
    def __init__(self, database_url: str) -> None:
        self.engine = create_engine(database_url, future=True)

    def save_quote(self, quote_request: QuoteRequest) -> QuoteRequest:
        with Session(self.engine) as session:
            session.merge(
                QuoteRecord(
                    case_id=quote_request.case_id,
                    tenant_id=quote_request.tenant_id,
                    payload=quote_request.model_dump(mode="json"),
                )
            )
            session.commit()
        return quote_request

    def get_quote(self, case_id: str, tenant_id: str | None = None) -> QuoteRequest | None:
        with Session(self.engine) as session:
            statement = select(QuoteRecord).where(QuoteRecord.case_id == case_id)
            if tenant_id is not None:
                statement = statement.where(QuoteRecord.tenant_id == tenant_id)
            record = session.scalar(statement)
            return QuoteRequest.model_validate(record.payload) if record else None

    def save_lobs(self, case_id: str, lobs: list[LOBRequest]) -> list[LOBRequest]:
        with Session(self.engine) as session:
            for lob in lobs:
                session.merge(
                    LobRecord(
                        lob_case_id=lob.lob_case_id,
                        tenant_id=lob.tenant_id,
                        case_id=case_id,
                        payload=lob.model_dump(mode="json"),
                    )
                )
            session.commit()
        return lobs

    def get_lobs(self, case_id: str, tenant_id: str | None = None) -> list[LOBRequest]:
        with Session(self.engine) as session:
            statement = select(LobRecord).where(LobRecord.case_id == case_id)
            if tenant_id is not None:
                statement = statement.where(LobRecord.tenant_id == tenant_id)
            rows = session.scalars(statement).all()
            return [LOBRequest.model_validate(row.payload) for row in rows]

    def save_census(self, census: CensusDataset) -> CensusDataset:
        with Session(self.engine) as session:
            session.merge(
                CensusRecord(
                    census_id=census.census_id,
                    tenant_id=census.tenant_id,
                    case_id=census.parent_case_id,
                    payload=census.model_dump(mode="json"),
                )
            )
            session.commit()
        return census

    def get_census(self, case_id: str, tenant_id: str | None = None) -> CensusDataset | None:
        with Session(self.engine) as session:
            statement = select(CensusRecord).where(CensusRecord.case_id == case_id)
            if tenant_id is not None:
                statement = statement.where(CensusRecord.tenant_id == tenant_id)
            row = session.scalar(statement)
            return CensusDataset.model_validate(row.payload) if row else None

    def save_output(self, output: BQMOutput) -> BQMOutput:
        quote = self.get_quote(output.caseId)
        tenant_id = quote.tenant_id if quote else "default"
        with Session(self.engine) as session:
            session.merge(OutputRecord(case_id=output.caseId, tenant_id=tenant_id, payload=output.model_dump(mode="json")))
            session.commit()
        return output

    def get_output(self, case_id: str, tenant_id: str | None = None) -> BQMOutput | None:
        with Session(self.engine) as session:
            statement = select(OutputRecord).where(OutputRecord.case_id == case_id)
            if tenant_id is not None:
                statement = statement.where(OutputRecord.tenant_id == tenant_id)
            row = session.scalar(statement)
            return BQMOutput.model_validate(row.payload) if row else None

    def save_carrier_output(self, output: CarrierGroupRfp, case_id: str, tenant_id: str) -> CarrierGroupRfp:
        with Session(self.engine) as session:
            session.merge(CarrierOutputRecord(case_id=case_id, tenant_id=tenant_id, payload=output.model_dump(mode="json")))
            session.commit()
        return output

    def get_carrier_output(self, case_id: str, tenant_id: str | None = None) -> CarrierGroupRfp | None:
        with Session(self.engine) as session:
            statement = select(CarrierOutputRecord).where(CarrierOutputRecord.case_id == case_id)
            if tenant_id is not None:
                statement = statement.where(CarrierOutputRecord.tenant_id == tenant_id)
            row = session.scalar(statement)
            return CarrierGroupRfp.model_validate(row.payload) if row else None


class PostgresOperationsRepository(OperationsRepository):
    def __init__(self, database_url: str) -> None:
        self.engine = create_engine(database_url, future=True)

    def save_cursor(self, cursor: ConnectorCursor) -> ConnectorCursor:
        with Session(self.engine) as session:
            session.merge(ConnectorCursorRecord(provider=cursor.provider.value, tenant_id=cursor.tenant_id, payload=cursor.model_dump(mode="json")))
            session.commit()
        return cursor

    def get_cursor(self, provider: EmailProvider, tenant_id: str = "default") -> ConnectorCursor | None:
        with Session(self.engine) as session:
            row = session.scalar(
                select(ConnectorCursorRecord).where(
                    ConnectorCursorRecord.provider == provider.value,
                    ConnectorCursorRecord.tenant_id == tenant_id,
                )
            )
            return ConnectorCursor.model_validate(row.payload) if row else None

    def list_cursors(self, tenant_id: str | None = None) -> list[ConnectorCursor]:
        with Session(self.engine) as session:
            statement = select(ConnectorCursorRecord)
            if tenant_id is not None:
                statement = statement.where(ConnectorCursorRecord.tenant_id == tenant_id)
            rows = session.scalars(statement).all()
            return [ConnectorCursor.model_validate(row.payload) for row in rows]

    def save_mailbox(self, mailbox: InboundMailboxConfig) -> InboundMailboxConfig:
        with Session(self.engine) as session:
            session.merge(InboundMailboxRecord(provider=mailbox.provider.value, tenant_id=mailbox.tenant_id, payload=mailbox.model_dump(mode="json")))
            session.commit()
        return mailbox

    def get_mailbox(self, provider: EmailProvider, tenant_id: str = "default") -> InboundMailboxConfig | None:
        with Session(self.engine) as session:
            row = session.scalar(
                select(InboundMailboxRecord).where(
                    InboundMailboxRecord.provider == provider.value,
                    InboundMailboxRecord.tenant_id == tenant_id,
                )
            )
            return InboundMailboxConfig.model_validate(row.payload) if row else None

    def list_mailboxes(self, tenant_id: str | None = None) -> list[InboundMailboxConfig]:
        with Session(self.engine) as session:
            statement = select(InboundMailboxRecord)
            if tenant_id is not None:
                statement = statement.where(InboundMailboxRecord.tenant_id == tenant_id)
            rows = session.scalars(statement).all()
            return [InboundMailboxConfig.model_validate(row.payload) for row in rows]

    def delete_mailbox(self, provider: EmailProvider, tenant_id: str = "default") -> None:
        with Session(self.engine) as session:
            row = session.scalar(
                select(InboundMailboxRecord).where(
                    InboundMailboxRecord.provider == provider.value,
                    InboundMailboxRecord.tenant_id == tenant_id,
                )
            )
            if row is not None:
                session.delete(row)
                session.commit()

    def enqueue_job(self, job: JobRecord) -> JobRecord:
        with Session(self.engine) as session:
            session.merge(
                JobRecordTable(
                    job_id=job.job_id,
                    tenant_id=job.tenant_id,
                    status=job.status.value,
                    dedupe_key=job.dedupe_key,
                    available_at=job.available_at,
                    payload=job.model_dump(mode="json"),
                )
            )
            session.commit()
        return job

    def get_job_by_dedupe_key(self, dedupe_key: str, tenant_id: str) -> JobRecord | None:
        with Session(self.engine) as session:
            row = session.scalar(
                select(JobRecordTable).where(
                    JobRecordTable.tenant_id == tenant_id,
                    JobRecordTable.dedupe_key == dedupe_key,
                )
            )
            return JobRecord.model_validate(row.payload) if row else None

    def get_job(self, job_id: str) -> JobRecord | None:
        with Session(self.engine) as session:
            row = session.get(JobRecordTable, job_id)
            return JobRecord.model_validate(row.payload) if row else None

    def update_job(self, job: JobRecord) -> JobRecord:
        with Session(self.engine) as session:
            session.merge(
                JobRecordTable(
                    job_id=job.job_id,
                    tenant_id=job.tenant_id,
                    status=job.status.value,
                    dedupe_key=job.dedupe_key,
                    available_at=job.available_at,
                    payload=job.model_dump(mode="json"),
                )
            )
            session.commit()
        return job

    def list_jobs(self, status: str | None = None, tenant_id: str | None = None) -> list[JobRecord]:
        with Session(self.engine) as session:
            statement = select(JobRecordTable)
            if status is not None:
                statement = statement.where(JobRecordTable.status == status)
            if tenant_id is not None:
                statement = statement.where(JobRecordTable.tenant_id == tenant_id)
            rows = session.scalars(statement.order_by(JobRecordTable.available_at)).all()
            return [JobRecord.model_validate(row.payload) for row in rows]

    def next_available_job(self, tenant_id: str | None = None) -> JobRecord | None:
        with Session(self.engine) as session:
            statement = (
                select(JobRecordTable)
                .where(JobRecordTable.status == JobStatus.queued.value)
                .order_by(JobRecordTable.available_at)
                .limit(1)
            )
            if tenant_id is not None:
                statement = statement.where(JobRecordTable.tenant_id == tenant_id)
            row = session.scalar(statement)
            return JobRecord.model_validate(row.payload) if row else None

    def save_alert(self, alert: AlertEvent) -> AlertEvent:
        with Session(self.engine) as session:
            session.merge(
                AlertRecord(
                    alert_id=alert.alert_id,
                    tenant_id=alert.tenant_id,
                    created_at=alert.created_at,
                    payload=alert.model_dump(mode="json"),
                )
            )
            session.commit()
        return alert

    def list_alerts(self, limit: int = 50, tenant_id: str | None = None) -> list[AlertEvent]:
        with Session(self.engine) as session:
            statement = select(AlertRecord).order_by(AlertRecord.created_at.desc()).limit(limit)
            if tenant_id is not None:
                statement = statement.where(AlertRecord.tenant_id == tenant_id)
            rows = session.scalars(statement).all()
            return [AlertEvent.model_validate(row.payload) for row in rows]

    def save_replay_audit(self, audit: ReplayAuditRecord) -> ReplayAuditRecord:
        with Session(self.engine) as session:
            session.merge(
                ReplayAuditTable(
                    audit_id=audit.audit_id,
                    tenant_id=audit.tenant_id,
                    created_at=audit.created_at,
                    payload=audit.model_dump(mode="json"),
                )
            )
            session.commit()
        return audit

    def list_replay_audits(self, limit: int = 50, tenant_id: str | None = None) -> list[ReplayAuditRecord]:
        with Session(self.engine) as session:
            statement = select(ReplayAuditTable).order_by(ReplayAuditTable.created_at.desc()).limit(limit)
            if tenant_id is not None:
                statement = statement.where(ReplayAuditTable.tenant_id == tenant_id)
            rows = session.scalars(statement).all()
            return [ReplayAuditRecord.model_validate(row.payload) for row in rows]


def create_all_tables(database_url: str) -> None:
    engine = create_engine(database_url, future=True)
    Base.metadata.create_all(engine)
