from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime, String, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from quanta_api.domain.models import BQMOutput, CensusDataset, LOBRequest, QuoteRequest, SubmissionEnvelope
from quanta_api.domain.repositories import CaseRepository, SubmissionRepository


class Base(DeclarativeBase):
    pass


class SubmissionRecord(Base):
    __tablename__ = "submissions"

    submission_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    case_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class QuoteRecord(Base):
    __tablename__ = "quotes"

    case_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class LobRecord(Base):
    __tablename__ = "lobs"

    lob_case_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    case_id: Mapped[str] = mapped_column(String(32), index=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class CensusRecord(Base):
    __tablename__ = "census"

    census_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    case_id: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class OutputRecord(Base):
    __tablename__ = "outputs"

    case_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


class PostgresSubmissionRepository(SubmissionRepository):
    def __init__(self, database_url: str) -> None:
        self.engine = create_engine(database_url, future=True)

    def create(self, submission: SubmissionEnvelope) -> SubmissionEnvelope:
        with Session(self.engine) as session:
            session.merge(SubmissionRecord(submission_id=submission.submission_id, payload=submission.model_dump(mode="json")))
            session.commit()
        return submission

    def get(self, submission_id: str) -> SubmissionEnvelope | None:
        with Session(self.engine) as session:
            record = session.get(SubmissionRecord, submission_id)
            return SubmissionEnvelope.model_validate(record.payload) if record else None

    def update(self, submission: SubmissionEnvelope) -> SubmissionEnvelope:
        with Session(self.engine) as session:
            existing = session.get(SubmissionRecord, submission.submission_id)
            case_id = existing.case_id if existing else None
            session.merge(
                SubmissionRecord(
                    submission_id=submission.submission_id,
                    payload=submission.model_dump(mode="json"),
                    case_id=case_id,
                    updated_at=datetime.now(timezone.utc),
                )
            )
            session.commit()
        return submission

    def set_case_id(self, submission_id: str, case_id: str) -> None:
        with Session(self.engine) as session:
            record = session.get(SubmissionRecord, submission_id)
            if record:
                record.case_id = case_id
                record.updated_at = datetime.now(timezone.utc)
                session.commit()

    def get_case_id(self, submission_id: str) -> str | None:
        with Session(self.engine) as session:
            record = session.get(SubmissionRecord, submission_id)
            return record.case_id if record else None


class PostgresCaseRepository(CaseRepository):
    def __init__(self, database_url: str) -> None:
        self.engine = create_engine(database_url, future=True)

    def save_quote(self, quote_request: QuoteRequest) -> QuoteRequest:
        with Session(self.engine) as session:
            session.merge(QuoteRecord(case_id=quote_request.case_id, payload=quote_request.model_dump(mode="json")))
            session.commit()
        return quote_request

    def get_quote(self, case_id: str) -> QuoteRequest | None:
        with Session(self.engine) as session:
            record = session.get(QuoteRecord, case_id)
            return QuoteRequest.model_validate(record.payload) if record else None

    def save_lobs(self, case_id: str, lobs: list[LOBRequest]) -> list[LOBRequest]:
        with Session(self.engine) as session:
            for lob in lobs:
                session.merge(LobRecord(lob_case_id=lob.lob_case_id, case_id=case_id, payload=lob.model_dump(mode="json")))
            session.commit()
        return lobs

    def get_lobs(self, case_id: str) -> list[LOBRequest]:
        with Session(self.engine) as session:
            rows = session.scalars(select(LobRecord).where(LobRecord.case_id == case_id)).all()
            return [LOBRequest.model_validate(row.payload) for row in rows]

    def save_census(self, census: CensusDataset) -> CensusDataset:
        with Session(self.engine) as session:
            session.merge(
                CensusRecord(
                    census_id=census.census_id,
                    case_id=census.parent_case_id,
                    payload=census.model_dump(mode="json"),
                )
            )
            session.commit()
        return census

    def get_census(self, case_id: str) -> CensusDataset | None:
        with Session(self.engine) as session:
            row = session.scalar(select(CensusRecord).where(CensusRecord.case_id == case_id))
            return CensusDataset.model_validate(row.payload) if row else None

    def save_output(self, output: BQMOutput) -> BQMOutput:
        with Session(self.engine) as session:
            session.merge(OutputRecord(case_id=output.caseId, payload=output.model_dump(mode="json")))
            session.commit()
        return output

    def get_output(self, case_id: str) -> BQMOutput | None:
        with Session(self.engine) as session:
            row = session.get(OutputRecord, case_id)
            return BQMOutput.model_validate(row.payload) if row else None


def create_all_tables(database_url: str) -> None:
    engine = create_engine(database_url, future=True)
    Base.metadata.create_all(engine)
