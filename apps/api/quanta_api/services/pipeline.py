from __future__ import annotations

import re
from datetime import datetime

from quanta_api.domain.enums import ProcessingStatus, QuoteType
from quanta_api.domain.models import BQMConfidence, BQMCensusOutput, BQMLobOutput, BQMOutput, Broker, CensusDataset, Employer, EvidenceReference, LOBRequest, PlanDesign, QuoteRequest
from quanta_api.domain.repositories import CaseRepository, SubmissionRepository
from quanta_api.services.attachment_intelligence import AttachmentIntelligenceService
from quanta_api.services.bqm_validator import BQMValidationService
from quanta_api.services.census_extractor import CensusExtractionService
from quanta_api.services.id_factory import IdFactory
from quanta_api.services.normalizer import NormalizationService

LOB_PATTERNS = {
    "group_life": ["life", "basic life", "vol life", "voluntary life"],
    "group_std": ["std", "short term disability", "short-term disability"],
    "group_ltd": ["ltd", "long term disability", "long-term disability"],
    "supplemental_ci": ["critical illness", "ci"],
    "supplemental_accident": ["accident"],
    "supplemental_hi": ["hospital indemnity", "hi"],
    "dental": ["dental"],
    "vision": ["vision"],
}

STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
    "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT",
    "VA", "WA", "WV", "WI", "WY",
}


class PipelineService:
    def __init__(
        self,
        submission_repository: SubmissionRepository,
        case_repository: CaseRepository,
        attachment_intelligence: AttachmentIntelligenceService,
        census_extractor: CensusExtractionService,
        bqm_validator: BQMValidationService,
        normalization_service: NormalizationService,
        ids: IdFactory,
    ) -> None:
        self.submission_repository = submission_repository
        self.case_repository = case_repository
        self.attachment_intelligence = attachment_intelligence
        self.census_extractor = census_extractor
        self.bqm_validator = bqm_validator
        self.normalization_service = normalization_service
        self.ids = ids

    def parse_submission(self, submission_id: str):
        submission = self.submission_repository.get(submission_id)
        if submission is None:
            raise KeyError(submission_id)
        submission.processing_status = ProcessingStatus.parsed
        return self.submission_repository.update(submission)

    def run_extraction(self, submission_id: str):
        submission = self.submission_repository.get(submission_id)
        if submission is None:
            raise KeyError(submission_id)
        body = f"{submission.subject}\n{submission.email_body_text}".lower()
        detected_lobs = [lob for lob, patterns in LOB_PATTERNS.items() if any(p in body for p in patterns)]
        attachment_insight = self.attachment_intelligence.analyze(submission.attachments)
        detected_lobs = sorted(set(detected_lobs).union(attachment_insight.detected_lobs))
        if not detected_lobs:
            detected_lobs = ["group_life"]

        case_id = self.submission_repository.get_case_id(submission_id)
        if case_id is None:
            case_id = self.ids.next_case_id()
            self.submission_repository.set_case_id(submission_id, case_id)

        core_fields = self.normalization_service.resolve_core_fields(
            email_fields={
                "employer_name": self._extract_employer_name(submission.email_body_text),
                "broker_name": self._extract_broker_name(submission.email_body_text),
                "effective_date": self._extract_effective_date(submission.email_body_text),
                "situs_state": self._extract_state(submission.email_body_text),
            },
            attachment_insight=attachment_insight,
            parsers={
                "employer_name": lambda value: value,
                "broker_name": lambda value: value,
                "effective_date": self._extract_effective_date,
                "situs_state": self._extract_state,
            },
        )
        quote_evidence = self._build_quote_evidence(submission)
        quote_evidence.extend(core_fields.evidence)
        for lob_type in detected_lobs:
            quote_evidence.extend(attachment_insight.lob_evidence.get(lob_type, []))
        warnings = list(core_fields.warnings)
        if core_fields.effective_date is None:
            warnings.append("Effective date not explicitly found; review required")
        if core_fields.situs_state is None:
            warnings.append("Situs state not explicitly found; review required")

        quote = QuoteRequest(
            case_id=case_id,
            submission_id=submission_id,
            quote_type=QuoteType.new_business,
            employer_name=core_fields.employer_name,
            broker_name=core_fields.broker_name,
            effective_date=core_fields.effective_date,
            situs_state=core_fields.situs_state,
            requested_lobs=detected_lobs,
            overall_status=ProcessingStatus.extracted,
            extraction_confidence=0.72,
            warnings=warnings.copy(),
            evidence_references=quote_evidence,
        )

        lob_requests = [
            self.normalization_service.normalize_lob_request(self._build_lob_request(case_id, lob, attachment_insight))
            for lob in detected_lobs
        ]
        census = self.census_extractor.extract(case_id, submission.attachments)
        quote = self.normalization_service.normalize_quote_request(quote)

        self.case_repository.save_quote(quote)
        self.case_repository.save_lobs(case_id, lob_requests)
        self.case_repository.save_census(census)
        submission.processing_status = ProcessingStatus.extracted
        self.submission_repository.update(submission)
        return quote, lob_requests, census

    def run_normalization(self, submission_id: str):
        case_id = self.submission_repository.get_case_id(submission_id)
        if case_id is None:
            quote, lobs, census = self.run_extraction(submission_id)
        else:
            quote = self.case_repository.get_quote(case_id)
            lobs = self.case_repository.get_lobs(case_id)
            census = self.case_repository.get_census(case_id)
            if quote is None or census is None:
                quote, lobs, census = self.run_extraction(submission_id)

        quote.overall_status = ProcessingStatus.normalized
        self.case_repository.save_quote(quote)
        self.case_repository.save_output(self._build_output(quote, lobs, census))
        submission = self.submission_repository.get(submission_id)
        if submission is None:
            raise KeyError(submission_id)
        submission.processing_status = ProcessingStatus.normalized
        self.submission_repository.update(submission)
        return quote, lobs, census

    def get_output(self, case_id: str) -> BQMOutput:
        output = self.case_repository.get_output(case_id)
        if output is not None:
            return output
        quote = self.case_repository.get_quote(case_id)
        census = self.case_repository.get_census(case_id)
        if quote is None or census is None:
            raise KeyError(case_id)
        output = self._build_output(quote, self.case_repository.get_lobs(case_id), census)
        return self.case_repository.save_output(output)

    def _build_lob_request(self, case_id: str, lob_type: str, attachment_insight) -> LOBRequest:
        attachment_plans = attachment_insight.plan_designs.get(lob_type, [])
        evidence = attachment_insight.lob_evidence.get(lob_type, [])
        plan = attachment_plans[0] if attachment_plans else self._default_plan_for(lob_type)
        warnings = []
        if attachment_plans:
            warnings.append("Plan design derived from attachment evidence; review extracted terms")
        elif plan.plan_type:
            warnings.append("Plan design populated from heuristic defaults in scaffold")
        return LOBRequest(
            lob_case_id=self.ids.lob_case_id(case_id, lob_type),
            parent_case_id=case_id,
            lob_type=lob_type,
            requested_plan_designs=[plan],
            extraction_confidence=0.68,
            warnings=warnings,
            evidence_references=evidence
            or [
                EvidenceReference(
                    source_type="email_body",
                    snippet=f"LOB keyword match for {lob_type}",
                    confidence=0.66,
                )
            ],
        )

    def _default_plan_for(self, lob_type: str) -> PlanDesign:
        if lob_type == "group_life":
            return PlanDesign(plan_type="basic_life", benefit_basis="2x_salary", max_benefit=500000)
        if lob_type == "group_ltd":
            return PlanDesign(plan_type="ltd", benefit_percent=60, elimination_period_days=90, max_monthly_benefit=10000)
        if lob_type == "group_std":
            return PlanDesign(plan_type="std", benefit_percent=60, elimination_period_days=14, max_weekly_benefit=1500)
        if lob_type == "dental":
            return PlanDesign(plan_type="ppo", notes=["Benefit tiers pending extraction"])
        if lob_type == "vision":
            return PlanDesign(plan_type="vision", notes=["Copay schedule pending extraction"])
        return PlanDesign(plan_type=lob_type)

    def _build_output(self, quote: QuoteRequest, lobs: list[LOBRequest], census: CensusDataset) -> BQMOutput:
        warnings = quote.warnings + census.anomalies
        return self.bqm_validator.validate(BQMOutput(
            caseId=quote.case_id,
            submissionId=quote.submission_id,
            quoteType=quote.quote_type.value,
            employer=Employer(
                name=quote.employer_name,
                effective_date=quote.effective_date,
                situs_state=quote.situs_state,
            ),
            broker=Broker(name=quote.broker_name),
            lobs=[
                BQMLobOutput(
                    lobCaseId=lob.lob_case_id,
                    lobType=lob.lob_type,
                    planDesigns=[plan.model_dump(exclude_none=True) for plan in lob.requested_plan_designs],
                    warnings=lob.warnings,
                    evidenceReferences=[ref.model_dump(exclude_none=True) for ref in lob.evidence_references],
                )
                for lob in lobs
            ],
            census=BQMCensusOutput(
                employeeCount=census.employee_count,
                dependentCount=census.dependent_count,
                classesDetected=census.classes_detected,
                statesDetected=census.states_detected,
                rows=census.census_rows,
                summary=census.summary_statistics.model_dump(exclude_none=True),
                evidenceReferences=[ref.model_dump(exclude_none=True) for ref in census.evidence_references],
            ),
            warnings=warnings,
            confidence=BQMConfidence(
                overall=quote.extraction_confidence,
                census=census.extraction_confidence,
                plan_design=min([lob.extraction_confidence for lob in lobs], default=0.0),
            ),
        ))

    def _build_quote_evidence(self, submission) -> list[EvidenceReference]:
        snippet = submission.email_body_text[:180] if submission.email_body_text else submission.subject[:180]
        return [
            EvidenceReference(
                source_type="email_body",
                snippet=snippet,
                confidence=0.7,
            )
        ]

    def _extract_employer_name(self, text: str) -> str | None:
        match = re.search(r"employer\s*[:\-]\s*(.+)", text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return lines[0] if lines else None

    def _extract_broker_name(self, text: str) -> str | None:
        match = re.search(r"broker\s*[:\-]\s*(.+)", text, re.IGNORECASE)
        return match.group(1).strip() if match else None

    def _extract_effective_date(self, text: str):
        match = re.search(r"(20\d{2}-\d{2}-\d{2})", text)
        if match:
            return datetime.strptime(match.group(1), "%Y-%m-%d").date()
        return None

    def _extract_state(self, text: str) -> str | None:
        for token in re.findall(r"\b[A-Z]{2}\b", text.upper()):
            if token in STATE_CODES:
                return token
        return None
