from __future__ import annotations

import re
from datetime import datetime
from uuid import uuid5, NAMESPACE_URL

from quanta_api.domain.enums import ProcessingStatus, QuoteType
from quanta_api.domain.models import (
    BQMConfidence,
    BQMCensusOutput,
    BQMLobOutput,
    BQMOutput,
    Broker,
    CarrierBenefitClass,
    CarrierBroker,
    CarrierContact,
    CarrierCoverage,
    CarrierDentalPlanDetails,
    CarrierEmployer,
    CarrierFile,
    CarrierGroupConfiguration,
    CarrierGroupMember,
    CarrierGroupRfp,
    CarrierLocation,
    CarrierProducer,
    CarrierVisionPlanDetails,
    CarrierBroker,
    CensusDataset,
    Employer,
    EvidenceReference,
    FieldExtractionResult,
    LOBRequest,
    PlanDesign,
    QuoteRequest,
)
from quanta_api.domain.repositories import CaseRepository, SubmissionRepository
from quanta_api.services.attachment_intelligence import AttachmentIntelligenceService
from quanta_api.services.bqm_validator import BQMValidationService
from quanta_api.services.census_extractor import CensusExtractionService
from quanta_api.services.id_factory import IdFactory
from quanta_api.services.normalizer import NormalizationService
from quanta_api.services.reader import ReaderService
from quanta_api.services.tracing import TraceSinkService

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

CARRIER_COVERAGE_TYPES = {
    "group_life": "basicLife",
    "group_std": "shortTermDisability",
    "group_ltd": "longTermDisability",
    "dental": "dental",
    "vision": "vision",
    "supplemental_ci": "criticalIllness",
    "supplemental_accident": "accident",
    "supplemental_hi": "hospitalIndemnity",
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
        reader_service: ReaderService,
        ids: IdFactory,
        trace_sink: TraceSinkService,
    ) -> None:
        self.submission_repository = submission_repository
        self.case_repository = case_repository
        self.attachment_intelligence = attachment_intelligence
        self.census_extractor = census_extractor
        self.bqm_validator = bqm_validator
        self.normalization_service = normalization_service
        self.reader_service = reader_service
        self.ids = ids
        self.trace_sink = trace_sink

    def parse_submission(self, submission_id: str, tenant_id: str | None = None):
        submission = self.submission_repository.get(submission_id, tenant_id=tenant_id)
        if submission is None:
            raise KeyError(submission_id)
        submission.processing_status = ProcessingStatus.parsed
        self.trace_sink.emit("submission.parsed", {"submission_id": submission_id, "tenant_id": submission.tenant_id})
        return self.submission_repository.update(submission)

    def run_extraction(self, submission_id: str, tenant_id: str | None = None):
        submission = self.submission_repository.get(submission_id, tenant_id=tenant_id)
        if submission is None:
            raise KeyError(submission_id)
        if any(attachment.document_type.value == "unknown" for attachment in submission.attachments):
            self.reader_service.parse_submission(submission_id, tenant_id=submission.tenant_id)
            submission = self.submission_repository.get(submission_id, tenant_id=submission.tenant_id)
            if submission is None:
                raise KeyError(submission_id)
        body = f"{submission.subject}\n{submission.email_body_text}".lower()
        detected_lobs = [lob for lob, patterns in LOB_PATTERNS.items() if any(p in body for p in patterns)]
        attachment_insight = self.attachment_intelligence.analyze(submission.attachments)
        detected_lobs = sorted(set(detected_lobs).union(attachment_insight.detected_lobs))
        if not detected_lobs:
            detected_lobs = ["group_life"]

        case_id = self.submission_repository.get_case_id(submission_id, tenant_id=submission.tenant_id)
        if case_id is None:
            case_id = self.ids.next_case_id()
            self.submission_repository.set_case_id(submission_id, case_id, tenant_id=submission.tenant_id)

        core_fields = self.normalization_service.resolve_core_fields(
            email_fields={
                "employer_name": self._extract_employer_name(submission.email_body_text),
                "broker_name": self._extract_broker_name(submission.email_body_text),
                "broker_agency_name": self._extract_broker_name(submission.email_body_text),
                "broker_contact_name": self._extract_labeled_value(submission.email_body_text, "broker contact"),
                "broker_contact_email": self._extract_labeled_value(submission.email_body_text, "broker email"),
                "employer_contact_name": self._extract_labeled_value(submission.email_body_text, "employer contact"),
                "employer_contact_email": self._extract_labeled_value(submission.email_body_text, "employer email"),
                "effective_date": self._extract_effective_date(submission.email_body_text),
                "response_due_date": self._extract_due_date(submission.email_body_text),
                "situs_state": self._extract_state(submission.email_body_text),
                "market_segment": self._extract_market_segment(submission.email_body_text),
                "incumbent_carrier": self._extract_incumbent_carrier(submission.email_body_text),
                "quote_type": self._extract_quote_type(f"{submission.subject}\n{submission.email_body_text}"),
            },
            attachment_insight=attachment_insight,
            parsers={
                "employer_name": lambda value: value,
                "broker_name": lambda value: value,
                "broker_agency_name": self._title_case,
                "broker_contact_name": self._title_case,
                "broker_contact_email": lambda value: value,
                "employer_contact_name": self._title_case,
                "employer_contact_email": lambda value: value,
                "effective_date": self._extract_effective_date,
                "response_due_date": self._extract_effective_date,
                "situs_state": self._extract_state,
                "market_segment": lambda value: value.lower().replace(" ", "_"),
                "incumbent_carrier": lambda value: value,
                "quote_type": self._extract_quote_type,
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
            tenant_id=submission.tenant_id,
            submission_id=submission_id,
            quote_type=core_fields.quote_type or QuoteType.new_business,
            employer_name=core_fields.employer_name,
            broker_name=core_fields.broker_name,
            broker_agency_name=core_fields.broker_agency_name,
            broker_contact_name=core_fields.broker_contact_name,
            broker_contact_email=core_fields.broker_contact_email,
            employer_contact_name=core_fields.employer_contact_name,
            employer_contact_email=core_fields.employer_contact_email,
            effective_date=core_fields.effective_date,
            response_due_date=core_fields.response_due_date,
            situs_state=core_fields.situs_state,
            market_segment=core_fields.market_segment,
            incumbent_carrier=core_fields.incumbent_carrier,
            requested_lobs=detected_lobs,
            overall_status=ProcessingStatus.extracted,
            extraction_confidence=0.72,
            warnings=warnings.copy(),
            evidence_references=quote_evidence,
            field_results=core_fields.field_results,
        )

        lob_requests = [
            self.normalization_service.normalize_lob_request(
                self._build_lob_request(submission.tenant_id, case_id, lob, attachment_insight)
            )
            for lob in detected_lobs
        ]
        census = self.census_extractor.extract(case_id, submission.attachments)
        census.tenant_id = submission.tenant_id
        quote = self.normalization_service.normalize_quote_request(quote)

        self.case_repository.save_quote(quote)
        self.case_repository.save_lobs(case_id, lob_requests)
        self.case_repository.save_census(census)
        submission.processing_status = ProcessingStatus.extracted
        self.submission_repository.update(submission)
        self.trace_sink.emit(
            "submission.extracted",
            {"submission_id": submission_id, "case_id": case_id, "tenant_id": submission.tenant_id, "lob_count": len(lob_requests)},
        )
        return quote, lob_requests, census

    def run_normalization(self, submission_id: str, tenant_id: str | None = None):
        case_id = self.submission_repository.get_case_id(submission_id, tenant_id=tenant_id)
        if case_id is None:
            quote, lobs, census = self.run_extraction(submission_id, tenant_id=tenant_id)
        else:
            quote = self.case_repository.get_quote(case_id, tenant_id=tenant_id)
            lobs = self.case_repository.get_lobs(case_id, tenant_id=tenant_id)
            census = self.case_repository.get_census(case_id, tenant_id=tenant_id)
            if quote is None or census is None:
                quote, lobs, census = self.run_extraction(submission_id, tenant_id=tenant_id)

        quote.overall_status = ProcessingStatus.normalized
        self.case_repository.save_quote(quote)
        bqm_output = self._build_output(quote, lobs, census)
        self.case_repository.save_output(bqm_output)
        self.case_repository.save_carrier_output(self._build_carrier_output(quote, lobs, census), quote.case_id, quote.tenant_id)
        submission = self.submission_repository.get(submission_id, tenant_id=quote.tenant_id)
        if submission is None:
            raise KeyError(submission_id)
        submission.processing_status = ProcessingStatus.normalized
        self.submission_repository.update(submission)
        self.trace_sink.emit(
            "submission.normalized",
            {"submission_id": submission_id, "case_id": quote.case_id, "tenant_id": quote.tenant_id},
        )
        return quote, lobs, census

    def get_output(self, case_id: str, tenant_id: str | None = None) -> BQMOutput:
        output = self.case_repository.get_output(case_id, tenant_id=tenant_id)
        if output is not None:
            return output
        quote = self.case_repository.get_quote(case_id, tenant_id=tenant_id)
        census = self.case_repository.get_census(case_id, tenant_id=tenant_id)
        if quote is None or census is None:
            raise KeyError(case_id)
        output = self._build_output(quote, self.case_repository.get_lobs(case_id, tenant_id=quote.tenant_id), census)
        return self.case_repository.save_output(output)

    def get_carrier_output(self, case_id: str, tenant_id: str | None = None) -> CarrierGroupRfp:
        output = self.case_repository.get_carrier_output(case_id, tenant_id=tenant_id)
        if output is not None:
            return output
        quote = self.case_repository.get_quote(case_id, tenant_id=tenant_id)
        census = self.case_repository.get_census(case_id, tenant_id=tenant_id)
        if quote is None or census is None:
            raise KeyError(case_id)
        carrier = self._build_carrier_output(quote, self.case_repository.get_lobs(case_id, tenant_id=quote.tenant_id), census)
        return self.case_repository.save_carrier_output(carrier, case_id, quote.tenant_id)

    def _build_lob_request(self, tenant_id: str, case_id: str, lob_type: str, attachment_insight) -> LOBRequest:
        attachment_plans = attachment_insight.plan_designs.get(lob_type, [])
        evidence = attachment_insight.lob_evidence.get(lob_type, [])
        metadata = attachment_insight.lob_metadata.get(lob_type, {})
        plans = attachment_plans if attachment_plans else [self._default_plan_for(lob_type)]
        warnings = []
        if attachment_plans:
            warnings.append("Plan design derived from attachment evidence; review extracted terms")
        elif plans[0].plan_type:
            warnings.append("Plan design populated from heuristic defaults in scaffold")
        return LOBRequest(
            lob_case_id=self.ids.lob_case_id(case_id, lob_type),
            tenant_id=tenant_id,
            parent_case_id=case_id,
            lob_type=lob_type,
            requested_plan_designs=plans,
            class_structure=metadata.get("class_structure", []),
            eligibility_rules=metadata.get("eligibility_rules", []),
            contribution_details={"splits": metadata.get("contribution_splits", [])},
            waiting_periods=metadata.get("waiting_periods", []),
            notes=metadata.get("participation_minimums", []),
            extraction_confidence=0.68,
            warnings=warnings,
            evidence_references=evidence
            or [EvidenceReference(source_type="email_body", snippet=f"LOB keyword match for {lob_type}", confidence=0.66)],
            field_results=self._build_lob_field_results(plans, metadata, evidence),
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
        warnings = sorted(set(quote.warnings + census.anomalies))
        return self.bqm_validator.validate(
            BQMOutput(
                caseId=quote.case_id,
                submissionId=quote.submission_id,
                quoteType=quote.quote_type.value,
                employer=Employer(name=quote.employer_name, effective_date=quote.effective_date, situs_state=quote.situs_state),
                broker=Broker(name=quote.broker_name),
                lobs=[
                    BQMLobOutput(
                        lobCaseId=lob.lob_case_id,
                        lobType=lob.lob_type,
                        planDesigns=[plan.model_dump(exclude_none=True) for plan in lob.requested_plan_designs],
                        warnings=lob.warnings,
                        evidenceReferences=[ref.model_dump(exclude_none=True) for ref in lob.evidence_references],
                        fieldResults=lob.field_results,
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
                    fieldResults=census.field_results,
                ),
                warnings=warnings,
                confidence=BQMConfidence(
                    overall=quote.extraction_confidence,
                    census=census.extraction_confidence,
                    plan_design=min([lob.extraction_confidence for lob in lobs], default=0.0),
                ),
            )
        )

    def _build_carrier_output(self, quote: QuoteRequest, lobs: list[LOBRequest], census: CensusDataset) -> CarrierGroupRfp:
        submission = self.submission_repository.get(quote.submission_id, tenant_id=quote.tenant_id)
        attachment_insight = self.attachment_intelligence.analyze(submission.attachments) if submission else None
        carrier_contacts = self._extract_carrier_contacts(submission.email_body_text if submission else "", attachment_insight)
        carrier_locations = self._extract_carrier_locations(submission.email_body_text if submission else "", attachment_insight)
        employer_contact = self._build_carrier_contact(
            full_name=self._title_case(quote.employer_contact_name or carrier_contacts.get("employer_contact_name")),
            email=quote.employer_contact_email or carrier_contacts.get("employer_contact_email"),
            name_field_key="employer_contact_name",
            email_field_key="employer_contact_email",
            quote=quote,
        )
        broker_contact = self._build_carrier_contact(
            full_name=self._title_case(quote.broker_contact_name or carrier_contacts.get("broker_contact_name")),
            email=quote.broker_contact_email or carrier_contacts.get("broker_contact_email"),
            name_field_key="broker_contact_name",
            email_field_key="broker_contact_email",
            quote=quote,
        )
        broker_agency_name = self._title_case(quote.broker_agency_name or quote.broker_name or carrier_contacts.get("broker_agency_name"))
        benefit_classes = [
            CarrierBenefitClass(identifier=f"class-{index + 1}", name=name, minWeeklyEligibleHours="30")
            for index, name in enumerate(census.classes_detected or ["All Employees"])
        ]
        coverages = []
        for lob in lobs:
            primary_plan = lob.requested_plan_designs[0] if lob.requested_plan_designs else PlanDesign(plan_type=lob.lob_type)
            coverages.append(
                CarrierCoverage(
                    coverageType=CARRIER_COVERAGE_TYPES.get(lob.lob_type, lob.lob_type),
                    benefitPlanName=primary_plan.plan_type,
                    contributionType=self._carrier_contribution_type(lob),
                    planDetails=self._carrier_plan_details(lob, primary_plan),
                    dentalPlanDetails=self._carrier_dental_plan_details(primary_plan) if lob.lob_type == "dental" else None,
                    visionPlanDetails=self._carrier_vision_plan_details(primary_plan) if lob.lob_type == "vision" else None,
                    fieldResults=lob.field_results,
                )
            )
        members = [self._carrier_member(row) for row in census.census_rows]
        producer = CarrierProducer(
            broker=CarrierBroker(
                agencyName=broker_agency_name,
                contact=broker_contact,
                fieldResults=self._filter_field_results(
                    {
                        **({"agency_name": self._retarget_field_result(
                            quote.field_results.get("broker_agency_name") or quote.field_results.get("broker_name"),
                            broker_agency_name,
                        )} if broker_agency_name else {}),
                        **broker_contact.fieldResults,
                    },
                    {"agency_name", "full_name", "work_email"},
                ),
            ) if broker_agency_name or broker_contact.fullName or broker_contact.workEmail else None,
            fieldResults=self._filter_field_results(
                {
                    **({"agency_name": self._retarget_field_result(
                        quote.field_results.get("broker_agency_name") or quote.field_results.get("broker_name"),
                        broker_agency_name,
                    )} if broker_agency_name else {}),
                    **broker_contact.fieldResults,
                },
                {"agency_name", "full_name", "work_email"},
            ) if broker_agency_name or broker_contact.fullName or broker_contact.workEmail else {},
        )
        attachments = [
            CarrierFile(
                fileName=attachment.file_name,
                mediaType=attachment.detected_content_type or attachment.content_type,
                documentType=attachment.document_type.value,
                storageKey=attachment.storage_key,
                fieldResults={
                    "document_type": FieldExtractionResult(
                        value=attachment.document_type.value,
                        confidence=0.92 if attachment.document_type.value != "unknown" else 0.2,
                        evidence=attachment.evidence_references,
                        warnings=[] if attachment.document_type.value != "unknown" else ["Reader classification unavailable"],
                    ),
                    "media_type": FieldExtractionResult(
                        value=attachment.detected_content_type or attachment.content_type,
                        confidence=0.95 if (attachment.detected_content_type or attachment.content_type) else 0.1,
                        evidence=attachment.evidence_references,
                        warnings=[],
                    ),
                },
            )
            for attachment in (submission.attachments if submission else [])
        ]
        return CarrierGroupRfp(
            identifier=str(uuid5(NAMESPACE_URL, f"{quote.tenant_id}:{quote.case_id}")),
            effectiveDate=quote.effective_date.isoformat() if quote.effective_date else None,
            dueDate=quote.response_due_date.isoformat() if quote.response_due_date else None,
            notes="; ".join(sorted(set(quote.warnings))) or None,
            status="pending",
            employer=CarrierEmployer(
                name=quote.employer_name or "Unknown Employer",
                contacts=[employer_contact] if employer_contact.fullName or employer_contact.workEmail else [],
                locations=carrier_locations,
                fieldResults={
                    **quote.field_results,
                    **self._filter_field_results(employer_contact.fieldResults, {"full_name", "work_email"}),
                    **self._flatten_location_field_results(carrier_locations),
                },
            ),
            groupConfiguration=CarrierGroupConfiguration(
                benefitClasses=benefit_classes,
                coverages=coverages,
                producers=[producer] if broker_agency_name or broker_contact.fullName or broker_contact.workEmail else [],
                numberOfEligibleEmployees=census.employee_count,
            ),
            census=members,
            marketingStrategy=quote.market_segment,
            files=attachments,
            fieldResults={
                **quote.field_results,
                **self._filter_field_results(employer_contact.fieldResults, {"full_name", "work_email"}),
                **self._flatten_location_field_results(carrier_locations),
                "employee_count": census.field_results.get("employee_count", FieldExtractionResult(value=census.employee_count, confidence=census.extraction_confidence, evidence=census.evidence_references, warnings=[])),
            },
        )

    def _carrier_plan_details(self, lob: LOBRequest, primary_plan: PlanDesign) -> dict[str, object]:
        return {
            "attributes": primary_plan.attributes,
            "benefitPercent": primary_plan.benefit_percent,
            "benefitBasis": primary_plan.benefit_basis,
            "eliminationPeriodDays": primary_plan.elimination_period_days,
            "maxBenefit": primary_plan.max_benefit,
            "maxMonthlyBenefit": primary_plan.max_monthly_benefit,
            "maxWeeklyBenefit": primary_plan.max_weekly_benefit,
            "classStructure": lob.class_structure,
            "eligibilityRules": lob.eligibility_rules,
            "waitingPeriods": lob.waiting_periods,
            "participationMinimums": lob.notes,
        }

    def _carrier_dental_plan_details(self, primary_plan: PlanDesign) -> CarrierDentalPlanDetails | None:
        attrs = primary_plan.attributes
        if not any(
            key in attrs
            for key in {
                "coverage_tiers",
                "preventive_percent",
                "basic_percent",
                "major_percent",
                "orthodontia_percent",
                "orthodontia_age_limit",
                "deductible",
                "annual_maximum",
                "office_visit_copay",
                "service_waiting_periods",
            }
        ):
            return None
        return CarrierDentalPlanDetails(
            coverageTiers=attrs.get("coverage_tiers"),
            preventivePercent=attrs.get("preventive_percent"),
            basicPercent=attrs.get("basic_percent"),
            majorPercent=attrs.get("major_percent"),
            orthodontiaPercent=attrs.get("orthodontia_percent"),
            orthodontiaAgeLimit=attrs.get("orthodontia_age_limit"),
            deductible=attrs.get("deductible"),
            annualMaximum=attrs.get("annual_maximum"),
            officeVisitCopay=attrs.get("office_visit_copay"),
            serviceWaitingPeriods=attrs.get("service_waiting_periods"),
            fieldResults=self._filter_field_results(
                primary_plan.field_results,
                {
                    "coverage_tiers",
                    "preventive_percent",
                    "basic_percent",
                    "major_percent",
                    "orthodontia_percent",
                    "orthodontia_age_limit",
                    "deductible",
                    "annual_maximum",
                    "office_visit_copay",
                    "service_waiting_periods",
                },
            ),
        )

    def _carrier_vision_plan_details(self, primary_plan: PlanDesign) -> CarrierVisionPlanDetails | None:
        attrs = primary_plan.attributes
        if not any(
            key in attrs
            for key in {
                "exam_copay",
                "materials_copay",
                "lens_copay",
                "frame_allowance",
                "contact_allowance",
                "frequency_months",
                "laser_correction_allowance",
            }
        ):
            return None
        return CarrierVisionPlanDetails(
            examCopay=attrs.get("exam_copay"),
            materialsCopay=attrs.get("materials_copay"),
            lensCopay=attrs.get("lens_copay"),
            frameAllowance=attrs.get("frame_allowance"),
            contactAllowance=attrs.get("contact_allowance"),
            frequencyMonths=attrs.get("frequency_months"),
            laserCorrectionAllowance=attrs.get("laser_correction_allowance"),
            fieldResults=self._filter_field_results(
                primary_plan.field_results,
                {
                    "exam_copay",
                    "materials_copay",
                    "lens_copay",
                    "frame_allowance",
                    "contact_allowance",
                    "frequency_months",
                    "laser_correction_allowance",
                },
            ),
        )

    def _carrier_member(self, row: dict[str, object]) -> CarrierGroupMember:
        dependent_count = self._safe_int(row.get("dependent_count"))
        coverage = []
        if row.get("coverage_amount"):
            coverage.append({"coverageAmount": self._safe_float(row.get("coverage_amount"))})
        return CarrierGroupMember(
            employeeCode=str(row.get("employee_id") or row.get("employee_code") or row.get("id") or "unknown"),
            dependentRelationship="employee",
            employmentType=str(row.get("employment_type") or "fullTime") if row.get("employment_type") else None,
            gender=str(row.get("gender")).lower() if row.get("gender") else None,
            birthDate=str(row.get("birth_date")) if row.get("birth_date") else None,
            postalCode=str(row.get("zip") or row.get("postal_code")) if row.get("zip") or row.get("postal_code") else None,
            employmentStatus=str(row.get("status") or "active") if row.get("status") else None,
            employeeJobTitle=str(row.get("job_title")) if row.get("job_title") else None,
            benefitClassName=str(row.get("class")) if row.get("class") else None,
            annualIncome=self._safe_float(row.get("salary")),
            coverage=coverage + ([{"coverageTierCode": "family"}] if dependent_count > 0 else [{"coverageTierCode": "employee"}]),
        )

    def _build_carrier_contact(
        self,
        full_name: str | None,
        email: str | None,
        name_field_key: str,
        email_field_key: str,
        quote: QuoteRequest,
    ) -> CarrierContact:
        field_results: dict[str, FieldExtractionResult] = {}
        name_result = quote.field_results.get(name_field_key)
        email_result = quote.field_results.get(email_field_key)
        if full_name is not None:
            field_results["full_name"] = self._retarget_field_result(name_result, full_name)
        if email is not None:
            field_results["work_email"] = self._retarget_field_result(email_result, email, confidence=0.82)
        return CarrierContact(fullName=full_name, workEmail=email, fieldResults=field_results)

    def _carrier_contribution_type(self, lob: LOBRequest) -> str:
        joined = " ".join(lob.contribution_details.get("splits", [])).lower()
        if "employer paid" in joined and "employee paid" not in joined:
            return "fullyEmployerPaid"
        if "voluntary" in joined:
            return "voluntary"
        if joined:
            return "contributory"
        return "notDisclosed"

    def _build_quote_evidence(self, submission) -> list[EvidenceReference]:
        snippet = submission.email_body_text[:180] if submission.email_body_text else submission.subject[:180]
        return [EvidenceReference(source_type="email_body", snippet=snippet, confidence=0.7)]

    def _extract_employer_name(self, text: str) -> str | None:
        value = self._extract_labeled_value(text, "employer")
        if value:
            return value
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return lines[0] if lines else None

    def _extract_broker_name(self, text: str) -> str | None:
        return self._extract_labeled_value(text, "broker")

    def _extract_effective_date(self, text: str):
        match = re.search(r"(20\d{2}-\d{2}-\d{2})", text)
        if match:
            return datetime.strptime(match.group(1), "%Y-%m-%d").date()
        alt_match = re.search(r"(\d{1,2}/\d{1,2}/20\d{2})", text)
        if alt_match:
            return datetime.strptime(alt_match.group(1), "%m/%d/%Y").date()
        return None

    def _extract_state(self, text: str) -> str | None:
        for token in re.findall(r"\b[A-Z]{2}\b", text.upper()):
            if token in STATE_CODES:
                return token
        return None

    def _extract_market_segment(self, text: str) -> str | None:
        match = re.search(r"market segment\s*[:\-]\s*([A-Za-z ]+)", text, re.IGNORECASE)
        if match:
            return match.group(1).strip().lower().replace(" ", "_")
        employee_match = re.search(r"(\d{1,5})\s+employees", text, re.IGNORECASE)
        if employee_match:
            count = int(employee_match.group(1))
            if count < 100:
                return "small_group"
            if count < 1000:
                return "mid_market"
            return "large_group"
        return None

    def _extract_due_date(self, text: str):
        match = re.search(r"(?:due date|response due|proposal due)\s*[:\-]?\s*(20\d{2}-\d{2}-\d{2})", text, re.IGNORECASE)
        if match:
            return datetime.strptime(match.group(1), "%Y-%m-%d").date()
        alt_match = re.search(r"(?:due date|response due|proposal due)\s*[:\-]?\s*(\d{1,2}/\d{1,2}/20\d{2})", text, re.IGNORECASE)
        if alt_match:
            return datetime.strptime(alt_match.group(1), "%m/%d/%Y").date()
        return None

    def _extract_incumbent_carrier(self, text: str) -> str | None:
        return self._extract_labeled_value(text, "incumbent") or self._extract_labeled_value(text, "incumbent carrier")

    def _extract_quote_type(self, text: str) -> QuoteType | None:
        lowered = text.lower()
        if "renewal" in lowered:
            return QuoteType.renewal
        if "requote" in lowered or "re-quote" in lowered:
            return QuoteType.requote
        if "amendment" in lowered:
            return QuoteType.amendment
        if "new business" in lowered or "new case" in lowered:
            return QuoteType.new_business
        return None

    def _extract_labeled_value(self, text: str, label: str) -> str | None:
        pattern = rf"{label}\s*[:\-]\s*(.+?)(?=(?:\n|;)\s*(?:employer|broker|effective date|due date|response due|proposal due|situs|incumbent|incumbent carrier|market segment|please quote|requested lines)\b|$)"
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        return match.group(1).strip(" ;\n\t") if match else None

    def _extract_carrier_contacts(self, text: str, attachment_insight=None) -> dict[str, str]:
        def candidate(field_name: str) -> str | None:
            if not attachment_insight:
                return None
            ranked = sorted(attachment_insight.field_candidates.get(field_name, []), key=lambda item: item[1].confidence, reverse=True)
            return ranked[0][0] if ranked else None

        return {
            key: value
            for key, value in {
                "broker_agency_name": self._extract_broker_name(text) or candidate("broker_agency_name") or candidate("broker_name"),
                "broker_contact_name": self._extract_labeled_value(text, "broker contact") or candidate("broker_contact_name") or candidate("broker_contact"),
                "broker_contact_email": self._extract_labeled_value(text, "broker email") or candidate("broker_contact_email") or candidate("broker_email"),
                "employer_contact_name": self._extract_labeled_value(text, "employer contact") or candidate("employer_contact_name") or candidate("employer_contact"),
                "employer_contact_email": self._extract_labeled_value(text, "employer email") or candidate("employer_contact_email") or candidate("employer_email"),
            }.items()
            if value
        }

    def _extract_carrier_locations(self, text: str, attachment_insight=None) -> list[CarrierLocation]:
        def candidate(field_name: str) -> str | None:
            if not attachment_insight:
                return None
            ranked = sorted(attachment_insight.field_candidates.get(field_name, []), key=lambda item: item[1].confidence, reverse=True)
            return ranked[0][0] if ranked else None

        address = self._extract_labeled_value(text, "worksite address") or candidate("worksite_address") or self._extract_labeled_value(text, "address")
        city = self._extract_labeled_value(text, "city") or candidate("city")
        state = self._extract_labeled_value(text, "state") or candidate("situs_state") or self._extract_state(text)
        postal_code = self._extract_labeled_value(text, "zip") or self._extract_labeled_value(text, "postal code") or candidate("postal_code")
        if not any([address, city, state, postal_code]):
            return []
        field_results: dict[str, FieldExtractionResult] = {}
        formatted_address = self._title_case(address) if address else None
        normalized_state = state.upper() if state else None
        if formatted_address:
            field_results["address_line_1"] = self._attachment_field_result(formatted_address, attachment_insight, "worksite_address", 0.84)
        if city:
            field_results["city"] = self._attachment_field_result(self._title_case(city), attachment_insight, "city", 0.84)
        if normalized_state:
            field_results["state"] = self._attachment_field_result(normalized_state, attachment_insight, "situs_state", 0.86)
        if postal_code:
            field_results["postal_code"] = self._attachment_field_result(postal_code, attachment_insight, "postal_code", 0.84)
        return [CarrierLocation(addressLine1=formatted_address, city=self._title_case(city), state=normalized_state, postalCode=postal_code, fieldResults=field_results)]

    def _attachment_field_result(
        self,
        value: str,
        attachment_insight,
        field_name: str,
        default_confidence: float,
    ) -> FieldExtractionResult:
        if attachment_insight:
            ranked = sorted(attachment_insight.field_candidates.get(field_name, []), key=lambda item: item[1].confidence, reverse=True)
            if ranked:
                candidate_value, evidence = ranked[0]
                return FieldExtractionResult(
                    value=value,
                    confidence=evidence.confidence,
                    evidence=[evidence],
                    warnings=[] if candidate_value == value or candidate_value.lower() == str(value).lower() else [f"{field_name} normalized for carrier output"],
                )
        return FieldExtractionResult(value=value, confidence=default_confidence, evidence=[], warnings=[])

    def _retarget_field_result(
        self,
        source_result: FieldExtractionResult | None,
        value: str,
        confidence: float | None = None,
    ) -> FieldExtractionResult:
        if source_result is None:
            return FieldExtractionResult(value=value, confidence=confidence or 0.82, evidence=[], warnings=[])
        return FieldExtractionResult(
            value=value,
            confidence=confidence if confidence is not None else source_result.confidence,
            evidence=source_result.evidence,
            warnings=source_result.warnings,
        )

    def _filter_field_results(
        self,
        field_results: dict[str, FieldExtractionResult],
        allowed_keys: set[str],
    ) -> dict[str, FieldExtractionResult]:
        return {key: value for key, value in field_results.items() if key in allowed_keys}

    def _flatten_location_field_results(
        self,
        locations: list[CarrierLocation],
    ) -> dict[str, FieldExtractionResult]:
        if not locations:
            return {}
        location = locations[0]
        return {
            f"location_{key}": value
            for key, value in location.fieldResults.items()
        }

    def _title_case(self, value: str | None) -> str | None:
        if value is None:
            return None
        return " ".join(part.capitalize() for part in value.split())

    def _build_lob_field_results(self, plans: list[PlanDesign], metadata: dict[str, list[str]], evidence: list[EvidenceReference]) -> dict[str, FieldExtractionResult]:
        field_results: dict[str, FieldExtractionResult] = {}
        primary_plan = plans[0] if plans else None
        if primary_plan is not None:
            field_results.update(primary_plan.field_results)
            for field_name in [
                "plan_type",
                "benefit_basis",
                "benefit_percent",
                "elimination_period_days",
                "max_benefit",
                "max_monthly_benefit",
                "max_weekly_benefit",
                "guarantee_issue",
                "contribution_details",
            ]:
                value = getattr(primary_plan, field_name)
                if value is None:
                    continue
                existing = primary_plan.field_results.get(field_name)
                field_results[field_name] = existing or FieldExtractionResult(
                    value=value,
                    confidence=0.78,
                    evidence=evidence[:1],
                    warnings=[],
                )
        for field_name, metadata_key in {
            "class_structure": "class_structure",
            "eligibility_rules": "eligibility_rules",
            "contribution_splits": "contribution_splits",
            "waiting_periods": "waiting_periods",
            "participation_minimums": "participation_minimums",
        }.items():
            values = metadata.get(metadata_key, [])
            if not values:
                continue
            field_results[field_name] = FieldExtractionResult(value=values, confidence=0.72, evidence=evidence[:1], warnings=[])
        return field_results

    def _safe_float(self, value) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(str(value).replace(",", "").replace("$", ""))
        except ValueError:
            return None

    def _safe_int(self, value) -> int:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0
