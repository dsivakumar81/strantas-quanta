from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable

from quanta_api.domain.enums import QuoteType
from quanta_api.domain.models import EvidenceReference, FieldExtractionResult, LOBRequest, PlanDesign, QuoteRequest
from quanta_api.services.attachment_intelligence import AttachmentInsight


@dataclass
class NormalizedCoreFields:
    employer_name: str | None
    broker_name: str | None
    broker_agency_name: str | None
    broker_contact_name: str | None
    broker_contact_email: str | None
    employer_contact_name: str | None
    employer_contact_email: str | None
    effective_date: date | None
    response_due_date: date | None
    situs_state: str | None
    market_segment: str | None
    incumbent_carrier: str | None
    quote_type: QuoteType | None
    evidence: list[EvidenceReference]
    warnings: list[str]
    field_results: dict[str, FieldExtractionResult]


class NormalizationService:
    def resolve_core_fields(
        self,
        email_fields: dict[str, object | None],
        attachment_insight: AttachmentInsight,
        parsers: dict[str, Callable[[str], object | None]],
    ) -> NormalizedCoreFields:
        evidence: list[EvidenceReference] = []
        warnings: list[str] = []
        resolved: dict[str, object | None] = {}
        field_results: dict[str, FieldExtractionResult] = {}

        for field_name, email_value in email_fields.items():
            attachment_candidates = sorted(
                attachment_insight.field_candidates.get(field_name, []),
                key=lambda item: self._source_rank(item[1]),
                reverse=True,
            )
            if email_value is not None:
                field_confidence = 0.9
                evidence.append(
                    EvidenceReference(
                        source_type="email_body",
                        snippet=f"{field_name} extracted from email body",
                        confidence=0.72,
                    )
                )
                resolved[field_name] = email_value
            elif attachment_candidates:
                parser = parsers[field_name]
                resolved[field_name] = parser(attachment_candidates[0][0])
                field_confidence = attachment_candidates[0][1].confidence
            else:
                resolved[field_name] = None
                field_confidence = 0.0

            field_warnings: list[str] = []
            if attachment_candidates:
                evidence.append(attachment_candidates[0][1])
                unique_attachment_values = sorted({candidate for candidate, _ref in attachment_candidates})
                if len(unique_attachment_values) > 1:
                    message = f"{field_name} contradiction detected across attachments; using '{attachment_candidates[0][0]}'"
                    warnings.append(message)
                    field_warnings.append(message)
                if email_value is not None:
                    comparable_email = email_value.isoformat() if hasattr(email_value, "isoformat") else str(email_value)
                    if comparable_email != attachment_candidates[0][0]:
                        message = f"{field_name} conflict detected; email value retained over attachment value '{attachment_candidates[0][0]}' from ranked source"
                        warnings.append(message)
                        field_warnings.append(message)

            field_results[field_name] = FieldExtractionResult(
                value=resolved[field_name].isoformat() if hasattr(resolved[field_name], "isoformat") else resolved[field_name],
                confidence=field_confidence,
                evidence=[
                    ref
                    for ref in evidence
                    if ref.snippet == f"{field_name} extracted from email body" or ref in [candidate[1] for candidate in attachment_candidates]
                ],
                warnings=field_warnings,
            )

        return NormalizedCoreFields(
            employer_name=resolved.get("employer_name"),
            broker_name=resolved.get("broker_name"),
            broker_agency_name=resolved.get("broker_agency_name") or resolved.get("broker_name"),
            broker_contact_name=resolved.get("broker_contact_name"),
            broker_contact_email=resolved.get("broker_contact_email"),
            employer_contact_name=resolved.get("employer_contact_name"),
            employer_contact_email=resolved.get("employer_contact_email"),
            effective_date=resolved.get("effective_date"),
            response_due_date=resolved.get("response_due_date"),
            situs_state=resolved.get("situs_state"),
            market_segment=resolved.get("market_segment"),
            incumbent_carrier=resolved.get("incumbent_carrier"),
            quote_type=resolved.get("quote_type"),
            evidence=evidence,
            warnings=warnings,
            field_results=field_results,
        )

    def normalize_lob_request(self, lob_request: LOBRequest) -> LOBRequest:
        normalized_designs, design_warnings = self.merge_plan_designs(lob_request.lob_type, lob_request.requested_plan_designs)
        lob_request.requested_plan_designs = normalized_designs
        lob_request.warnings = sorted(set(lob_request.warnings + design_warnings))
        return lob_request

    def normalize_quote_request(self, quote_request: QuoteRequest) -> QuoteRequest:
        quote_request.requested_lobs = sorted(set(quote_request.requested_lobs))
        quote_request.warnings = sorted(set(quote_request.warnings))
        return quote_request

    def merge_plan_designs(self, lob_type: str, plan_designs: list[PlanDesign]) -> tuple[list[PlanDesign], list[str]]:
        if not plan_designs:
            return [], []
        if len(plan_designs) == 1:
            return [self._normalize_plan_design(plan_designs[0])], []

        warnings: list[str] = []
        merged = PlanDesign(plan_type=plan_designs[0].plan_type)
        merged.benefit_basis = self._resolve_text([plan.benefit_basis for plan in plan_designs], "benefit_basis", warnings)
        merged.contribution_details = self._resolve_text([plan.contribution_details for plan in plan_designs], "contribution_details", warnings)
        merged.benefit_percent = self._resolve_number([plan.benefit_percent for plan in plan_designs], "benefit_percent", warnings)
        merged.elimination_period_days = self._resolve_number([plan.elimination_period_days for plan in plan_designs], "elimination_period_days", warnings)
        merged.max_benefit = self._resolve_number([plan.max_benefit for plan in plan_designs], "max_benefit", warnings)
        merged.max_monthly_benefit = self._resolve_number([plan.max_monthly_benefit for plan in plan_designs], "max_monthly_benefit", warnings)
        merged.max_weekly_benefit = self._resolve_number([plan.max_weekly_benefit for plan in plan_designs], "max_weekly_benefit", warnings)
        merged.guarantee_issue = self._resolve_number([plan.guarantee_issue for plan in plan_designs], "guarantee_issue", warnings)
        merged.attributes = self._merge_attributes(plan_designs, warnings)
        merged.notes = sorted({note for plan in plan_designs for note in plan.notes})
        return [self._normalize_plan_design(merged)], [f"{lob_type} {warning}" for warning in warnings]

    def _normalize_plan_design(self, plan_design: PlanDesign) -> PlanDesign:
        if plan_design.plan_type == "critical_illness":
            plan_design.notes = sorted(set(plan_design.notes + ["Supplemental health design"]))
        if plan_design.plan_type == "hospital_indemnity":
            plan_design.notes = sorted(set(plan_design.notes + ["Supplemental health design"]))
        if plan_design.plan_type == "vision" and not any("copay" in note.lower() for note in plan_design.notes):
            plan_design.notes = sorted(set(plan_design.notes + ["Copay schedule pending extraction"]))
        if plan_design.plan_type == "dental" and "coverage_tiers" in plan_design.attributes:
            plan_design.notes = sorted(set(plan_design.notes + [f"Coverage tiers {plan_design.attributes['coverage_tiers']}"]))
        return plan_design

    def _resolve_text(self, values: list[str | None], field_name: str, warnings: list[str]) -> str | None:
        normalized = [value.strip() for value in values if value]
        if not normalized:
            return None
        unique_values = sorted(set(normalized))
        if len(unique_values) > 1:
            warnings.append(f"{field_name} conflict resolved using first extracted value '{unique_values[0]}'")
        return unique_values[0]

    def _resolve_number(self, values: list[float | int | None], field_name: str, warnings: list[str]):
        normalized = [value for value in values if value is not None]
        if not normalized:
            return None
        unique_values = sorted(set(normalized))
        if len(unique_values) > 1:
            warnings.append(f"{field_name} conflict resolved using highest extracted value '{unique_values[-1]}'")
        return unique_values[-1]

    def _merge_attributes(self, plan_designs: list[PlanDesign], warnings: list[str]) -> dict[str, object]:
        merged: dict[str, object] = {}
        for plan in plan_designs:
            for key, value in plan.attributes.items():
                if key not in merged:
                    merged[key] = value
                    continue
                if merged[key] != value:
                    warnings.append(f"attribute '{key}' conflict resolved using latest attachment-backed value")
                    merged[key] = value
        return merged

    def _source_rank(self, evidence: EvidenceReference) -> float:
        if evidence.source_type == "email_body":
            return 1.0
        file_name = (evidence.file_name or "").lower()
        if file_name.endswith(".xlsx"):
            return 0.9
        if "plan" in file_name or "benefit" in file_name:
            return 0.88
        if file_name.endswith(".pdf"):
            return 0.8
        if "census" in file_name:
            return 0.65
        if file_name.endswith(".csv"):
            return 0.6
        return 0.5
