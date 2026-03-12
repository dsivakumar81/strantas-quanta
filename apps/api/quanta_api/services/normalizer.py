from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable

from quanta_api.domain.models import EvidenceReference, LOBRequest, PlanDesign, QuoteRequest
from quanta_api.services.attachment_intelligence import AttachmentInsight


@dataclass
class NormalizedCoreFields:
    employer_name: str | None
    broker_name: str | None
    effective_date: date | None
    situs_state: str | None
    evidence: list[EvidenceReference]
    warnings: list[str]


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

        for field_name, email_value in email_fields.items():
            attachment_candidates = attachment_insight.field_candidates.get(field_name, [])
            if email_value is not None:
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
            else:
                resolved[field_name] = None

            if attachment_candidates:
                evidence.append(attachment_candidates[0][1])
                if email_value is not None:
                    comparable_email = email_value.isoformat() if hasattr(email_value, "isoformat") else str(email_value)
                    if comparable_email != attachment_candidates[0][0]:
                        warnings.append(
                            f"{field_name} conflict detected; email value retained over attachment value '{attachment_candidates[0][0]}'"
                        )

        return NormalizedCoreFields(
            employer_name=resolved.get("employer_name"),
            broker_name=resolved.get("broker_name"),
            effective_date=resolved.get("effective_date"),
            situs_state=resolved.get("situs_state"),
            evidence=evidence,
            warnings=warnings,
        )

    def normalize_lob_request(self, lob_request: LOBRequest) -> LOBRequest:
        normalized_designs = [self._normalize_plan_design(plan) for plan in lob_request.requested_plan_designs]
        lob_request.requested_plan_designs = normalized_designs
        lob_request.warnings = sorted(set(lob_request.warnings))
        return lob_request

    def normalize_quote_request(self, quote_request: QuoteRequest) -> QuoteRequest:
        quote_request.requested_lobs = sorted(set(quote_request.requested_lobs))
        quote_request.warnings = sorted(set(quote_request.warnings))
        return quote_request

    def _normalize_plan_design(self, plan_design: PlanDesign) -> PlanDesign:
        if plan_design.plan_type == "critical_illness":
            plan_design.notes = sorted(set(plan_design.notes + ["Supplemental health design"]))
        if plan_design.plan_type == "hospital_indemnity":
            plan_design.notes = sorted(set(plan_design.notes + ["Supplemental health design"]))
        if plan_design.plan_type == "vision" and not any("copay" in note.lower() for note in plan_design.notes):
            plan_design.notes = sorted(set(plan_design.notes + ["Copay schedule pending extraction"]))
        return plan_design
