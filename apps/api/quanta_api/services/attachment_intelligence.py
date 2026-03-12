from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, field

from openpyxl import load_workbook

from quanta_api.domain.enums import SourceType
from quanta_api.domain.models import Attachment, EvidenceReference, PlanDesign
from quanta_api.domain.repositories import ObjectStore
from quanta_api.services.pdf_tables import extract_pdf_text

LOB_PATTERNS = {
    "group_life": [r"\bbasic life\b", r"\bvoluntary life\b", r"\blife\b"],
    "group_std": [r"\bstd\b", r"\bshort[- ]term disability\b"],
    "group_ltd": [r"\bltd\b", r"\blong[- ]term disability\b"],
    "dental": [r"\bdental\b", r"\bppo dental\b", r"\bdppo\b"],
    "vision": [r"\bvision\b"],
    "supplemental_ci": [r"\bcritical illness\b", r"\bci\b"],
    "supplemental_accident": [r"\baccident\b"],
    "supplemental_hi": [r"\bhospital indemnity\b", r"\bhi\b"],
}


@dataclass
class AttachmentInsight:
    detected_lobs: set[str] = field(default_factory=set)
    lob_evidence: dict[str, list[EvidenceReference]] = field(default_factory=dict)
    plan_designs: dict[str, list[PlanDesign]] = field(default_factory=dict)
    field_candidates: dict[str, list[tuple[str, EvidenceReference]]] = field(default_factory=dict)


class AttachmentIntelligenceService:
    def __init__(self, object_store: ObjectStore) -> None:
        self.object_store = object_store

    def analyze(self, attachments: list[Attachment]) -> AttachmentInsight:
        insight = AttachmentInsight()
        for attachment in attachments:
            if not attachment.storage_key:
                continue
            text = self._read_text(attachment)
            if not text:
                continue
            for lob_type, patterns in LOB_PATTERNS.items():
                matches = [pattern for pattern in patterns if re.search(pattern, text, re.IGNORECASE)]
                if matches:
                    insight.detected_lobs.add(lob_type)
                    evidence = EvidenceReference(
                        source_type=SourceType.attachment,
                        file_name=attachment.file_name,
                        snippet=self._matching_snippet(text, matches[0]),
                        confidence=0.78,
                    )
                    insight.lob_evidence.setdefault(lob_type, []).append(evidence)
                    plan = self._plan_design_for(lob_type, text, attachment.file_name)
                    if plan is not None:
                        insight.plan_designs.setdefault(lob_type, []).append(plan)
            self._collect_field_candidates(insight, attachment.file_name, text)
        return insight

    def _read_text(self, attachment: Attachment) -> str:
        content = self.object_store.get_bytes(attachment.storage_key)
        file_name = attachment.file_name.lower()
        if file_name.endswith(".csv"):
            return content.decode("utf-8-sig", errors="ignore")
        if file_name.endswith(".xlsx"):
            workbook = load_workbook(io.BytesIO(content), data_only=True, read_only=True)
            chunks: list[str] = []
            for sheet in workbook.worksheets:
                for row in sheet.iter_rows(values_only=True):
                    values = [str(value).strip() for value in row if value is not None and str(value).strip()]
                    if values:
                        chunks.append(" ".join(values))
            return "\n".join(chunks)
        if file_name.endswith(".pdf"):
            return "\n".join(text for _, text in extract_pdf_text(content) if text)
        return ""

    def _plan_design_for(self, lob_type: str, text: str, file_name: str) -> PlanDesign | None:
        if lob_type == "group_life":
            basis_match = re.search(r"(\d+)x\s+salary", text, re.IGNORECASE)
            max_match = re.search(r"max(?:imum)?\s+(?:benefit|life benefit)\s+\$?([\d,]+)", text, re.IGNORECASE)
            if basis_match or max_match:
                return PlanDesign(
                    plan_type="basic_life",
                    benefit_basis=f"{basis_match.group(1)}x_salary" if basis_match else None,
                    max_benefit=float(max_match.group(1).replace(",", "")) if max_match else None,
                    notes=[f"Attachment-derived life design from {file_name}"],
                )
        if lob_type in {"group_ltd", "group_std"}:
            percent_match = re.search(r"(\d{1,2})\s*%\s*(?:benefit|of earnings)?", text, re.IGNORECASE)
            elimination_match = re.search(r"(?:elimination period|waiting period)\s*(?:of)?\s*(\d+)\s*days", text, re.IGNORECASE)
            max_match = re.search(r"max(?:imum)?\s+(?:monthly|weekly)\s+benefit\s+\$?([\d,]+)", text, re.IGNORECASE)
            if percent_match or elimination_match or max_match:
                return PlanDesign(
                    plan_type="ltd" if lob_type == "group_ltd" else "std",
                    benefit_percent=float(percent_match.group(1)) if percent_match else None,
                    elimination_period_days=int(elimination_match.group(1)) if elimination_match else None,
                    max_monthly_benefit=float(max_match.group(1).replace(",", "")) if lob_type == "group_ltd" and max_match else None,
                    max_weekly_benefit=float(max_match.group(1).replace(",", "")) if lob_type == "group_std" and max_match else None,
                    notes=[f"Attachment-derived disability design from {file_name}"],
                )
        if lob_type == "dental":
            coverage_match = re.search(r"(100/80/50|90/80/50|80/80/50)", text, re.IGNORECASE)
            contribution_match = re.search(r"employer\s+paid|employee\s+paid|contributory", text, re.IGNORECASE)
            if re.search(r"\bppo\b", text, re.IGNORECASE) or coverage_match or contribution_match:
                notes = [f"Attachment-derived dental design from {file_name}"]
                if coverage_match:
                    notes.append(f"Coverage tiers {coverage_match.group(1)}")
                return PlanDesign(
                    plan_type="ppo" if re.search(r"\bppo\b", text, re.IGNORECASE) else "dental",
                    contribution_details=contribution_match.group(0) if contribution_match else None,
                    notes=notes,
                )
        if lob_type == "vision":
            exam_match = re.search(r"exam\s+copay\s+\$?(\d+)", text, re.IGNORECASE)
            material_match = re.search(r"materials?\s+copay\s+\$?(\d+)", text, re.IGNORECASE)
            if re.search(r"\bvision\b", text, re.IGNORECASE):
                notes = [f"Attachment-derived vision design from {file_name}"]
                if exam_match:
                    notes.append(f"Exam copay ${exam_match.group(1)}")
                if material_match:
                    notes.append(f"Materials copay ${material_match.group(1)}")
                return PlanDesign(plan_type="vision", notes=notes)
        if lob_type == "supplemental_ci":
            benefit_match = re.search(r"critical illness.*?\$?([\d,]+)", text, re.IGNORECASE)
            if benefit_match:
                return PlanDesign(
                    plan_type="critical_illness",
                    max_benefit=float(benefit_match.group(1).replace(",", "")),
                    notes=[f"Attachment-derived CI design from {file_name}"],
                )
        if lob_type == "supplemental_accident":
            if re.search(r"accident", text, re.IGNORECASE):
                return PlanDesign(
                    plan_type="accident",
                    notes=[f"Attachment-derived accident design from {file_name}"],
                )
        if lob_type == "supplemental_hi":
            benefit_match = re.search(r"hospital indemnity.*?\$?([\d,]+)", text, re.IGNORECASE)
            if benefit_match:
                return PlanDesign(
                    plan_type="hospital_indemnity",
                    max_benefit=float(benefit_match.group(1).replace(",", "")),
                    notes=[f"Attachment-derived hospital indemnity design from {file_name}"],
                )
        return None

    def _matching_snippet(self, text: str, pattern: str) -> str:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            return text[:180]
        start = max(0, match.start() - 40)
        end = min(len(text), match.end() + 80)
        return text[start:end].replace("\n", " ")

    def _collect_field_candidates(self, insight: AttachmentInsight, file_name: str, text: str) -> None:
        patterns = {
            "employer_name": r"employer\s*[:\-]\s*([A-Za-z0-9&.,' -]+)",
            "broker_name": r"broker\s*[:\-]\s*([A-Za-z0-9&.,' -]+)",
            "effective_date": r"(20\d{2}-\d{2}-\d{2})",
            "situs_state": r"situs(?: state)?\s*[:\-]\s*([A-Z]{2})",
        }
        for field_name, pattern in patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if not match:
                continue
            value = match.group(1).strip()
            evidence = EvidenceReference(
                source_type=SourceType.attachment,
                file_name=file_name,
                snippet=self._matching_snippet(text, pattern),
                confidence=0.76,
            )
            insight.field_candidates.setdefault(field_name, []).append((value, evidence))
