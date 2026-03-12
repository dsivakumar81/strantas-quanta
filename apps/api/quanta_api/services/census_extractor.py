from __future__ import annotations

import csv
import io
from datetime import datetime
from statistics import mean, median

from openpyxl import load_workbook

from quanta_api.domain.enums import SourceType
from quanta_api.domain.models import Attachment, CensusDataset, CensusSummary, EvidenceReference
from quanta_api.domain.repositories import ObjectStore
from quanta_api.services.id_factory import IdFactory
from quanta_api.services.ocr import ocr_pdf_rows
from quanta_api.services.pdf_tables import extract_pdf_rows, extract_pdf_text

AGE_COLUMNS = {"age", "employee_age"}
SALARY_COLUMNS = {"salary", "annual_salary", "earnings", "annual_earnings"}
STATE_COLUMNS = {"state", "employee_state", "work_state", "resident_state"}
CLASS_COLUMNS = {"class", "employee_class", "benefit_class"}
DEPENDENT_COLUMNS = {"dependent_count", "dependents"}


class CensusExtractionService:
    def __init__(self, object_store: ObjectStore, ids: IdFactory) -> None:
        self.object_store = object_store
        self.ids = ids

    def extract(self, parent_case_id: str, attachments: list[Attachment]) -> CensusDataset:
        best_dataset: CensusDataset | None = None
        for attachment in attachments:
            if not attachment.storage_key:
                continue
            if attachment.file_name.lower().endswith(".csv"):
                rows = self._read_csv(attachment)
                dataset = self._build_dataset(parent_case_id, attachment, rows, attachment.file_name)
                best_dataset = self._pick_better(best_dataset, dataset)
            if attachment.file_name.lower().endswith(".xlsx"):
                rows, sheet_name = self._read_xlsx(attachment)
                dataset = self._build_dataset(parent_case_id, attachment, rows, attachment.file_name, sheet_name)
                best_dataset = self._pick_better(best_dataset, dataset)
            if attachment.file_name.lower().endswith(".pdf"):
                pdf_dataset = self._read_pdf(parent_case_id, attachment)
                best_dataset = self._pick_better(best_dataset, pdf_dataset)

        if best_dataset is not None:
            return best_dataset

        return CensusDataset(
            census_id=self.ids.next_census_id(),
            parent_case_id=parent_case_id,
            source_files=[item.file_name for item in attachments],
            anomalies=["No CSV/XLSX/PDF census attachment detected"],
            extraction_confidence=0.1,
        )

    def _read_csv(self, attachment: Attachment) -> list[dict[str, str]]:
        content = self.object_store.get_bytes(attachment.storage_key)
        text = content.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))
        return [{(key or "").strip(): (value or "").strip() for key, value in row.items()} for row in reader]

    def _read_xlsx(self, attachment: Attachment) -> tuple[list[dict[str, str]], str]:
        content = self.object_store.get_bytes(attachment.storage_key)
        workbook = load_workbook(io.BytesIO(content), data_only=True, read_only=True)
        sheet = workbook.active
        rows = list(sheet.iter_rows(values_only=True))
        headers = [str(cell).strip() if cell is not None else "" for cell in rows[0]]
        data_rows = []
        for row in rows[1:]:
            item = {}
            for index, value in enumerate(row):
                header = headers[index] if index < len(headers) else f"column_{index+1}"
                item[header] = "" if value is None else str(value).strip()
            if any(item.values()):
                data_rows.append(item)
        return data_rows, sheet.title

    def _build_dataset(
        self,
        parent_case_id: str,
        attachment: Attachment,
        rows: list[dict[str, str]],
        file_name: str,
        sheet_name: str | None = None,
    ) -> CensusDataset:
        normalized_rows = [self._normalize_row(row) for row in rows if any(value for value in row.values())]
        columns = list(normalized_rows[0].keys()) if normalized_rows else []
        employee_count = len(normalized_rows)
        dependent_count = sum(self._safe_int(row.get("dependent_count")) for row in normalized_rows)
        classes_detected = sorted({row.get("class", "") for row in normalized_rows if row.get("class")})
        states_detected = sorted({row.get("state", "") for row in normalized_rows if row.get("state")})
        ages = [self._safe_float(row.get("age")) for row in normalized_rows if self._safe_float(row.get("age")) is not None]
        salaries = [self._safe_float(row.get("salary")) for row in normalized_rows if self._safe_float(row.get("salary")) is not None]

        evidence = [
            EvidenceReference(
                source_type=SourceType.attachment,
                file_name=file_name,
                sheet_name=sheet_name,
                cell_range=f"A1:{self._column_letter(len(columns))}{min(employee_count + 1, 25)}" if columns else None,
                snippet=f"Detected {employee_count} census rows with columns: {', '.join(columns[:8])}",
                confidence=0.92 if employee_count else 0.4,
            )
        ]

        anomalies = []
        if not employee_count:
            anomalies.append("Attachment was readable but produced zero census rows")
        if "salary" not in columns:
            anomalies.append("Salary column not detected")
        if "age" not in columns:
            anomalies.append("Age column not detected")

        return CensusDataset(
            census_id=self.ids.next_census_id(),
            parent_case_id=parent_case_id,
            source_files=[file_name],
            employee_count=employee_count,
            dependent_count=dependent_count,
            classes_detected=classes_detected,
            states_detected=states_detected,
            census_columns_detected=columns,
            census_rows=normalized_rows[:200],
            summary_statistics=CensusSummary(
                avg_age=round(mean(ages), 1) if ages else None,
                median_salary=round(median(salaries), 2) if salaries else None,
            ),
            anomalies=anomalies,
            extraction_confidence=0.92 if employee_count else 0.3,
            evidence_references=evidence,
        )

    def _read_pdf(self, parent_case_id: str, attachment: Attachment) -> CensusDataset:
        content = self.object_store.get_bytes(attachment.storage_key)
        table_pages = extract_pdf_rows(content)
        text_pages = extract_pdf_text(content)
        ocr_pages: list[tuple[int, list[dict[str, str]]]] = []
        ocr_warnings: list[str] = []
        if not table_pages:
            ocr_pages, ocr_warnings = ocr_pdf_rows(content)

        normalized_rows: list[dict[str, str]] = []
        evidence: list[EvidenceReference] = []
        for page_number, rows in table_pages:
            page_rows = [self._normalize_row(row) for row in rows if any(value for value in row.values())]
            if page_rows:
                normalized_rows.extend(page_rows)
                columns = list(page_rows[0].keys())
                evidence.append(
                    EvidenceReference(
                        source_type=SourceType.attachment,
                        file_name=attachment.file_name,
                        page_number=page_number,
                        cell_range=f"A1:{self._column_letter(len(columns))}{min(len(page_rows) + 1, 25)}",
                        snippet=f"PDF table rows detected on page {page_number}: {', '.join(columns[:8])}",
                        confidence=0.86,
                    )
                )
        for page_number, rows in ocr_pages:
            page_rows = [self._normalize_row(row) for row in rows if any(value for value in row.values())]
            if page_rows:
                normalized_rows.extend(page_rows)
                evidence.append(
                    EvidenceReference(
                        source_type=SourceType.attachment,
                        file_name=attachment.file_name,
                        page_number=page_number,
                        snippet=f"OCR recovered census-like rows from scanned PDF page {page_number}",
                        confidence=0.58,
                    )
                )

        text_hint = next((text for _, text in text_pages if text), "")
        dataset = self._build_dataset(
            parent_case_id=parent_case_id,
            attachment=attachment,
            rows=normalized_rows,
            file_name=attachment.file_name,
        )
        if evidence:
            dataset.evidence_references = evidence
        if ocr_warnings:
            dataset.anomalies.extend(ocr_warnings)
        if text_hint and not evidence:
            dataset.evidence_references = [
                EvidenceReference(
                    source_type=SourceType.attachment,
                    file_name=attachment.file_name,
                    page_number=text_pages[0][0] if text_pages else None,
                    snippet=text_hint[:180],
                    confidence=0.45,
                )
            ]
            dataset.anomalies.append("PDF text detected but no structured census table was extracted")
            dataset.extraction_confidence = 0.25
        return dataset

    def _pick_better(self, current: CensusDataset | None, candidate: CensusDataset) -> CensusDataset:
        if current is None:
            return candidate
        if candidate.employee_count > current.employee_count:
            return candidate
        if candidate.employee_count == current.employee_count and candidate.extraction_confidence > current.extraction_confidence:
            return candidate
        return current

    def _normalize_row(self, row: dict[str, str]) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for raw_key, value in row.items():
            key = raw_key.strip().lower().replace(" ", "_")
            if key in AGE_COLUMNS:
                normalized["age"] = value
            elif key in SALARY_COLUMNS:
                normalized["salary"] = value.replace(",", "").replace("$", "")
            elif key in STATE_COLUMNS:
                normalized["state"] = value.upper()
            elif key in CLASS_COLUMNS:
                normalized["class"] = value
            elif key in DEPENDENT_COLUMNS:
                normalized["dependent_count"] = value
            else:
                normalized[key] = value
        return normalized

    def _safe_float(self, value: str | None) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except ValueError:
            try:
                return float(datetime.fromisoformat(value).year)
            except ValueError:
                return None

    def _safe_int(self, value: str | None) -> int:
        if value in (None, ""):
            return 0
        try:
            return int(float(value))
        except ValueError:
            return 0

    def _column_letter(self, column_number: int) -> str:
        if column_number <= 0:
            return "A"
        result = ""
        current = column_number
        while current:
            current, remainder = divmod(current - 1, 26)
            result = chr(65 + remainder) + result
        return result
