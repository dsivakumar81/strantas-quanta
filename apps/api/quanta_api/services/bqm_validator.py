from __future__ import annotations

from quanta_api.domain.enums import QuoteType
from quanta_api.domain.models import BQMOutput


class BQMValidationService:
    def validate(self, output: BQMOutput) -> BQMOutput:
        validated = BQMOutput.model_validate(output.model_dump())
        if not validated.caseId.startswith("QNT-"):
            raise ValueError("BQM output caseId must start with QNT-")
        if not validated.submissionId.startswith("SUB-"):
            raise ValueError("BQM output submissionId must start with SUB-")
        if not validated.lobs:
            raise ValueError("BQM output must include at least one LOB")
        if validated.quoteType not in {item.value for item in QuoteType}:
            raise ValueError("BQM output quoteType is invalid")
        if validated.census.employeeCount is None:
            raise ValueError("BQM output census must include employeeCount")
        if validated.census.employeeCount < 0 or validated.census.dependentCount < 0:
            raise ValueError("BQM output census counts cannot be negative")
        if validated.employer.name is not None and len(validated.employer.name.strip()) == 0:
            raise ValueError("BQM output employer name cannot be blank")
        for lob in validated.lobs:
            if not lob.lobCaseId.startswith(validated.caseId):
                raise ValueError("BQM output lobCaseId must belong to caseId")
            if not lob.fieldResults:
                raise ValueError("BQM output lob fieldResults must be populated")
        if not validated.census.fieldResults:
            raise ValueError("BQM output census fieldResults must be populated")
        if len({lob.lobCaseId for lob in validated.lobs}) != len(validated.lobs):
            raise ValueError("BQM output lobCaseIds must be unique")
        if len(validated.warnings) != len(set(validated.warnings)):
            raise ValueError("BQM output warnings must be unique")
        return validated
