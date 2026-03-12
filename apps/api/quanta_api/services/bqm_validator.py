from __future__ import annotations

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
        if validated.census.employeeCount is None:
            raise ValueError("BQM output census must include employeeCount")
        if validated.census.employeeCount < 0 or validated.census.dependentCount < 0:
            raise ValueError("BQM output census counts cannot be negative")
        for lob in validated.lobs:
            if not lob.lobCaseId.startswith(validated.caseId):
                raise ValueError("BQM output lobCaseId must belong to caseId")
        return validated
