from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone


class IdFactory:
    def __init__(self) -> None:
        self._counters: dict[tuple[str, int], int] = defaultdict(int)

    def _next(self, prefix: str) -> tuple[int, int]:
        year = datetime.now(timezone.utc).year
        key = (prefix, year)
        self._counters[key] += 1
        return year, self._counters[key]

    def next_submission_id(self) -> str:
        year, counter = self._next("SUB")
        return f"SUB-{year}-{counter:06d}"

    def next_case_id(self) -> str:
        year, counter = self._next("QNT")
        return f"QNT-{year}-{counter:06d}"

    def next_attachment_id(self) -> str:
        year, counter = self._next("ATT")
        return f"ATT-{year}-{counter:06d}"

    def next_census_id(self) -> str:
        year, counter = self._next("CEN")
        return f"CEN-{year}-{counter:06d}"

    def next_job_id(self) -> str:
        year, counter = self._next("JOB")
        return f"JOB-{year}-{counter:06d}"

    def next_alert_id(self) -> str:
        year, counter = self._next("ALT")
        return f"ALT-{year}-{counter:06d}"

    def lob_case_id(self, parent_case_id: str, lob_type: str) -> str:
        suffix = lob_type.upper().replace("GROUP_", "").replace("SUPPLEMENTAL_", "SUPP_")
        return f"{parent_case_id}-{suffix}"
