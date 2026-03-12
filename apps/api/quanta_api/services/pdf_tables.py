from __future__ import annotations

import io

import pdfplumber


def extract_pdf_rows(content: bytes) -> list[tuple[int, list[dict[str, str]]]]:
    rows_by_page: list[tuple[int, list[dict[str, str]]]] = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables() or []
            for table in tables:
                cleaned = [[_cell_to_text(cell) for cell in row] for row in table if row and any(cell for cell in row)]
                if len(cleaned) < 2:
                    continue
                headers = [header or f"column_{index+1}" for index, header in enumerate(cleaned[0])]
                records: list[dict[str, str]] = []
                for row in cleaned[1:]:
                    item: dict[str, str] = {}
                    for index, value in enumerate(row):
                        header = headers[index] if index < len(headers) else f"column_{index+1}"
                        item[header] = value
                    if any(item.values()):
                        records.append(item)
                if records:
                    rows_by_page.append((page_index, records))
    return rows_by_page


def extract_pdf_text(content: bytes) -> list[tuple[int, str]]:
    pages: list[tuple[int, str]] = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            pages.append((page_index, page.extract_text() or ""))
    return pages


def _cell_to_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()
