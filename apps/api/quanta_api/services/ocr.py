from __future__ import annotations

import io
import re
import shutil

import pypdfium2 as pdfium
import pytesseract
from PIL import Image


def ocr_pdf_rows(content: bytes) -> tuple[list[tuple[int, list[dict[str, str]]]], list[str]]:
    warnings: list[str] = []
    if shutil.which("tesseract") is None:
        return [], ["Tesseract is not installed; OCR fallback unavailable"]

    document = pdfium.PdfDocument(content)
    extracted: list[tuple[int, list[dict[str, str]]]] = []
    try:
        for page_index in range(len(document)):
            page = document[page_index]
            bitmap = page.render(scale=2)
            pil_image = bitmap.to_pil()
            text = pytesseract.image_to_string(pil_image)
            rows = _parse_ocr_text(text)
            if rows:
                extracted.append((page_index + 1, rows))
    finally:
        document.close()
    return extracted, warnings


def _parse_ocr_text(text: str) -> list[dict[str, str]]:
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines() if line.strip()]
    if len(lines) < 2:
        return []
    header_index = next((i for i, line in enumerate(lines) if "employee_id" in line.lower() and "salary" in line.lower()), None)
    if header_index is None:
        return []
    headers = re.split(r"\s+", lines[header_index].lower())
    expected = ["employee_id", "first_name", "last_name", "age", "state", "salary", "class", "dependent_count"]
    if not all(token in headers for token in ["employee_id", "salary", "dependent_count"]):
        return []

    rows: list[dict[str, str]] = []
    for line in lines[header_index + 1 :]:
        parts = re.split(r"\s+", line)
        if len(parts) < 8 or not parts[0].isdigit():
            continue
        rows.append(
            {
                "employee_id": parts[0],
                "first_name": parts[1],
                "last_name": parts[2],
                "age": parts[3],
                "state": parts[4],
                "salary": parts[5],
                "class": parts[6],
                "dependent_count": parts[7],
            }
        )
    return rows
