from __future__ import annotations

import io
import json
import zipfile
from dataclasses import dataclass
from pathlib import PurePosixPath


@dataclass
class FileDetectionResult:
    media_type: str
    extension: str
    matched_by_signature: bool


class FileSniffer:
    SIGNATURES = {
        b"%PDF-": "application/pdf",
        b"\x89PNG\r\n\x1a\n": "image/png",
        b"\xff\xd8\xff": "image/jpeg",
        b"II*\x00": "image/tiff",
        b"MM\x00*": "image/tiff",
    }

    EXTENSION_MEDIA_TYPES = {
        ".csv": "text/csv",
        ".tsv": "text/tab-separated-values",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xls": "application/vnd.ms-excel",
        ".pdf": "application/pdf",
        ".zip": "application/zip",
        ".json": "application/json",
        ".xml": "application/xml",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".tif": "image/tiff",
        ".tiff": "image/tiff",
    }

    def detect(self, file_name: str, content: bytes) -> FileDetectionResult:
        extension = self._extension_for(file_name)
        signature_type = self._detect_by_signature(content)
        if signature_type:
            return FileDetectionResult(signature_type, extension, True)
        if extension == ".xlsx" and self._is_zip(content):
            return FileDetectionResult(self.EXTENSION_MEDIA_TYPES[extension], extension, True)
        if extension == ".zip" and self._looks_like_zip(content):
            return FileDetectionResult(self.EXTENSION_MEDIA_TYPES[extension], extension, True)
        if extension in {".csv", ".tsv"} and self._looks_tabular_text(content, delimiter="\t" if extension == ".tsv" else ","):
            return FileDetectionResult(self.EXTENSION_MEDIA_TYPES[extension], extension, False)
        if extension == ".json" and self._looks_json(content):
            return FileDetectionResult(self.EXTENSION_MEDIA_TYPES[extension], extension, False)
        if extension == ".xml" and self._looks_xml(content):
            return FileDetectionResult(self.EXTENSION_MEDIA_TYPES[extension], extension, False)
        if extension in self.EXTENSION_MEDIA_TYPES:
            return FileDetectionResult(self.EXTENSION_MEDIA_TYPES[extension], extension, False)
        return FileDetectionResult("application/octet-stream", extension, False)

    def media_type_matches(self, declared_type: str | None, detected_type: str) -> bool:
        if not declared_type:
            return True
        declared = declared_type.split(";", 1)[0].strip().lower()
        detected = detected_type.lower()
        if declared == detected:
            return True
        compatibility = {
            "application/octet-stream": True,
            "application/vnd.ms-excel": detected in {"text/csv", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
            "application/zip": detected == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "text/plain": detected in {"text/csv", "text/tab-separated-values", "application/json", "application/xml"},
        }
        return compatibility.get(declared, False)

    def _extension_for(self, file_name: str) -> str:
        suffix = PurePosixPath(file_name).suffix.lower()
        return suffix

    def _detect_by_signature(self, content: bytes) -> str | None:
        for signature, media_type in self.SIGNATURES.items():
            if content.startswith(signature):
                return media_type
        return None

    def _is_zip(self, content: bytes) -> bool:
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                return any(name.startswith("xl/") for name in archive.namelist())
        except zipfile.BadZipFile:
            return False

    def _looks_like_zip(self, content: bytes) -> bool:
        try:
            with zipfile.ZipFile(io.BytesIO(content)):
                return True
        except zipfile.BadZipFile:
            return False

    def _looks_tabular_text(self, content: bytes, delimiter: str) -> bool:
        try:
            text = content[:4096].decode("utf-8-sig")
        except UnicodeDecodeError:
            return False
        lines = [line for line in text.splitlines() if line.strip()]
        return len(lines) >= 1 and delimiter in lines[0]

    def _looks_json(self, content: bytes) -> bool:
        try:
            json.loads(content.decode("utf-8-sig"))
            return True
        except (UnicodeDecodeError, json.JSONDecodeError):
            return False

    def _looks_xml(self, content: bytes) -> bool:
        try:
            text = content[:4096].decode("utf-8-sig").lstrip()
        except UnicodeDecodeError:
            return False
        return text.startswith("<?xml") or text.startswith("<")
