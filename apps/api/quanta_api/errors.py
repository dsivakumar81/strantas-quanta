from __future__ import annotations


class QuantaError(Exception):
    def __init__(self, message: str, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class InputValidationError(QuantaError):
    pass


class ConflictError(QuantaError):
    pass
