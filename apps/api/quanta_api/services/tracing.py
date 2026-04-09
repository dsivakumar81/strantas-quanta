from __future__ import annotations

import logging
from typing import Any

import httpx

from quanta_api.core.config import Settings


class TraceSinkService:
    def __init__(self, settings: Settings, client: httpx.Client | None = None) -> None:
        self.settings = settings
        self.client = client or httpx.Client(timeout=settings.connector_timeout_seconds)
        self.logger = logging.getLogger("quanta.trace")

    def emit(self, event_name: str, payload: dict[str, Any]) -> None:
        safe_payload = {
            key: value
            for key, value in payload.items()
            if key not in {"email_body_raw", "email_body_text", "attachments", "raw_payload"}
        }
        self.logger.info("trace_event=%s payload=%s", event_name, safe_payload)
        if not self.settings.trace_sink_url:
            return
        try:
            self.client.post(
                self.settings.trace_sink_url,
                json={"event": event_name, "payload": safe_payload},
                headers={"x-quanta-trace-secret": self.settings.trace_sink_secret or ""},
            )
        except httpx.HTTPError:
            self.logger.warning("Trace sink delivery failed for %s", event_name)
