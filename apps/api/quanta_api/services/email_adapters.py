from __future__ import annotations

import base64

from quanta_api.domain.contracts import EmailAttachmentInput, InboundEmailPayload
from quanta_api.domain.enums import EmailProvider


class EmailAdapterService:
    def parse(self, provider: EmailProvider, payload: dict) -> InboundEmailPayload:
        parser = {
            EmailProvider.manual: self._parse_manual,
            EmailProvider.microsoft_graph: self._parse_graph,
            EmailProvider.gmail: self._parse_gmail,
            EmailProvider.smtp_webhook: self._parse_smtp_webhook,
        }[provider]
        return parser(payload)

    def _parse_manual(self, payload: dict) -> InboundEmailPayload:
        return InboundEmailPayload.model_validate(payload)

    def _parse_graph(self, payload: dict) -> InboundEmailPayload:
        attachments = [
            EmailAttachmentInput(
                file_name=item.get("name", "attachment.bin"),
                content_type=item.get("contentType"),
                content_base64=item.get("contentBytes"),
            )
            for item in payload.get("attachments", [])
        ]
        return InboundEmailPayload(
            sender=payload.get("from", {}).get("emailAddress", {}).get("address", "unknown@graph.local"),
            recipients=[item.get("emailAddress", {}).get("address", "") for item in payload.get("toRecipients", []) if item.get("emailAddress", {}).get("address")],
            subject=payload.get("subject", ""),
            body_raw=payload.get("body", {}).get("content", ""),
            body_text=payload.get("bodyPreview", payload.get("body", {}).get("content", "")),
            attachments=attachments,
        )

    def _parse_gmail(self, payload: dict) -> InboundEmailPayload:
        headers = {item.get("name", "").lower(): item.get("value", "") for item in payload.get("headers", [])}
        attachments = [
            EmailAttachmentInput(
                file_name=item.get("filename", "attachment.bin"),
                content_type=item.get("mimeType"),
                content_base64=item.get("data"),
            )
            for item in payload.get("attachments", [])
        ]
        recipients = [item.strip() for item in headers.get("to", "").split(",") if item.strip()]
        return InboundEmailPayload(
            sender=headers.get("from", "unknown@gmail.local"),
            recipients=recipients,
            subject=headers.get("subject", ""),
            body_raw=payload.get("body", ""),
            body_text=payload.get("snippet", payload.get("body", "")),
            attachments=attachments,
        )

    def _parse_smtp_webhook(self, payload: dict) -> InboundEmailPayload:
        attachments = []
        for item in payload.get("attachments", []):
            content = item.get("content") or item.get("content_base64")
            content_base64 = base64.b64encode(content.encode()).decode() if isinstance(content, str) and not self._looks_base64(content) else content
            attachments.append(
                EmailAttachmentInput(
                    file_name=item.get("filename", item.get("file_name", "attachment.bin")),
                    content_type=item.get("content_type", item.get("content_type")),
                    content_base64=content_base64,
                )
            )
        recipients = payload.get("to", payload.get("to_emails", []))
        if isinstance(recipients, str):
            recipients = [item.strip() for item in recipients.split(",") if item.strip()]
        return InboundEmailPayload(
            sender=payload.get("from", payload.get("from_email", "unknown@smtp.local")),
            recipients=recipients,
            subject=payload.get("subject", ""),
            body_raw=payload.get("html", payload.get("text", "")),
            body_text=payload.get("text", payload.get("html", "")),
            attachments=attachments,
        )

    def _looks_base64(self, value: str) -> bool:
        try:
            base64.b64decode(value, validate=True)
            return True
        except Exception:
            return False
