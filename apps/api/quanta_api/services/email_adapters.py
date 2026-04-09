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
        if "payload" in payload:
            return self._parse_gmail_message(payload)
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
            body_text=payload.get("body") or payload.get("snippet", ""),
            attachments=attachments,
        )

    def _parse_gmail_message(self, payload: dict) -> InboundEmailPayload:
        message_payload = payload.get("payload", {})
        headers = {item.get("name", "").lower(): item.get("value", "") for item in message_payload.get("headers", [])}
        body_plain, body_html = self._gmail_bodies(message_payload)
        attachments = [
            EmailAttachmentInput(
                file_name=item.get("filename", "attachment.bin"),
                content_type=item.get("mimeType"),
                content_base64=item.get("data"),
            )
            for item in self._gmail_attachments(message_payload)
        ]
        recipients = [item.strip() for item in headers.get("to", "").split(",") if item.strip()]
        return InboundEmailPayload(
            sender=headers.get("from", "unknown@gmail.local"),
            recipients=recipients,
            subject=headers.get("subject", ""),
            body_raw=body_html or body_plain or payload.get("snippet", ""),
            body_text=body_plain or body_html or payload.get("snippet", ""),
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

    def _gmail_bodies(self, payload: dict) -> tuple[str, str]:
        plain_parts: list[str] = []
        html_parts: list[str] = []
        for part in self._walk_gmail_parts(payload):
            mime_type = part.get("mimeType", "")
            data = part.get("body", {}).get("data")
            if mime_type not in {"text/plain", "text/html"} or not data:
                continue
            decoded = self._decode_websafe_base64(data)
            if mime_type == "text/plain":
                plain_parts.append(decoded)
            else:
                html_parts.append(decoded)
        if not plain_parts and not html_parts:
            body_data = payload.get("body", {}).get("data")
            if body_data:
                return self._decode_websafe_base64(body_data), ""
        return "\n".join(part for part in plain_parts if part).strip(), "\n".join(part for part in html_parts if part).strip()

    def _gmail_attachments(self, payload: dict) -> list[dict]:
        attachments: list[dict] = []
        for part in self._walk_gmail_parts(payload):
            filename = part.get("filename")
            data = part.get("body", {}).get("data")
            if filename and data:
                attachments.append(
                    {
                        "filename": filename,
                        "mimeType": part.get("mimeType"),
                        "data": self._normalize_base64(data),
                    }
                )
        return attachments

    def _walk_gmail_parts(self, payload: dict) -> list[dict]:
        parts: list[dict] = []
        queue = list(payload.get("parts", []))
        while queue:
            current = queue.pop(0)
            parts.append(current)
            queue[0:0] = current.get("parts", [])
        return parts

    def _decode_websafe_base64(self, value: str) -> str:
        return base64.urlsafe_b64decode(value.encode("utf-8")).decode("utf-8", errors="ignore")

    def _normalize_base64(self, value: str) -> str:
        if not value:
            return value
        return base64.b64encode(base64.urlsafe_b64decode(value.encode("utf-8"))).decode("utf-8")
