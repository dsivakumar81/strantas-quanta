from __future__ import annotations

from dataclasses import dataclass

from quanta_api.domain.enums import EmailProvider


@dataclass
class ProviderConnectorConfig:
    provider: EmailProvider
    enabled: bool
    mode: str
    description: str


class ProviderConnectorService:
    def scaffold_configs(self) -> list[ProviderConnectorConfig]:
        return [
            ProviderConnectorConfig(
                provider=EmailProvider.microsoft_graph,
                enabled=False,
                mode="webhook_or_polling",
                description="Use Microsoft Graph subscription/webhook or mailbox polling for inbound submission email events.",
            ),
            ProviderConnectorConfig(
                provider=EmailProvider.gmail,
                enabled=False,
                mode="watch_or_polling",
                description="Use Gmail watch notifications or mailbox polling for inbound submission email events.",
            ),
            ProviderConnectorConfig(
                provider=EmailProvider.smtp_webhook,
                enabled=False,
                mode="webhook",
                description="Use SMTP provider inbound parse/webhook delivery for raw email and attachments.",
            ),
        ]
