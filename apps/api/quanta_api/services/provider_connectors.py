from __future__ import annotations

from dataclasses import dataclass

from quanta_api.core.config import Settings
from quanta_api.domain.enums import EmailProvider


@dataclass
class ProviderConnectorConfig:
    provider: EmailProvider
    enabled: bool
    mode: str
    description: str
    configured: bool


class ProviderConnectorService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def scaffold_configs(self) -> list[ProviderConnectorConfig]:
        return [
            ProviderConnectorConfig(
                provider=EmailProvider.microsoft_graph,
                enabled=bool(self.settings.graph_access_token and self.settings.graph_mailbox_user),
                mode="webhook_or_polling",
                description="Use Microsoft Graph subscription/webhook or mailbox polling for inbound submission email events.",
                configured=bool(self.settings.graph_access_token and self.settings.graph_mailbox_user),
            ),
            ProviderConnectorConfig(
                provider=EmailProvider.gmail,
                enabled=bool(self.settings.gmail_access_token),
                mode="watch_or_polling",
                description="Use Gmail watch notifications or mailbox polling for inbound submission email events.",
                configured=bool(self.settings.gmail_access_token),
            ),
            ProviderConnectorConfig(
                provider=EmailProvider.smtp_webhook,
                enabled=bool(self.settings.smtp_webhook_secret),
                mode="webhook",
                description="Use SMTP provider inbound parse/webhook delivery for raw email and attachments.",
                configured=bool(self.settings.smtp_webhook_secret),
            ),
        ]
