from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx

from quanta_api.core.config import Settings
from quanta_api.domain.enums import AlertSeverity, EmailProvider, JobType
from quanta_api.domain.models import ConnectorCursor, InboundMailboxConfig
from quanta_api.domain.repositories import OperationsRepository
from quanta_api.services.alerts import AlertService
from quanta_api.services.job_queue import JobQueueService
from quanta_api.services.metrics import MetricsService
from quanta_api.services.retry import RetryService


class ConnectorOrchestrationService:
    def __init__(
        self,
        settings: Settings,
        operations_repository: OperationsRepository,
        retry_service: RetryService,
        metrics: MetricsService,
        alert_service: AlertService,
        job_queue: JobQueueService,
        graph_client: httpx.Client,
        gmail_client: httpx.Client,
    ) -> None:
        self.settings = settings
        self.operations_repository = operations_repository
        self.retry_service = retry_service
        self.metrics = metrics
        self.alert_service = alert_service
        self.job_queue = job_queue
        self.graph_client = graph_client
        self.gmail_client = gmail_client

    def _graph_mailbox(self, tenant_id: str) -> InboundMailboxConfig | None:
        mailbox = self.operations_repository.get_mailbox(EmailProvider.microsoft_graph, tenant_id=tenant_id)
        return mailbox if mailbox and mailbox.enabled else None

    def _gmail_mailbox(self, tenant_id: str) -> InboundMailboxConfig | None:
        mailbox = self.operations_repository.get_mailbox(EmailProvider.gmail, tenant_id=tenant_id)
        return mailbox if mailbox and mailbox.enabled else None

    def refresh_graph_subscription(self, tenant_id: str = "default") -> ConnectorCursor:
        mailbox = self._graph_mailbox(tenant_id)
        access_token = mailbox.access_token if mailbox else self.settings.graph_access_token
        mailbox_user = mailbox.provider_user_id if mailbox else self.settings.graph_mailbox_user
        mailbox_address = mailbox.mailbox_address if mailbox else mailbox_user
        if not access_token or not mailbox_user:
            raise ValueError("Microsoft Graph connector is not configured")

        def operation() -> ConnectorCursor:
            response = self.graph_client.post(
                f"{self.settings.graph_base_url}/subscriptions",
                headers={"Authorization": f"Bearer {access_token}"},
                json={
                    "changeType": "created",
                    "notificationUrl": "https://example.invalid/quanta/graph",
                    "resource": f"/users/{mailbox_user}/mailFolders('Inbox')/messages",
                    "expirationDateTime": (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat(),
                    "clientState": self.settings.graph_client_state,
                },
            )
            response.raise_for_status()
            payload = response.json()
            cursor = ConnectorCursor(
                tenant_id=tenant_id,
                provider=EmailProvider.microsoft_graph,
                subscription_id=payload.get("id"),
                subscription_expires_at=datetime.fromisoformat(payload["expirationDateTime"].replace("Z", "+00:00"))
                if payload.get("expirationDateTime")
                else None,
                status="subscribed",
                warnings=[] if mailbox_address else ["Mailbox address not recorded"],
            )
            return self.operations_repository.save_cursor(cursor)

        return self.retry_service.run(operation)

    def refresh_gmail_watch(self, tenant_id: str = "default") -> ConnectorCursor:
        mailbox = self._gmail_mailbox(tenant_id)
        access_token = mailbox.access_token if mailbox else self.settings.gmail_access_token
        user_id = mailbox.provider_user_id if mailbox else self.settings.gmail_user_id
        if not access_token:
            raise ValueError("Gmail connector is not configured")

        def operation() -> ConnectorCursor:
            response = self.gmail_client.post(
                f"{self.settings.gmail_base_url}/users/{user_id}/watch",
                headers={"Authorization": f"Bearer {access_token}"},
                json={"topicName": "projects/quanta/topics/inbound-mail"},
            )
            response.raise_for_status()
            payload = response.json()
            cursor = ConnectorCursor(
                tenant_id=tenant_id,
                provider=EmailProvider.gmail,
                subscription_id=payload.get("historyId"),
                cursor=payload.get("historyId"),
                subscription_expires_at=datetime.fromtimestamp(int(payload["expiration"]) / 1000, tz=timezone.utc)
                if payload.get("expiration")
                else None,
                status="watching",
            )
            return self.operations_repository.save_cursor(cursor)

        return self.retry_service.run(operation)

    def poll_graph_messages(self, tenant_id: str = "default") -> dict[str, object]:
        cursor = self.operations_repository.get_cursor(EmailProvider.microsoft_graph, tenant_id=tenant_id) or ConnectorCursor(provider=EmailProvider.microsoft_graph, tenant_id=tenant_id)
        mailbox = self._graph_mailbox(tenant_id)
        access_token = mailbox.access_token if mailbox else self.settings.graph_access_token
        mailbox_user = mailbox.provider_user_id if mailbox else self.settings.graph_mailbox_user
        if not access_token or not mailbox_user:
            raise ValueError("Microsoft Graph connector is not configured")

        def operation() -> dict[str, object]:
            params = {"$top": 10, "$select": "id,receivedDateTime"}
            if cursor.last_polled_at is not None:
                params["$filter"] = f"receivedDateTime ge {cursor.last_polled_at.isoformat()}"
            response = self.graph_client.get(
                f"{self.settings.graph_base_url}/users/{mailbox_user}/mailFolders/inbox/messages",
                headers={"Authorization": f"Bearer {access_token}"},
                params=params,
            )
            response.raise_for_status()
            return response.json()

        payload = self.retry_service.run(operation)
        message_ids = [item["id"] for item in payload.get("value", []) if item.get("id")]
        for message_id in message_ids:
            self.job_queue.enqueue_connector_ingest(provider=EmailProvider.microsoft_graph, message_id=message_id, tenant_id=tenant_id, run_pipeline=True)
        cursor.last_polled_at = datetime.now(timezone.utc)
        cursor.last_message_id = message_ids[0] if message_ids else cursor.last_message_id
        cursor.status = "polled"
        self.operations_repository.save_cursor(cursor)
        self.metrics.increment("connector.graph.polled")
        return {"provider": EmailProvider.microsoft_graph.value, "tenantId": tenant_id, "queued": len(message_ids), "messageIds": message_ids}

    def poll_gmail_messages(self, tenant_id: str = "default", history_id: str | None = None) -> dict[str, object]:
        cursor = self.operations_repository.get_cursor(EmailProvider.gmail, tenant_id=tenant_id) or ConnectorCursor(provider=EmailProvider.gmail, tenant_id=tenant_id)
        mailbox = self._gmail_mailbox(tenant_id)
        access_token = mailbox.access_token if mailbox else self.settings.gmail_access_token
        user_id = mailbox.provider_user_id if mailbox else self.settings.gmail_user_id
        if not access_token:
            raise ValueError("Gmail connector is not configured")
        effective_history_id = history_id or cursor.cursor

        def operation() -> dict[str, object]:
            if effective_history_id:
                response = self.gmail_client.get(
                    f"{self.settings.gmail_base_url}/users/{user_id}/history",
                    headers={"Authorization": f"Bearer {access_token}"},
                    params={"startHistoryId": effective_history_id, "historyTypes": "messageAdded", "maxResults": 50},
                )
            else:
                response = self.gmail_client.get(
                    f"{self.settings.gmail_base_url}/users/{user_id}/messages",
                    headers={"Authorization": f"Bearer {access_token}"},
                    params={"maxResults": 10},
                )
            response.raise_for_status()
            return response.json()

        payload = self.retry_service.run(operation)
        if effective_history_id:
            message_ids: list[str] = []
            seen: set[str] = set()
            for item in payload.get("history", []):
                for added in item.get("messagesAdded", []):
                    message = added.get("message", {})
                    message_id = message.get("id")
                    if message_id and message_id not in seen:
                        seen.add(message_id)
                        message_ids.append(message_id)
            cursor.cursor = payload.get("historyId", effective_history_id)
        else:
            message_ids = [item["id"] for item in payload.get("messages", []) if item.get("id")]
            if payload.get("historyId"):
                cursor.cursor = payload.get("historyId")
        for message_id in message_ids:
            self.job_queue.enqueue_connector_ingest(provider=EmailProvider.gmail, message_id=message_id, tenant_id=tenant_id, run_pipeline=True)
        cursor.last_polled_at = datetime.now(timezone.utc)
        cursor.last_message_id = message_ids[0] if message_ids else cursor.last_message_id
        cursor.status = "polled"
        self.operations_repository.save_cursor(cursor)
        self.metrics.increment("connector.gmail.polled")
        if not message_ids:
            self.alert_service.emit(AlertSeverity.info, "connector_poll", "No new Gmail messages found", {"provider": "gmail"}, tenant_id=tenant_id)
        return {
            "provider": EmailProvider.gmail.value,
            "tenantId": tenant_id,
            "queued": len(message_ids),
            "messageIds": message_ids,
            "historyId": cursor.cursor,
            "incremental": bool(effective_history_id),
        }

    def list_cursors(self, tenant_id: str | None = None) -> list[ConnectorCursor]:
        return self.operations_repository.list_cursors(tenant_id=tenant_id)

    def ensure_connector_readiness(self, tenant_id: str = "default") -> list[ConnectorCursor]:
        refreshed: list[ConnectorCursor] = []
        now = datetime.now(timezone.utc)
        graph_cursor = self.operations_repository.get_cursor(EmailProvider.microsoft_graph, tenant_id=tenant_id)
        graph_mailbox = self._graph_mailbox(tenant_id)
        graph_ready = (graph_mailbox and graph_mailbox.mode != "polling") or (self.settings.graph_access_token and self.settings.graph_mailbox_user)
        if graph_ready:
            if graph_cursor is None or graph_cursor.subscription_expires_at is None or graph_cursor.subscription_expires_at <= now + timedelta(minutes=5):
                refreshed.append(self.refresh_graph_subscription(tenant_id=tenant_id))
        gmail_cursor = self.operations_repository.get_cursor(EmailProvider.gmail, tenant_id=tenant_id)
        gmail_mailbox = self._gmail_mailbox(tenant_id)
        gmail_ready = (gmail_mailbox and gmail_mailbox.mode != "polling") or self.settings.gmail_access_token
        if gmail_ready:
            if gmail_cursor is None or gmail_cursor.subscription_expires_at is None or gmail_cursor.subscription_expires_at <= now + timedelta(minutes=5):
                refreshed.append(self.refresh_gmail_watch(tenant_id=tenant_id))
        return refreshed
