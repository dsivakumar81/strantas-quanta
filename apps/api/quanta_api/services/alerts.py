from __future__ import annotations

import logging

import httpx

from quanta_api.core.config import Settings
from quanta_api.domain.enums import AlertSeverity
from quanta_api.domain.models import AlertEvent
from quanta_api.domain.repositories import OperationsRepository
from quanta_api.services.id_factory import IdFactory

logger = logging.getLogger("quanta.alerts")


class AlertService:
    def __init__(
        self,
        operations_repository: OperationsRepository,
        ids: IdFactory,
        settings: Settings,
        client: httpx.Client | None = None,
    ) -> None:
        self.operations_repository = operations_repository
        self.ids = ids
        self.settings = settings
        self.client = client or httpx.Client(timeout=10.0)

    def emit(self, severity: AlertSeverity, source: str, message: str, context: dict | None = None, tenant_id: str = "default") -> AlertEvent:
        alert = AlertEvent(
            alert_id=self.ids.next_alert_id(),
            tenant_id=tenant_id,
            severity=severity,
            source=source,
            message=message,
            context=context or {},
        )
        saved = self.operations_repository.save_alert(alert)
        self._log(saved)
        self._notify_webhook(saved)
        return saved

    def list_alerts(self, limit: int = 50, tenant_id: str | None = None) -> list[AlertEvent]:
        return self.operations_repository.list_alerts(limit=limit, tenant_id=tenant_id)

    def _log(self, alert: AlertEvent) -> None:
        log_method = {
            AlertSeverity.info: logger.info,
            AlertSeverity.warning: logger.warning,
            AlertSeverity.error: logger.error,
        }[alert.severity]
        log_method("%s: %s", alert.source, alert.message)

    def _notify_webhook(self, alert: AlertEvent) -> None:
        if not self.settings.alert_webhook_url:
            return
        try:
            response = self.client.post(self.settings.alert_webhook_url, json=alert.model_dump(mode="json"))
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Alert webhook delivery failed: %s", exc)
