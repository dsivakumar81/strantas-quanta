import os

from pydantic import BaseModel, model_validator


class Settings(BaseModel):
    app_name: str = os.getenv("QUANTA_APP_NAME", "STRANTAS QUANTA API")
    app_version: str = os.getenv("QUANTA_APP_VERSION", "0.1.0")
    environment: str = os.getenv("QUANTA_ENVIRONMENT", "local")
    repository_backend: str = os.getenv("QUANTA_REPOSITORY_BACKEND", "memory")
    object_store_backend: str = os.getenv("QUANTA_OBJECT_STORE_BACKEND", "local")
    database_url: str = os.getenv(
        "QUANTA_DATABASE_URL",
        "postgresql+psycopg://quanta:quanta@localhost:5432/quanta",
    )
    object_store_root: str = os.getenv("QUANTA_OBJECT_STORE_ROOT", ".data/object_store")
    s3_bucket_name: str = os.getenv("QUANTA_S3_BUCKET_NAME", "quanta-local")
    s3_endpoint_url: str | None = os.getenv("QUANTA_S3_ENDPOINT_URL")
    s3_region_name: str | None = os.getenv("QUANTA_S3_REGION_NAME")
    s3_access_key_id: str | None = os.getenv("QUANTA_S3_ACCESS_KEY_ID")
    s3_secret_access_key: str | None = os.getenv("QUANTA_S3_SECRET_ACCESS_KEY")
    smtp_webhook_secret: str | None = os.getenv("QUANTA_SMTP_WEBHOOK_SECRET", "quanta-dev-secret")
    provider_webhook_secret: str | None = os.getenv("QUANTA_PROVIDER_WEBHOOK_SECRET", "quanta-provider-secret")
    connector_admin_secret: str | None = os.getenv("QUANTA_CONNECTOR_ADMIN_SECRET", "quanta-admin-secret")
    connector_retry_attempts: int = int(os.getenv("QUANTA_CONNECTOR_RETRY_ATTEMPTS", "3"))
    connector_retry_base_delay_seconds: float = float(os.getenv("QUANTA_CONNECTOR_RETRY_BASE_DELAY_SECONDS", "0.1"))
    connector_worker_enabled: bool = os.getenv("QUANTA_CONNECTOR_WORKER_ENABLED", "false").lower() == "true"
    connector_poll_interval_seconds: int = int(os.getenv("QUANTA_CONNECTOR_POLL_INTERVAL_SECONDS", "60"))
    connector_job_batch_size: int = int(os.getenv("QUANTA_CONNECTOR_JOB_BATCH_SIZE", "5"))
    inbound_queue_lag_alert_threshold_seconds: int = int(os.getenv("QUANTA_INBOUND_QUEUE_LAG_ALERT_THRESHOLD_SECONDS", "300"))
    graph_base_url: str = os.getenv("QUANTA_GRAPH_BASE_URL", "https://graph.microsoft.com/v1.0")
    graph_access_token: str | None = os.getenv("QUANTA_GRAPH_ACCESS_TOKEN")
    graph_mailbox_user: str | None = os.getenv("QUANTA_GRAPH_MAILBOX_USER")
    graph_client_state: str = os.getenv("QUANTA_GRAPH_CLIENT_STATE", "quanta")
    gmail_base_url: str = os.getenv("QUANTA_GMAIL_BASE_URL", "https://gmail.googleapis.com/gmail/v1")
    gmail_access_token: str | None = os.getenv("QUANTA_GMAIL_ACCESS_TOKEN")
    gmail_user_id: str = os.getenv("QUANTA_GMAIL_USER_ID", "me")
    connector_timeout_seconds: float = float(os.getenv("QUANTA_CONNECTOR_TIMEOUT_SECONDS", "20.0"))
    alert_webhook_url: str | None = os.getenv("QUANTA_ALERT_WEBHOOK_URL")
    trace_sink_url: str | None = os.getenv("QUANTA_TRACE_SINK_URL")
    trace_sink_secret: str | None = os.getenv("QUANTA_TRACE_SINK_SECRET")
    metrics_otlp_endpoint: str | None = os.getenv("QUANTA_METRICS_OTLP_ENDPOINT")
    metrics_otlp_secret: str | None = os.getenv("QUANTA_METRICS_OTLP_SECRET")
    max_attachment_size_bytes: int = int(os.getenv("QUANTA_MAX_ATTACHMENT_SIZE_BYTES", str(15 * 1024 * 1024)))
    allowed_attachment_extensions: tuple[str, ...] = (
        ".csv",
        ".tsv",
        ".xlsx",
        ".xls",
        ".pdf",
        ".zip",
        ".json",
        ".xml",
        ".png",
        ".jpg",
        ".jpeg",
        ".tif",
        ".tiff",
    )
    supported_media_types: tuple[str, ...] = (
        "text/csv",
        "text/tab-separated-values",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
        "application/pdf",
        "application/zip",
        "application/json",
        "application/xml",
        "image/png",
        "image/jpeg",
        "image/tiff",
    )

    @model_validator(mode="after")
    def validate_runtime(self) -> "Settings":
        valid_envs = {"local", "local-dev", "dev", "test", "prod"}
        if self.environment not in valid_envs:
            raise ValueError(f"Unsupported QUANTA_ENVIRONMENT '{self.environment}'")
        if self.max_attachment_size_bytes <= 0:
            raise ValueError("QUANTA_MAX_ATTACHMENT_SIZE_BYTES must be positive")
        if self.environment == "prod":
            if self.connector_admin_secret in {None, "", "quanta-admin-secret"}:
                raise ValueError("Production requires QUANTA_CONNECTOR_ADMIN_SECRET to be set to a non-default value")
            if self.smtp_webhook_secret in {None, "", "quanta-dev-secret"}:
                raise ValueError("Production requires QUANTA_SMTP_WEBHOOK_SECRET to be set to a non-default value")
            if self.provider_webhook_secret in {None, "", "quanta-provider-secret"}:
                raise ValueError("Production requires QUANTA_PROVIDER_WEBHOOK_SECRET to be set to a non-default value")
            if self.repository_backend != "postgres":
                raise ValueError("Production requires QUANTA_REPOSITORY_BACKEND=postgres")
            if self.object_store_backend != "s3":
                raise ValueError("Production requires QUANTA_OBJECT_STORE_BACKEND=s3")
            if self.trace_sink_url and not self.trace_sink_secret:
                raise ValueError("Production requires QUANTA_TRACE_SINK_SECRET when QUANTA_TRACE_SINK_URL is set")
            if self.metrics_otlp_endpoint and not self.metrics_otlp_secret:
                raise ValueError("Production requires QUANTA_METRICS_OTLP_SECRET when QUANTA_METRICS_OTLP_ENDPOINT is set")
        return self


settings = Settings()
