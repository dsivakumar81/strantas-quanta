import os

from pydantic import BaseModel


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


settings = Settings()
