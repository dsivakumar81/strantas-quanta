from __future__ import annotations

import os
import subprocess
from pathlib import Path

import boto3
import httpx
import psycopg
import pytest
from fastapi.testclient import TestClient

from quanta_api.bootstrap import build_service_container
from quanta_api.core.config import Settings
from quanta_api.main import create_app

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session", autouse=True)
def ensure_dev_stack() -> None:
    subprocess.run(["docker", "compose", "up", "-d"], cwd=ROOT, check=True)
    env = os.environ.copy()
    env.update(
        {
            "QUANTA_DATABASE_URL": "postgresql+psycopg://quanta:quanta@127.0.0.1:5432/quanta",
            "QUANTA_DATABASE_DSN": "postgresql://quanta:quanta@127.0.0.1:5432/quanta",
        }
    )
    subprocess.run(["python", "scripts/reset_dev_db.py"], cwd=ROOT, env=env, check=True)
    subprocess.run(["alembic", "upgrade", "head"], cwd=ROOT, env=env, check=True)
    subprocess.run(["python", "scripts/generate_sample_pdf.py"], cwd=ROOT, check=True)
    subprocess.run(["python", "scripts/generate_sample_xlsx.py"], cwd=ROOT, check=True)
    subprocess.run(["python", "scripts/generate_scanned_pdf.py"], cwd=ROOT, check=True)


@pytest.fixture
def integration_settings() -> Settings:
    return Settings(
        repository_backend="postgres",
        object_store_backend="s3",
        database_url="postgresql+psycopg://quanta:quanta@127.0.0.1:5432/quanta",
        s3_bucket_name="quanta-local",
        s3_endpoint_url="http://127.0.0.1:9100",
        s3_region_name="us-east-1",
        s3_access_key_id="minio",
        s3_secret_access_key="minio123",
        connector_admin_secret="quanta-admin-secret",
    )


@pytest.fixture(autouse=True)
def reset_backends(integration_settings: Settings) -> None:
    with psycopg.connect("postgresql://quanta:quanta@127.0.0.1:5432/quanta") as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE id_counters, idempotency_keys, replay_audits, alerts, jobs, inbound_mailboxes, connector_cursors, outputs, census, lobs, quotes, submissions RESTART IDENTITY CASCADE")
        connection.commit()

    client = boto3.client(
        "s3",
        endpoint_url=integration_settings.s3_endpoint_url,
        region_name=integration_settings.s3_region_name,
        aws_access_key_id=integration_settings.s3_access_key_id,
        aws_secret_access_key=integration_settings.s3_secret_access_key,
    )
    response = client.list_objects_v2(Bucket=integration_settings.s3_bucket_name)
    if response.get("Contents"):
        client.delete_objects(
            Bucket=integration_settings.s3_bucket_name,
            Delete={"Objects": [{"Key": item["Key"]} for item in response["Contents"]]},
        )


@pytest.fixture
def client(integration_settings: Settings) -> TestClient:
    app = create_app(container=build_service_container(integration_settings))
    return TestClient(app)


@pytest.fixture
def connector_settings(integration_settings: Settings) -> Settings:
    return integration_settings.model_copy(
        update={
            "graph_access_token": "graph-token",
            "graph_mailbox_user": "broker-mailbox@example.com",
            "gmail_access_token": "gmail-token",
            "gmail_user_id": "me",
        }
    )


@pytest.fixture
def connector_client(connector_settings: Settings) -> TestClient:
    container = build_service_container(connector_settings)
    app = create_app(container=container)
    return TestClient(app)


@pytest.fixture
def mock_transport_factory():
    def factory(handler):
        return httpx.Client(transport=httpx.MockTransport(handler), timeout=20.0)

    return factory
