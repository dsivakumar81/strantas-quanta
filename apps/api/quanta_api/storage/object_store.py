from __future__ import annotations

from pathlib import Path

import boto3

from quanta_api.domain.repositories import ObjectStore


class LocalObjectStore(ObjectStore):
    def __init__(self, root: str) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def put_bytes(self, storage_key: str, content: bytes, content_type: str | None = None) -> str:
        path = self.root / storage_key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return storage_key

    def get_bytes(self, storage_key: str) -> bytes:
        return (self.root / storage_key).read_bytes()


class S3ObjectStore(ObjectStore):
    def __init__(
        self,
        bucket_name: str,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
    ) -> None:
        self.bucket_name = bucket_name
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region_name,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )
        self._ensure_bucket()

    def put_bytes(self, storage_key: str, content: bytes, content_type: str | None = None) -> str:
        kwargs = {"Bucket": self.bucket_name, "Key": storage_key, "Body": content}
        if content_type:
            kwargs["ContentType"] = content_type
        self.client.put_object(**kwargs)
        return storage_key

    def get_bytes(self, storage_key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket_name, Key=storage_key)
        return response["Body"].read()

    def _ensure_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
        except Exception:
            self.client.create_bucket(Bucket=self.bucket_name)
