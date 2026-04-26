"""S3-compatible object-store backend for Garage/Netcup."""

from __future__ import annotations

import json
import os
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .object_store import ObjectStoreError


@dataclass(frozen=True)
class S3Connection:
    bucket: str
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    region: str
    prefix: str = ""


class S3ObjectStore:
    """S3/Garage object store using a manifest-backed scan."""

    def __init__(
        self,
        client: Any,
        bucket: str,
        prefix: str = "",
    ) -> None:
        self.client = client
        self.bucket = bucket
        self.prefix = _normalize_prefix(prefix)

    @classmethod
    def from_config(cls, config: dict[str, Any], client: Any | None = None) -> "S3ObjectStore":
        connection = s3_connection_from_config(config)
        if client is None:
            client = _build_boto3_client(connection)
        return cls(client=client, bucket=connection.bucket, prefix=connection.prefix)

    def scan(self) -> dict[str, Any]:
        manifest_key = "manifests/s3.json"
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self._remote_key(manifest_key),
            )
        except Exception as exc:
            if _is_missing_object_error(exc):
                return _empty_manifest(manifest_key)
            raise ObjectStoreError(f"failed to read S3 manifest {manifest_key}: {exc}") from exc

        try:
            manifest = json.loads(_read_body(response["Body"]).decode("utf-8"))
        except (KeyError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ObjectStoreError(f"invalid S3 manifest {manifest_key}") from exc
        if not isinstance(manifest, dict):
            raise ObjectStoreError(f"invalid S3 manifest {manifest_key}")
        manifest["backend"] = "s3"
        manifest["manifest_key"] = manifest_key
        return manifest

    def scan_live(self) -> dict[str, Any]:
        items = []
        for entry in self._list_objects("systems/"):
            sync_key = self._strip_prefix(entry["Key"])
            item = self._scan_live_object(sync_key)
            if item is not None:
                items.append(item)
        return {
            "backend": "s3",
            "device": "s3",
            "items": sorted(items, key=lambda item: item["sync_key"]),
            "skipped": [],
            "summary": {
                "item_count": len(items),
                "skipped_count": 0,
                "total_size": sum(item["size"] for item in items),
            },
        }

    def object_exists(self, sync_key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._remote_key(sync_key))
            return True
        except Exception as exc:
            if _is_missing_object_error(exc):
                return False
            raise ObjectStoreError(f"failed to stat S3 object {sync_key}: {exc}") from exc

    def verify_object_fingerprint(self, sync_key: str, item: dict[str, Any], role: str) -> None:
        expected_sha = item.get("native_sha256")
        expected_size = item.get("native_size")
        if expected_sha is None and expected_size is None:
            return
        data = self._get_object_bytes(sync_key)
        actual_size = len(data)
        if expected_size is not None and actual_size != int(expected_size):
            raise ObjectStoreError(
                f"{role} S3 object changed since plan: {sync_key}; "
                f"size {actual_size} != expected {expected_size}"
            )
        if expected_sha is not None:
            actual_sha = hashlib.sha256(data).hexdigest()
            if actual_sha != expected_sha:
                raise ObjectStoreError(
                    f"{role} S3 object changed since plan: {sync_key}; "
                    f"sha256 {actual_sha} != expected {expected_sha}"
                )

    def put_file(self, source_path: Path, sync_key: str) -> None:
        data = source_path.read_bytes()
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self._remote_key(sync_key),
                Body=data,
                Metadata={
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "size": str(len(data)),
                },
            )
        except Exception as exc:
            raise ObjectStoreError(f"failed to upload S3 object {sync_key}: {exc}") from exc

    def copy_object(self, from_sync_key: str, to_sync_key: str) -> None:
        source_key = self._remote_key(from_sync_key)
        target_key = self._remote_key(to_sync_key)
        try:
            self.client.copy_object(
                Bucket=self.bucket,
                CopySource={"Bucket": self.bucket, "Key": source_key},
                Key=target_key,
            )
        except Exception as exc:
            raise ObjectStoreError(
                f"failed to copy S3 object {from_sync_key} -> {to_sync_key}: {exc}"
            ) from exc

    def delete_object(self, sync_key: str) -> None:
        try:
            self.client.delete_object(Bucket=self.bucket, Key=self._remote_key(sync_key))
        except Exception as exc:
            raise ObjectStoreError(f"failed to delete S3 object {sync_key}: {exc}") from exc

    def rename_object(self, from_sync_key: str, to_sync_key: str) -> None:
        self.copy_object(from_sync_key, to_sync_key)
        self.delete_object(from_sync_key)

    def trash_object(
        self,
        sync_key: str,
        origin_device: str,
        timestamp_utc: str | None = None,
    ) -> str:
        timestamp = timestamp_utc or _timestamp_utc()
        trash_key = f"trash/{timestamp}/{origin_device}/{sync_key}"
        self.rename_object(sync_key, trash_key)
        return trash_key

    def backup_object(
        self,
        sync_key: str,
        origin_device: str,
        timestamp_utc: str | None = None,
    ) -> str:
        timestamp = timestamp_utc or _timestamp_utc()
        backup_key = f"backups/{timestamp}/{origin_device}/{sync_key}"
        self.copy_object(sync_key, backup_key)
        return backup_key

    def list_trash(self) -> dict[str, Any]:
        items = []
        for entry in self._list_objects("trash/"):
            sync_key = self._strip_prefix(entry["Key"])
            try:
                parsed = trash_item_from_key(sync_key)
            except ObjectStoreError:
                continue
            items.append(
                {
                    **parsed,
                    "size": int(entry.get("Size", 0)),
                    "etag": str(entry.get("ETag", "")).strip('"'),
                }
            )
        return {
            "backend": "s3",
            "bucket": self.bucket,
            "prefix": self.prefix,
            "items": sorted(items, key=lambda item: item["trash_key"]),
            "summary": {
                "item_count": len(items),
                "total_size": sum(item["size"] for item in items),
            },
        }

    def restore_trash_object(
        self,
        trash_key: str,
        to_sync_key: str | None = None,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        item = trash_item_from_key(trash_key)
        target_key = to_sync_key or item["original_sync_key"]
        require_systems_sync_key(target_key)
        if not self.object_exists(trash_key):
            raise ObjectStoreError(f"trash object not found: {trash_key}")
        backup_key = None
        if self.object_exists(target_key):
            if not overwrite:
                raise ObjectStoreError(
                    f"restore target already exists: {target_key}; pass overwrite to replace it"
                )
            backup_key = self.backup_object(target_key, origin_device="restore")
        self.copy_object(trash_key, target_key)
        self.delete_object(trash_key)
        result = {
            "status": "restored",
            "trash_key": trash_key,
            "restored_sync_key": target_key,
            "origin_device": item["origin_device"],
            "trashed_at_utc": item["trashed_at_utc"],
        }
        if backup_key is not None:
            result["backup_key"] = backup_key
        return result

    def write_manifest(self, manifest: dict[str, Any], key: str = "manifests/s3.json") -> None:
        body = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8") + b"\n"
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self._remote_key(key),
                Body=body,
                ContentType="application/json",
            )
        except Exception as exc:
            raise ObjectStoreError(f"failed to write S3 manifest {key}: {exc}") from exc

    def _list_objects(self, sync_prefix: str) -> list[dict[str, Any]]:
        remote_prefix = self._remote_key(sync_prefix.rstrip("/")) + "/"
        entries: list[dict[str, Any]] = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": remote_prefix}
            if token:
                kwargs["ContinuationToken"] = token
            response = self.client.list_objects_v2(**kwargs)
            entries.extend(response.get("Contents", []))
            if not response.get("IsTruncated"):
                break
            token = response.get("NextContinuationToken")
        return entries

    def _get_object_bytes(self, sync_key: str) -> bytes:
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self._remote_key(sync_key),
            )
        except Exception as exc:
            raise ObjectStoreError(f"failed to read S3 object {sync_key}: {exc}") from exc
        try:
            return _read_body(response["Body"])
        except KeyError as exc:
            raise ObjectStoreError(f"invalid S3 response for object {sync_key}") from exc

    def _scan_live_object(self, sync_key: str) -> dict[str, Any] | None:
        parts = Path(sync_key).parts
        if len(parts) < 4 or parts[0] != "systems":
            return None
        data = self._get_object_bytes(sync_key)
        sha256 = hashlib.sha256(data).hexdigest()
        content_path = str(Path(*parts[3:]))
        return {
            "device": "s3",
            "system": parts[1],
            "type": parts[2],
            "absolute_path": self._remote_key(sync_key),
            "relative_path": sync_key,
            "content_path": content_path,
            "native_content_path": content_path,
            "sync_key": sync_key,
            "size": len(data),
            "native_size": len(data),
            "canonical_size": len(data),
            "modified_ns": 0,
            "sha256": sha256,
            "native_sha256": sha256,
            "canonical_sha256": sha256,
        }

    def _remote_key(self, sync_key: str) -> str:
        sync_key = sync_key.strip("/")
        if not sync_key:
            return self.prefix
        if not self.prefix:
            return sync_key
        return f"{self.prefix}/{sync_key}"

    def _strip_prefix(self, remote_key: str) -> str:
        if not self.prefix:
            return remote_key
        prefix = f"{self.prefix}/"
        if not remote_key.startswith(prefix):
            raise ObjectStoreError(f"S3 key is outside configured prefix: {remote_key}")
        return remote_key[len(prefix) :]


def s3_connection_from_config(config: dict[str, Any]) -> S3Connection:
    s3 = config.get("s3")
    if not isinstance(s3, dict):
        raise ObjectStoreError("missing s3 configuration")
    return S3Connection(
        bucket=_required_config_value(s3, "bucket"),
        endpoint_url=_required_env_value(s3, "endpoint_url_env"),
        access_key_id=_required_env_value(s3, "access_key_id_env"),
        secret_access_key=_required_env_value(s3, "secret_access_key_env"),
        region=str(s3.get("region", "garage")),
        prefix=str(s3.get("prefix", "")),
    )


def trash_item_from_key(trash_key: str) -> dict[str, str]:
    parts = Path(trash_key).parts
    if len(parts) < 5 or parts[0] != "trash":
        raise ObjectStoreError(f"invalid trash key: {trash_key}")
    original_sync_key = str(Path(*parts[3:]))
    original_parts = Path(original_sync_key).parts
    require_systems_sync_key(original_sync_key)
    return {
        "trash_key": trash_key,
        "trashed_at_utc": parts[1],
        "origin_device": parts[2],
        "original_sync_key": original_sync_key,
        "system": original_parts[1],
        "type": original_parts[2],
        "content_path": str(Path(*original_parts[3:])),
    }


def require_systems_sync_key(sync_key: str) -> None:
    parts = Path(sync_key).parts
    if len(parts) < 4 or parts[0] != "systems":
        raise ObjectStoreError(f"invalid systems sync key: {sync_key}")


def _build_boto3_client(connection: S3Connection) -> Any:
    try:
        import boto3  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:
        raise ObjectStoreError(
            "S3 backend requires boto3. From the repo, install with: pip install '.[s3]'"
        ) from exc
    return boto3.client(
        "s3",
        endpoint_url=connection.endpoint_url,
        aws_access_key_id=connection.access_key_id,
        aws_secret_access_key=connection.secret_access_key,
        region_name=connection.region,
    )


def _required_config_value(s3: dict[str, Any], key: str) -> str:
    value = s3.get(key)
    if not value:
        raise ObjectStoreError(f"missing s3 config value: {key}")
    return str(value)


def _required_env_value(s3: dict[str, Any], env_key: str) -> str:
    env_name = _required_config_value(s3, env_key)
    value = os.environ.get(env_name)
    if not value:
        raise ObjectStoreError(f"missing required environment variable: {env_name}")
    return value


def _normalize_prefix(prefix: str) -> str:
    return prefix.strip("/")


def _empty_manifest(manifest_key: str) -> dict[str, Any]:
    return {
        "backend": "s3",
        "device": "s3",
        "items": [],
        "skipped": [
            {
                "path": manifest_key,
                "reason": "missing_manifest",
            }
        ],
        "summary": {
            "item_count": 0,
            "skipped_count": 1,
            "total_size": 0,
        },
        "manifest_key": manifest_key,
    }


def _read_body(body: Any) -> bytes:
    data = body.read() if hasattr(body, "read") else body
    if isinstance(data, str):
        return data.encode("utf-8")
    if isinstance(data, bytes):
        return data
    raise ObjectStoreError("S3 response body is not bytes")


def _is_missing_object_error(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = response.get("Error", {}).get("Code")
        if str(code) in {"NoSuchKey", "404", "NotFound"}:
            return True
    return exc.__class__.__name__ in {"NoSuchKey", "NotFound"}


def _timestamp_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
