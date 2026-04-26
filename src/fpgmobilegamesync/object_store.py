"""Local object-store backend used to simulate S3 safely."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


class ObjectStoreError(Exception):
    """Raised when an object-store operation fails."""


@dataclass(frozen=True)
class ObjectItem:
    device: str
    system: str
    type: str
    absolute_path: str
    relative_path: str
    content_path: str
    native_content_path: str
    sync_key: str
    size: int
    native_size: int
    canonical_size: int
    modified_ns: int
    sha256: str
    native_sha256: str
    canonical_sha256: str


class LocalObjectStore:
    """Filesystem-backed object store with S3-like copy/delete semantics."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def scan(self) -> dict[str, Any]:
        items = [_scan_object(self.root, path) for path in _walk_files(self.root)]
        valid_items = [item for item in items if item is not None]
        return {
            "device": "s3",
            "items": [
                asdict(item)
                for item in sorted(valid_items, key=lambda item: item.sync_key)
            ],
            "skipped": [],
            "summary": {
                "item_count": len(valid_items),
                "skipped_count": 0,
                "total_size": sum(item.size for item in valid_items),
            },
        }

    def put_file(self, source_path: Path, sync_key: str) -> None:
        target_path = self._object_path(sync_key)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)

    def object_exists(self, sync_key: str) -> bool:
        return self._object_path(sync_key).exists()

    def verify_object_fingerprint(self, sync_key: str, item: dict[str, Any], role: str) -> None:
        _verify_file_fingerprint(self._object_path(sync_key), item, role)

    def copy_object(self, from_sync_key: str, to_sync_key: str) -> None:
        source_path = self._object_path(from_sync_key)
        if not source_path.exists():
            raise ObjectStoreError(f"object not found: {from_sync_key}")
        target_path = self._object_path(to_sync_key)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)

    def delete_object(self, sync_key: str) -> None:
        path = self._object_path(sync_key)
        if path.exists():
            path.unlink()
            _prune_empty_dirs(path.parent, self.root)

    def rename_object(self, from_sync_key: str, to_sync_key: str) -> None:
        source_path = self._object_path(from_sync_key)
        target_path = self._object_path(to_sync_key)
        if source_path.exists() and _same_filesystem_entry(source_path, target_path):
            target_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = source_path.with_name(
                f".{source_path.name}.case-rename-{uuid.uuid4().hex}.tmp"
            )
            source_path.rename(temp_path)
            temp_path.rename(target_path)
            return
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
        for path in _walk_files(self.root / "trash"):
            item = _scan_trash_object(self.root, path)
            if item is not None:
                items.append(item)
        return {
            "device": "s3",
            "trash_root": str(self.root / "trash"),
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
        item = _trash_item_from_key(self.root, trash_key)
        target_key = to_sync_key or item["original_sync_key"]
        _require_systems_sync_key(target_key)
        source_path = self._object_path(trash_key)
        if not source_path.exists():
            raise ObjectStoreError(f"trash object not found: {trash_key}")
        target_path = self._object_path(target_key)
        backup_key = None
        if target_path.exists() and not overwrite:
            raise ObjectStoreError(
                f"restore target already exists: {target_key}; pass overwrite to replace it"
            )
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if target_path.exists():
            backup_key = self.backup_object(target_key, origin_device="restore")
            target_path.unlink()
        shutil.move(str(source_path), str(target_path))
        _prune_empty_dirs(source_path.parent, self.root)
        stat = target_path.stat()
        sha256 = _sha256(target_path)
        result = {
            "status": "restored",
            "trash_key": trash_key,
            "restored_sync_key": target_key,
            "origin_device": item["origin_device"],
            "trashed_at_utc": item["trashed_at_utc"],
            "size": stat.st_size,
            "sha256": sha256,
        }
        if backup_key is not None:
            result["backup_key"] = backup_key
        return result

    def write_manifest(self, manifest: dict[str, Any], key: str = "manifests/s3.json") -> None:
        target_path = self._object_path(key)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with target_path.open("w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
            handle.write("\n")

    def _object_path(self, sync_key: str) -> Path:
        path = self.root / sync_key
        try:
            path.resolve().relative_to(self.root.resolve())
        except ValueError as exc:
            raise ObjectStoreError(f"object key escapes store root: {sync_key}") from exc
        return path


def _scan_object(root: Path, path: Path) -> ObjectItem | None:
    relative_path = str(path.relative_to(root))
    parts = Path(relative_path).parts
    if len(parts) < 4 or parts[0] != "systems":
        return None

    system = parts[1]
    content_type = parts[2]
    content_path = str(Path(*parts[3:]))
    stat = path.stat()
    sha256 = _sha256(path)
    return ObjectItem(
        device="s3",
        system=system,
        type=content_type,
        absolute_path=str(path),
        relative_path=relative_path,
        content_path=content_path,
        native_content_path=content_path,
        sync_key=relative_path,
        size=stat.st_size,
        native_size=stat.st_size,
        canonical_size=stat.st_size,
        modified_ns=stat.st_mtime_ns,
        sha256=sha256,
        native_sha256=sha256,
        canonical_sha256=sha256,
    )


def _scan_trash_object(root: Path, path: Path) -> dict[str, Any] | None:
    relative_path = str(path.relative_to(root))
    try:
        item = _trash_item_from_key(root, relative_path)
    except ObjectStoreError:
        return None
    stat = path.stat()
    sha256 = _sha256(path)
    return {
        **item,
        "absolute_path": str(path),
        "size": stat.st_size,
        "sha256": sha256,
    }


def _trash_item_from_key(root: Path, trash_key: str) -> dict[str, str]:
    parts = Path(trash_key).parts
    if len(parts) < 5 or parts[0] != "trash":
        raise ObjectStoreError(f"invalid trash key: {trash_key}")
    original_sync_key = str(Path(*parts[3:]))
    original_parts = Path(original_sync_key).parts
    if len(original_parts) < 4 or original_parts[0] != "systems":
        raise ObjectStoreError(f"invalid original sync key in trash key: {trash_key}")
    _require_systems_sync_key(original_sync_key)
    trash_path = (root / trash_key).resolve()
    try:
        trash_path.relative_to(root.resolve())
    except ValueError as exc:
        raise ObjectStoreError(f"trash key escapes store root: {trash_key}") from exc
    return {
        "trash_key": trash_key,
        "trashed_at_utc": parts[1],
        "origin_device": parts[2],
        "original_sync_key": original_sync_key,
        "system": original_parts[1],
        "type": original_parts[2],
        "content_path": str(Path(*original_parts[3:])),
    }


def _require_systems_sync_key(sync_key: str) -> None:
    parts = Path(sync_key).parts
    if len(parts) < 4 or parts[0] != "systems":
        raise ObjectStoreError(f"invalid systems sync key: {sync_key}")


def _walk_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []
    paths: list[Path] = []
    for current_root, _dirnames, filenames in os.walk(root):
        for filename in filenames:
            paths.append(Path(current_root) / filename)
    return paths


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_file_fingerprint(path: Path, item: dict[str, Any], role: str) -> None:
    expected_sha = item.get("native_sha256")
    expected_size = item.get("native_size")
    if expected_sha is None and expected_size is None:
        return
    actual_size = path.stat().st_size
    if expected_size is not None and actual_size != int(expected_size):
        raise ObjectStoreError(
            f"{role} object changed since plan: {path}; "
            f"size {actual_size} != expected {expected_size}"
        )
    if expected_sha is not None:
        actual_sha = _sha256(path)
        if actual_sha != expected_sha:
            raise ObjectStoreError(
                f"{role} object changed since plan: {path}; "
                f"sha256 {actual_sha} != expected {expected_sha}"
            )


def _same_filesystem_entry(source_path: Path, target_path: Path) -> bool:
    try:
        return source_path.samefile(target_path)
    except FileNotFoundError:
        return False


def _timestamp_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


def _prune_empty_dirs(start: Path, stop: Path) -> None:
    current = start
    stop = stop.resolve()
    while current.exists() and current.resolve() != stop:
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent
