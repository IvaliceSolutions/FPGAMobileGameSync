"""Local object-store backend used to simulate S3 safely."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
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
    sync_key: str
    size: int
    modified_ns: int
    sha256: str


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
    return ObjectItem(
        device="s3",
        system=system,
        type=content_type,
        absolute_path=str(path),
        relative_path=relative_path,
        content_path=content_path,
        sync_key=relative_path,
        size=stat.st_size,
        modified_ns=stat.st_mtime_ns,
        sha256=_sha256(path),
    )


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
