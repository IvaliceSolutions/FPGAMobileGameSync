"""Plan execution."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from .object_store import LocalObjectStore


class ApplyError(Exception):
    """Raised when a plan cannot be applied."""


def load_plan(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ApplyError(f"plan not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict) or not isinstance(data.get("actions"), list):
        raise ApplyError(f"invalid plan: {path}")
    return data


def apply_plan_to_local_store(
    plan: dict[str, Any],
    store_root: Path,
    timestamp_utc: str | None = None,
    allow_conflicts: bool = False,
) -> dict[str, Any]:
    store = LocalObjectStore(store_root)
    applied: list[dict[str, Any]] = []

    for action in plan["actions"]:
        applied.append(
            _apply_action(
                store=store,
                action=action,
                origin_device=str(plan.get("source", "source")),
                timestamp_utc=timestamp_utc,
                allow_conflicts=allow_conflicts,
            )
        )

    manifest = store.scan()
    store.write_manifest(manifest)

    return {
        "backend": "local",
        "store_root": str(store_root),
        "applied": applied,
        "summary": _summary(applied),
        "manifest_written": "manifests/s3.json",
    }


def apply_plan_to_local_target(
    plan: dict[str, Any],
    target_root: Path,
    trash_root: Path | None = None,
    timestamp_utc: str | None = None,
    allow_conflicts: bool = False,
) -> dict[str, Any]:
    trash_base = trash_root or (target_root / ".sync_trash")
    applied: list[dict[str, Any]] = []

    for action in plan["actions"]:
        applied.append(
            _apply_local_action(
                action=action,
                target_root=target_root,
                trash_root=trash_base,
                origin_device=str(plan.get("source", "source")),
                timestamp_utc=timestamp_utc,
                allow_conflicts=allow_conflicts,
            )
        )

    return {
        "backend": "local-target",
        "target_root": str(target_root),
        "trash_root": str(trash_base),
        "applied": applied,
        "summary": _summary(applied),
    }


def _apply_action(
    store: LocalObjectStore,
    action: dict[str, Any],
    origin_device: str,
    timestamp_utc: str | None,
    allow_conflicts: bool,
) -> dict[str, Any]:
    operation = action["operation"]

    if operation == "noop":
        return {"operation": operation, "status": "skipped", "reason": action["reason"]}
    if operation == "conflict":
        if not allow_conflicts:
            raise ApplyError("plan contains conflicts; refusing to apply")
        return {"operation": operation, "status": "skipped", "reason": action["reason"]}
    if operation == "upload":
        return _upload(store, action, origin_device, timestamp_utc)
    if operation == "rename_remote":
        return _rename_remote(store, action)
    if operation == "trash_remote":
        return _trash_remote(store, action, origin_device, timestamp_utc)

    raise ApplyError(f"unsupported operation for local object store: {operation}")


def _apply_local_action(
    action: dict[str, Any],
    target_root: Path,
    trash_root: Path,
    origin_device: str,
    timestamp_utc: str | None,
    allow_conflicts: bool,
) -> dict[str, Any]:
    operation = action["operation"]

    if operation == "noop":
        return {"operation": operation, "status": "skipped", "reason": action["reason"]}
    if operation == "conflict":
        if not allow_conflicts:
            raise ApplyError("plan contains conflicts; refusing to apply")
        return {"operation": operation, "status": "skipped", "reason": action["reason"]}
    if operation == "download":
        return _download(action, target_root, trash_root, origin_device, timestamp_utc)
    if operation == "rename_local":
        return _rename_local(action, target_root)
    if operation == "trash_local":
        return _trash_local(action, target_root, trash_root, origin_device, timestamp_utc)

    raise ApplyError(f"unsupported operation for local target: {operation}")


def _upload(
    store: LocalObjectStore,
    action: dict[str, Any],
    origin_device: str,
    timestamp_utc: str | None,
) -> dict[str, Any]:
    source = action["source"]
    source_path = Path(source["absolute_path"])
    if not source_path.exists():
        raise ApplyError(f"source file not found: {source_path}")

    target_key = _target_sync_key(action)
    backup_key = None
    if action.get("backup_target_before_apply") and store.object_exists(target_key):
        backup_key = store.backup_object(
            target_key,
            origin_device=origin_device,
            timestamp_utc=timestamp_utc,
        )
    store.put_file(source_path, target_key)

    result = {
        "operation": "upload",
        "status": "applied",
        "sync_key": target_key,
    }
    if backup_key is not None:
        result["backup_key"] = backup_key
    return result


def _download(
    action: dict[str, Any],
    target_root: Path,
    trash_root: Path,
    origin_device: str,
    timestamp_utc: str | None,
) -> dict[str, Any]:
    source = action["source"]
    source_path = Path(source["absolute_path"])
    if not source_path.exists():
        raise ApplyError(f"source file not found: {source_path}")

    target_path = _target_path_for_download(action, target_root)
    backup_path = None
    if action.get("backup_target_before_apply") and target_path.exists():
        backup_path = _backup_local_file(
            target_path,
            target_root,
            trash_root,
            origin_device,
            timestamp_utc,
        )
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, target_path)

    result = {
        "operation": "download",
        "status": "applied",
        "path": str(target_path),
    }
    if backup_path is not None:
        result["backup_path"] = str(backup_path)
    return result


def _rename_remote(store: LocalObjectStore, action: dict[str, Any]) -> dict[str, Any]:
    source_key = action["source"]["sync_key"]
    target_key = action["target"]["sync_key"]
    store.rename_object(target_key, source_key)
    return {
        "operation": "rename_remote",
        "status": "applied",
        "from_sync_key": target_key,
        "to_sync_key": source_key,
    }


def _rename_local(action: dict[str, Any], target_root: Path) -> dict[str, Any]:
    old_path = target_root / action["target"]["content_path"]
    new_path = target_root / action["source"]["content_path"]
    if not old_path.exists():
        raise ApplyError(f"target file not found for rename: {old_path}")
    new_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(old_path), str(new_path))
    _prune_empty_dirs(old_path.parent, target_root)
    return {
        "operation": "rename_local",
        "status": "applied",
        "from_path": str(old_path),
        "to_path": str(new_path),
    }


def _trash_remote(
    store: LocalObjectStore,
    action: dict[str, Any],
    origin_device: str,
    timestamp_utc: str | None,
) -> dict[str, Any]:
    target_key = action["target"]["sync_key"]
    trash_key = store.trash_object(
        target_key,
        origin_device=origin_device,
        timestamp_utc=timestamp_utc,
    )
    return {
        "operation": "trash_remote",
        "status": "applied",
        "from_sync_key": target_key,
        "trash_key": trash_key,
    }


def _trash_local(
    action: dict[str, Any],
    target_root: Path,
    trash_root: Path,
    origin_device: str,
    timestamp_utc: str | None,
) -> dict[str, Any]:
    target = action["target"]
    source_path = target_root / target["content_path"]
    if not source_path.exists():
        raise ApplyError(f"target file not found for trash: {source_path}")
    trash_path = _trash_path(
        source_path,
        target_root,
        trash_root,
        origin_device,
        timestamp_utc,
        category="deleted",
    )
    trash_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source_path), str(trash_path))
    _prune_empty_dirs(source_path.parent, target_root)
    return {
        "operation": "trash_local",
        "status": "applied",
        "from_path": str(source_path),
        "trash_path": str(trash_path),
    }


def _target_sync_key(action: dict[str, Any]) -> str:
    if "target" in action:
        return action["target"]["sync_key"]
    return action["source"]["sync_key"]


def _target_path_for_download(action: dict[str, Any], target_root: Path) -> Path:
    if "target" in action:
        return target_root / action["target"]["content_path"]
    return target_root / action["source"]["content_path"]


def _backup_local_file(
    path: Path,
    target_root: Path,
    trash_root: Path,
    origin_device: str,
    timestamp_utc: str | None,
) -> Path:
    backup_path = _trash_path(
        path,
        target_root,
        trash_root,
        origin_device,
        timestamp_utc,
        category="backups",
    )
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, backup_path)
    return backup_path


def _trash_path(
    path: Path,
    target_root: Path,
    trash_root: Path,
    origin_device: str,
    timestamp_utc: str | None,
    category: str,
) -> Path:
    timestamp = timestamp_utc or _timestamp_utc()
    relative_path = path.relative_to(target_root)
    return trash_root / category / timestamp / origin_device / relative_path


def _timestamp_utc() -> str:
    from datetime import datetime, timezone

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


def _summary(results: list[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for result in results:
        key = f"{result['operation']}:{result['status']}"
        summary[key] = summary.get(key, 0) + 1
    summary["total"] = len(results)
    return summary
