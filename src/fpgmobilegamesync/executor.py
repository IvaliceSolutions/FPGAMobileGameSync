"""Plan execution."""

from __future__ import annotations

import json
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


def _target_sync_key(action: dict[str, Any]) -> str:
    if "target" in action:
        return action["target"]["sync_key"]
    return action["source"]["sync_key"]


def _summary(results: list[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for result in results:
        key = f"{result['operation']}:{result['status']}"
        summary[key] = summary.get(key, 0) + 1
    summary["total"] = len(results)
    return summary
