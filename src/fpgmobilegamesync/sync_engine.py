"""High-level sync orchestration."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from .executor import apply_plan_to_local_store, apply_plan_to_local_target
from .object_store import LocalObjectStore
from .planner import build_plan
from .scanner import scan


class SyncError(Exception):
    """Raised when a high-level sync cannot be run."""


def run_local_sync(
    config: dict[str, Any],
    direction: str,
    store_root: Path,
    source_root: Path | None = None,
    target_root: Path | None = None,
    systems: list[str] | None = None,
    types: list[str] | None = None,
    apply: bool = False,
    timestamp_utc: str | None = None,
    allow_conflicts: bool = False,
) -> dict[str, Any]:
    """Run source -> local object store -> target sync on local paths."""

    runtime_config = copy.deepcopy(config)
    source_device, target_device = _sync_devices(runtime_config, direction)
    if source_root is not None:
        runtime_config["devices"][source_device]["local"]["root"] = str(source_root)
    if target_root is not None:
        runtime_config["devices"][target_device]["local"]["root"] = str(target_root)

    source_manifest = scan(
        config=runtime_config,
        device=source_device,
        systems=systems,
        types=types,
    )
    store = LocalObjectStore(store_root)
    store_manifest_before = store.scan()
    upload_plan = build_plan(
        source=source_manifest,
        target=store_manifest_before,
        mode="upload",
        source_name=source_device,
        target_name="s3",
    )

    upload_apply = None
    if apply:
        upload_apply = apply_plan_to_local_store(
            plan=upload_plan,
            store_root=store_root,
            timestamp_utc=timestamp_utc,
            allow_conflicts=allow_conflicts,
            config=runtime_config,
            source_device=source_device,
        )

    store_manifest_after_upload = store.scan()
    target_manifest = scan(
        config=runtime_config,
        device=target_device,
        systems=systems,
        types=types,
    )
    download_plan = build_plan(
        source=store_manifest_after_upload,
        target=target_manifest,
        mode="download",
        source_name="s3",
        target_name=target_device,
    )

    download_apply = None
    if apply:
        download_apply = _apply_download_plan_to_device(
            config=runtime_config,
            plan=download_plan,
            target_device=target_device,
            timestamp_utc=timestamp_utc,
            allow_conflicts=allow_conflicts,
        )

    return {
        "backend": "local",
        "direction": direction,
        "dry_run": not apply,
        "source_device": source_device,
        "target_device": target_device,
        "store_root": str(store_root),
        "source_summary": source_manifest["summary"],
        "store_summary_before_upload": store_manifest_before["summary"],
        "upload_plan": upload_plan,
        "upload_apply": upload_apply,
        "store_summary_after_upload": store_manifest_after_upload["summary"],
        "target_summary": target_manifest["summary"],
        "download_plan": download_plan,
        "download_apply": download_apply,
    }


def _apply_download_plan_to_device(
    config: dict[str, Any],
    plan: dict[str, Any],
    target_device: str,
    timestamp_utc: str | None,
    allow_conflicts: bool,
) -> dict[str, Any]:
    groups: dict[Path, list[dict[str, Any]]] = {}
    for action in plan["actions"]:
        root = _target_content_root_for_action(config, target_device, action)
        groups.setdefault(root, []).append(action)

    trash_root = Path(config["devices"][target_device]["local"]["trash"])
    applied_groups = []
    for target_content_root, actions in sorted(groups.items(), key=lambda item: str(item[0])):
        partial_plan = dict(plan)
        partial_plan["actions"] = actions
        applied_groups.append(
            {
                "target_root": str(target_content_root),
                "result": apply_plan_to_local_target(
                    plan=partial_plan,
                    target_root=target_content_root,
                    trash_root=trash_root,
                    timestamp_utc=timestamp_utc,
                    allow_conflicts=allow_conflicts,
                    config=config,
                    target_device=target_device,
                ),
            }
        )

    return {
        "groups": applied_groups,
        "summary": _combined_apply_summary(applied_groups),
    }


def _sync_devices(config: dict[str, Any], direction: str) -> tuple[str, str]:
    sync_modes = config.get("sync_modes", {})
    if direction not in sync_modes:
        raise SyncError(f"unknown sync direction: {direction}")
    mode = sync_modes[direction]
    source = mode.get("source")
    target = mode.get("target")
    if not isinstance(source, str) or not isinstance(target, str):
        raise SyncError(f"sync direction is missing source/target: {direction}")
    if source not in config.get("devices", {}):
        raise SyncError(f"sync source device is not configured: {source}")
    if target not in config.get("devices", {}):
        raise SyncError(f"sync target device is not configured: {target}")
    return source, target


def _target_content_root_for_action(
    config: dict[str, Any],
    target_device: str,
    action: dict[str, Any],
) -> Path:
    item = _action_item(action)
    system = item["system"]
    content_type = item["type"]
    content_path = item["content_path"]
    device_root = Path(config["devices"][target_device]["local"]["root"])
    configured_paths = config["systems"][system]["paths"][target_device].get(content_type)
    if configured_paths is None:
        raise SyncError(
            f"target path is not configured for {target_device}/{system}/{content_type}"
        )
    if isinstance(configured_paths, str):
        return device_root / configured_paths

    candidates = [Path(path) for path in configured_paths]
    content_name = Path(content_path).name.casefold()
    for candidate in candidates:
        if candidate.name.casefold() == content_name:
            return device_root / candidate.parent
    if len(candidates) == 1:
        return device_root / candidates[0].parent
    raise SyncError(
        f"cannot infer target content root for {target_device}/{system}/{content_type}/"
        f"{content_path}"
    )


def _action_item(action: dict[str, Any]) -> dict[str, Any]:
    if "source" in action:
        return action["source"]
    if "target" in action:
        return action["target"]
    raise SyncError(f"plan action has no source or target item: {action.get('operation')}")


def _combined_apply_summary(applied_groups: list[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for group in applied_groups:
        for key, value in group["result"].get("summary", {}).items():
            summary[key] = summary.get(key, 0) + int(value)
    return summary
