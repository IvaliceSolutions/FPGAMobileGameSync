"""High-level sync orchestration."""

from __future__ import annotations

import copy
import json
import posixpath
from pathlib import Path
from typing import Any

from .executor import (
    apply_plan_from_s3_to_sftp_target,
    apply_plan_from_s3_to_local_target,
    apply_plan_from_sftp_to_s3_store,
    apply_plan_to_local_store,
    apply_plan_to_local_target,
    apply_plan_to_s3_store,
)
from .object_store import LocalObjectStore
from .planner import build_plan
from .remote_scanner import scan_remote
from .s3_store import S3ObjectStore
from .scanner import scan
from .sftp_client import SftpDeviceClient


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
    skip_deletes: bool = False,
    report_dir: Path | None = None,
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
    if skip_deletes:
        upload_plan = _skip_delete_actions(upload_plan)

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
    if skip_deletes:
        download_plan = _skip_delete_actions(download_plan)

    download_apply = None
    if apply:
        download_apply = _apply_download_plan_to_device(
            config=runtime_config,
            plan=download_plan,
            target_device=target_device,
            timestamp_utc=timestamp_utc,
            allow_conflicts=allow_conflicts,
        )

    result = {
        "backend": "local",
        "direction": direction,
        "dry_run": not apply,
        "source_device": source_device,
        "target_device": target_device,
        "skip_deletes": skip_deletes,
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
    if report_dir is not None:
        result["report_dir"] = str(report_dir)
        result["report_files"] = _write_run_reports(
            report_dir=report_dir,
            artifacts={
                "source-manifest.json": source_manifest,
                "store-before-upload-manifest.json": store_manifest_before,
                "upload-plan.json": upload_plan,
                "upload-apply.json": upload_apply,
                "store-after-upload-manifest.json": store_manifest_after_upload,
                "target-manifest.json": target_manifest,
                "download-plan.json": download_plan,
                "download-apply.json": download_apply,
                "summary.json": _summary_report(result),
            },
        )
    return result


def run_s3_sync(
    config: dict[str, Any],
    direction: str,
    source_root: Path | None = None,
    target_root: Path | None = None,
    systems: list[str] | None = None,
    types: list[str] | None = None,
    apply: bool = False,
    timestamp_utc: str | None = None,
    allow_conflicts: bool = False,
    report_dir: Path | None = None,
    store: S3ObjectStore | None = None,
    scan_backend: str = "local",
    sftp_clients: dict[str, Any] | None = None,
    use_lock: bool = True,
    lock_ttl_seconds: int = 1800,
    lock_owner: str | None = None,
    source_scan_backend: str | None = None,
    target_scan_backend: str | None = None,
    skip_deletes: bool = False,
) -> dict[str, Any]:
    """Run source -> S3/Garage -> target sync on local or SFTP device roots."""
    if scan_backend not in {"local", "sftp"}:
        raise SyncError(f"unsupported sync scan backend: {scan_backend}")
    source_backend = source_scan_backend or scan_backend
    target_backend = target_scan_backend or scan_backend
    for role, backend in (("source", source_backend), ("target", target_backend)):
        if backend not in {"local", "sftp"}:
            raise SyncError(f"unsupported {role} scan backend: {backend}")

    runtime_config = copy.deepcopy(config)
    source_device, target_device = _sync_devices(runtime_config, direction)
    if source_root is not None:
        runtime_config["devices"][source_device]["local"]["root"] = str(source_root)
    if target_root is not None:
        runtime_config["devices"][target_device]["local"]["root"] = str(target_root)

    managed_clients: list[Any] = []
    clients = sftp_clients or {}
    s3_store = store or S3ObjectStore.from_config(runtime_config)
    lock = None
    lock_release = None
    try:
        sftp_devices = {
            device
            for device, backend in (
                (source_device, source_backend),
                (target_device, target_backend),
            )
            if backend == "sftp"
        }
        if sftp_devices:
            clients = dict(clients)
            for device in sorted(sftp_devices):
                if device not in clients:
                    client = SftpDeviceClient.from_config(runtime_config, device)
                    clients[device] = client
                    managed_clients.append(client)

        if apply and use_lock:
            lock = s3_store.acquire_lock(
                name="sync",
                owner=lock_owner or f"{source_device}-to-{target_device}",
                ttl_seconds=lock_ttl_seconds,
            )

        try:
            source_manifest = _scan_device(
                config=runtime_config,
                device=source_device,
                systems=systems,
                types=types,
                scan_backend=source_backend,
                sftp_client=clients.get(source_device),
            )
            store_manifest_before = s3_store.scan()
            upload_plan = build_plan(
                source=source_manifest,
                target=store_manifest_before,
                mode="upload",
                source_name=source_device,
                target_name="s3",
            )
            if skip_deletes:
                upload_plan = _skip_delete_actions(upload_plan)

            upload_apply = None
            if apply:
                if source_backend == "sftp":
                    upload_apply = apply_plan_from_sftp_to_s3_store(
                        plan=upload_plan,
                        config=runtime_config,
                        client=clients[source_device],
                        timestamp_utc=timestamp_utc,
                        allow_conflicts=allow_conflicts,
                        source_device=source_device,
                        store=s3_store,
                    )
                else:
                    upload_apply = apply_plan_to_s3_store(
                        plan=upload_plan,
                        config=runtime_config,
                        timestamp_utc=timestamp_utc,
                        allow_conflicts=allow_conflicts,
                        source_device=source_device,
                        store=s3_store,
                    )

            store_manifest_after_upload = s3_store.scan()
            target_manifest = _scan_device(
                config=runtime_config,
                device=target_device,
                systems=systems,
                types=types,
                scan_backend=target_backend,
                sftp_client=clients.get(target_device),
            )
            download_plan = build_plan(
                source=store_manifest_after_upload,
                target=target_manifest,
                mode="download",
                source_name="s3",
                target_name=target_device,
            )
            if skip_deletes:
                download_plan = _skip_delete_actions(download_plan)

            download_apply = None
            if apply:
                if target_backend == "sftp":
                    download_apply = _apply_s3_download_plan_to_sftp_device(
                        config=runtime_config,
                        plan=download_plan,
                        target_device=target_device,
                        timestamp_utc=timestamp_utc,
                        allow_conflicts=allow_conflicts,
                        store=s3_store,
                        client=clients[target_device],
                    )
                else:
                    download_apply = _apply_s3_download_plan_to_device(
                        config=runtime_config,
                        plan=download_plan,
                        target_device=target_device,
                        timestamp_utc=timestamp_utc,
                        allow_conflicts=allow_conflicts,
                        store=s3_store,
                    )
        finally:
            if lock is not None:
                lock_release = s3_store.release_lock(lock)
    finally:
        for client in managed_clients:
            client.close()

    result = {
        "backend": "s3",
        "scan_backend": scan_backend,
        "source_scan_backend": source_backend,
        "target_scan_backend": target_backend,
        "direction": direction,
        "dry_run": not apply,
        "source_device": source_device,
        "target_device": target_device,
        "skip_deletes": skip_deletes,
        "store": {
            "backend": "s3",
            "bucket": s3_store.bucket,
            "prefix": s3_store.prefix,
        },
        "source_summary": source_manifest["summary"],
        "store_summary_before_upload": store_manifest_before["summary"],
        "upload_plan": upload_plan,
        "upload_apply": upload_apply,
        "store_summary_after_upload": store_manifest_after_upload["summary"],
        "target_summary": target_manifest["summary"],
        "download_plan": download_plan,
        "download_apply": download_apply,
    }
    if lock is not None:
        result["lock"] = _lock_report(lock)
        result["lock_release"] = lock_release
    if report_dir is not None:
        result["report_dir"] = str(report_dir)
        result["report_files"] = _write_run_reports(
            report_dir=report_dir,
            artifacts={
                "source-manifest.json": source_manifest,
                "store-before-upload-manifest.json": store_manifest_before,
                "upload-plan.json": upload_plan,
                "upload-apply.json": upload_apply,
                "store-after-upload-manifest.json": store_manifest_after_upload,
                "target-manifest.json": target_manifest,
                "download-plan.json": download_plan,
                "download-apply.json": download_apply,
                "lock.json": _lock_report(lock) if lock is not None else None,
                "lock-release.json": lock_release,
                "summary.json": _summary_report(result),
            },
        )
    return result


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


def _apply_s3_download_plan_to_device(
    config: dict[str, Any],
    plan: dict[str, Any],
    target_device: str,
    timestamp_utc: str | None,
    allow_conflicts: bool,
    store: S3ObjectStore,
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
                "result": apply_plan_from_s3_to_local_target(
                    plan=partial_plan,
                    config=config,
                    target_root=target_content_root,
                    trash_root=trash_root,
                    timestamp_utc=timestamp_utc,
                    allow_conflicts=allow_conflicts,
                    target_device=target_device,
                    store=store,
                ),
            }
        )

    return {
        "groups": applied_groups,
        "summary": _combined_apply_summary(applied_groups),
    }


def _apply_s3_download_plan_to_sftp_device(
    config: dict[str, Any],
    plan: dict[str, Any],
    target_device: str,
    timestamp_utc: str | None,
    allow_conflicts: bool,
    store: S3ObjectStore,
    client: Any,
) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for action in plan["actions"]:
        root = _remote_target_content_root_for_action(config, target_device, action)
        groups.setdefault(root, []).append(action)

    trash_root = _remote_trash_root(config, target_device)
    applied_groups = []
    for target_content_root, actions in sorted(groups.items(), key=lambda item: item[0]):
        partial_plan = dict(plan)
        partial_plan["actions"] = actions
        applied_groups.append(
            {
                "target_root": target_content_root,
                "result": apply_plan_from_s3_to_sftp_target(
                    plan=partial_plan,
                    config=config,
                    client=client,
                    target_root=target_content_root,
                    trash_root=trash_root,
                    timestamp_utc=timestamp_utc,
                    allow_conflicts=allow_conflicts,
                    target_device=target_device,
                    store=store,
                ),
            }
        )

    return {
        "groups": applied_groups,
        "summary": _combined_apply_summary(applied_groups),
    }


def _scan_device(
    config: dict[str, Any],
    device: str,
    systems: list[str] | None,
    types: list[str] | None,
    scan_backend: str,
    sftp_client: Any | None,
) -> dict[str, Any]:
    if scan_backend == "local":
        return scan(config=config, device=device, systems=systems, types=types)
    if scan_backend == "sftp":
        return scan_remote(
            config=config,
            device=device,
            systems=systems,
            types=types,
            client=sftp_client,
        )
    raise SyncError(f"unsupported scan backend: {scan_backend}")


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


def _remote_target_content_root_for_action(
    config: dict[str, Any],
    target_device: str,
    action: dict[str, Any],
) -> str:
    item = _action_item(action)
    system = item["system"]
    content_type = item["type"]
    content_path = item["content_path"]
    device_root = _remote_device_root(config, target_device)
    configured_paths = config["systems"][system]["paths"][target_device].get(content_type)
    if configured_paths is None:
        raise SyncError(
            f"target path is not configured for {target_device}/{system}/{content_type}"
        )
    if isinstance(configured_paths, str):
        return _remote_join(device_root, configured_paths)

    candidates = [Path(path) for path in configured_paths]
    content_name = Path(content_path).name.casefold()
    for candidate in candidates:
        if candidate.name.casefold() == content_name:
            return _remote_join(device_root, str(candidate.parent))
    if len(candidates) == 1:
        return _remote_join(device_root, str(candidates[0].parent))
    raise SyncError(
        f"cannot infer target content root for {target_device}/{system}/{content_type}/"
        f"{content_path}"
    )


def _remote_device_root(config: dict[str, Any], device: str) -> str:
    remote_root = config["devices"][device].get("remote", {}).get("root")
    if isinstance(remote_root, str) and remote_root:
        return _remote_normalize(remote_root)
    local_root = config["devices"][device].get("local", {}).get("root")
    if isinstance(local_root, str) and local_root:
        return _remote_normalize(local_root)
    raise SyncError(f"device has no root: {device}")


def _remote_trash_root(config: dict[str, Any], device: str) -> str:
    remote_trash = config["devices"][device].get("remote", {}).get("trash")
    if isinstance(remote_trash, str) and remote_trash:
        return _remote_normalize(remote_trash)
    local_trash = config["devices"][device].get("local", {}).get("trash")
    if isinstance(local_trash, str) and local_trash:
        return _remote_normalize(local_trash)
    return _remote_join(_remote_device_root(config, device), ".sync_trash")


def _remote_join(*parts: str) -> str:
    return _remote_normalize(posixpath.join(*parts))


def _remote_normalize(path: str) -> str:
    normalized = posixpath.normpath(path)
    if path.startswith("/") and not normalized.startswith("/"):
        return f"/{normalized}"
    return normalized


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


def _skip_delete_actions(plan: dict[str, Any]) -> dict[str, Any]:
    filtered_plan = copy.deepcopy(plan)
    skipped = [
        action
        for action in filtered_plan["actions"]
        if action.get("operation") in {"trash_remote", "trash_local"}
    ]
    if not skipped:
        return filtered_plan

    filtered_plan["actions"] = [
        action
        for action in filtered_plan["actions"]
        if action.get("operation") not in {"trash_remote", "trash_local"}
    ]
    filtered_plan["summary"] = _action_summary(filtered_plan["actions"])
    filtered_plan["skipped_actions"] = skipped
    filtered_plan["skipped_summary"] = _action_summary(skipped)
    filtered_plan["skip_reason"] = "skip_deletes"
    return filtered_plan


def _action_summary(actions: list[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for action in actions:
        operation = action["operation"]
        summary[operation] = summary.get(operation, 0) + 1
    summary["total"] = len(actions)
    return summary


def _lock_report(lock: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in lock.items() if key != "token"}


def _write_run_reports(report_dir: Path, artifacts: dict[str, Any]) -> list[str]:
    report_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for filename, data in artifacts.items():
        if data is None:
            continue
        path = report_dir / filename
        with path.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, sort_keys=True)
            handle.write("\n")
        written.append(str(path))
    return written


def _summary_report(result: dict[str, Any]) -> dict[str, Any]:
    upload_apply = result.get("upload_apply") or {}
    download_apply = result.get("download_apply") or {}
    summary = {
        "backend": result["backend"],
        "direction": result["direction"],
        "dry_run": result["dry_run"],
        "skip_deletes": result.get("skip_deletes", False),
        "source_device": result["source_device"],
        "target_device": result["target_device"],
        "source_summary": result["source_summary"],
        "store_summary_before_upload": result["store_summary_before_upload"],
        "upload_plan_summary": result["upload_plan"]["summary"],
        "upload_skipped_summary": result["upload_plan"].get("skipped_summary"),
        "upload_apply_summary": upload_apply.get("summary"),
        "store_summary_after_upload": result["store_summary_after_upload"],
        "target_summary": result["target_summary"],
        "download_plan_summary": result["download_plan"]["summary"],
        "download_skipped_summary": result["download_plan"].get("skipped_summary"),
        "download_apply_summary": download_apply.get("summary"),
    }
    if "scan_backend" in result:
        summary["scan_backend"] = result["scan_backend"]
    if "source_scan_backend" in result:
        summary["source_scan_backend"] = result["source_scan_backend"]
    if "target_scan_backend" in result:
        summary["target_scan_backend"] = result["target_scan_backend"]
    if "store_root" in result:
        summary["store_root"] = result["store_root"]
    if "store" in result:
        summary["store"] = result["store"]
    if "lock" in result:
        summary["lock"] = result["lock"]
    if "lock_release" in result:
        summary["lock_release"] = result["lock_release"]
    return summary
