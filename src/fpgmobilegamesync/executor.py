"""Plan execution."""

from __future__ import annotations

import json
import hashlib
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Any

from .converter import ConversionError, convert_save_file
from .object_store import LocalObjectStore
from .s3_store import S3ObjectStore
from .save_paths import is_convertible_save, native_save_content_path


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
    config: dict[str, Any] | None = None,
    source_device: str | None = None,
) -> dict[str, Any]:
    store = LocalObjectStore(store_root)
    source_device = source_device or str(plan.get("source", "source"))
    applied: list[dict[str, Any]] = []

    for action in plan["actions"]:
        applied.append(
            _apply_action(
                store=store,
                action=action,
                origin_device=source_device,
                timestamp_utc=timestamp_utc,
                allow_conflicts=allow_conflicts,
                config=config,
                source_device=source_device,
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


def apply_plan_to_s3_store(
    plan: dict[str, Any],
    config: dict[str, Any],
    timestamp_utc: str | None = None,
    allow_conflicts: bool = False,
    source_device: str | None = None,
    store: S3ObjectStore | None = None,
) -> dict[str, Any]:
    s3_store = store or S3ObjectStore.from_config(config)
    source_device = source_device or str(plan.get("source", "source"))
    applied: list[dict[str, Any]] = []

    for action in plan["actions"]:
        applied.append(
            _apply_action(
                store=s3_store,
                action=action,
                origin_device=source_device,
                timestamp_utc=timestamp_utc,
                allow_conflicts=allow_conflicts,
                config=config,
                source_device=source_device,
            )
        )

    manifest = s3_store.scan_live()
    s3_store.write_manifest(manifest)

    return {
        "backend": "s3",
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
    config: dict[str, Any] | None = None,
    target_device: str | None = None,
) -> dict[str, Any]:
    trash_base = trash_root or (target_root / ".sync_trash")
    target_device = target_device or str(plan.get("target", "target"))
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
                config=config,
                target_device=target_device,
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
    store: LocalObjectStore | S3ObjectStore,
    action: dict[str, Any],
    origin_device: str,
    timestamp_utc: str | None,
    allow_conflicts: bool,
    config: dict[str, Any] | None,
    source_device: str,
) -> dict[str, Any]:
    operation = action["operation"]

    if operation == "noop":
        return {"operation": operation, "status": "skipped", "reason": action["reason"]}
    if operation == "conflict":
        if not allow_conflicts:
            raise ApplyError("plan contains conflicts; refusing to apply")
        return {"operation": operation, "status": "skipped", "reason": action["reason"]}
    if operation == "upload":
        return _upload(store, action, origin_device, timestamp_utc, config, source_device)
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
    config: dict[str, Any] | None,
    target_device: str,
) -> dict[str, Any]:
    operation = action["operation"]

    if operation == "noop":
        return {"operation": operation, "status": "skipped", "reason": action["reason"]}
    if operation == "conflict":
        if not allow_conflicts:
            raise ApplyError("plan contains conflicts; refusing to apply")
        return {"operation": operation, "status": "skipped", "reason": action["reason"]}
    if operation == "download":
        return _download(
            action,
            target_root,
            trash_root,
            origin_device,
            timestamp_utc,
            config,
            target_device,
        )
    if operation == "rename_local":
        return _rename_local(action, target_root, config, target_device)
    if operation == "trash_local":
        return _trash_local(action, target_root, trash_root, origin_device, timestamp_utc)

    raise ApplyError(f"unsupported operation for local target: {operation}")


def _upload(
    store: LocalObjectStore | S3ObjectStore,
    action: dict[str, Any],
    origin_device: str,
    timestamp_utc: str | None,
    config: dict[str, Any] | None,
    source_device: str,
) -> dict[str, Any]:
    source = action["source"]
    source_path = Path(source["absolute_path"])
    if not source_path.exists():
        raise ApplyError(f"source file not found: {source_path}")
    _verify_file_fingerprint(source_path, source, role="source")

    target_key = _target_sync_key(action)
    existing_target_key = action.get("target", {}).get("sync_key", target_key)
    backup_key = None
    if "target" in action and store.object_exists(existing_target_key):
        _verify_store_object_fingerprint(
            store,
            existing_target_key,
            action["target"],
            role="target",
        )
    if action.get("backup_target_before_apply") and store.object_exists(existing_target_key):
        backup_key = store.backup_object(
            existing_target_key,
            origin_device=origin_device,
            timestamp_utc=timestamp_utc,
        )
    if (
        action.get("rename_target_before_copy")
        and existing_target_key != target_key
        and store.object_exists(existing_target_key)
    ):
        store.rename_object(existing_target_key, target_key)
    conversion_result = None
    if _should_convert_for_store(source, config, source_device):
        with tempfile.TemporaryDirectory() as tmp:
            converted_path = Path(tmp) / Path(target_key).name
            conversion_result = _convert_save(
                config=config,
                system=source["system"],
                direction="thor-to-mister",
                source_path=source_path,
                output_path=converted_path,
            )
            _verify_conversion_fingerprint(conversion_result, source)
            store.put_file(converted_path, target_key)
    else:
        store.put_file(source_path, target_key)

    result = {
        "operation": "upload",
        "status": "applied",
        "sync_key": target_key,
    }
    if backup_key is not None:
        result["backup_key"] = backup_key
    if conversion_result is not None:
        result["conversion"] = _conversion_summary(conversion_result)
    return result


def _download(
    action: dict[str, Any],
    target_root: Path,
    trash_root: Path,
    origin_device: str,
    timestamp_utc: str | None,
    config: dict[str, Any] | None,
    target_device: str,
) -> dict[str, Any]:
    source = action["source"]
    source_path = Path(source["absolute_path"])
    if not source_path.exists():
        raise ApplyError(f"source file not found: {source_path}")
    _verify_file_fingerprint(source_path, source, role="source")

    target_path = _target_path_for_download(action, target_root, config, target_device)
    existing_target_path = _existing_target_path_for_download(action, target_root)
    backup_path = None
    if "target" in action and existing_target_path.exists():
        _verify_file_fingerprint(existing_target_path, action["target"], role="target")
    if action.get("backup_target_before_apply") and existing_target_path.exists():
        backup_path = _backup_local_file(
            existing_target_path,
            target_root,
            trash_root,
            origin_device,
            timestamp_utc,
        )
    if (
        action.get("rename_target_before_copy")
        and existing_target_path != target_path
        and existing_target_path.exists()
    ):
        _rename_path_case_aware(existing_target_path, target_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    conversion_result = None
    if _should_convert_for_target(source, config, target_device):
        conversion_result = _convert_save(
            config=config,
            system=source["system"],
            direction="mister-to-thor",
            source_path=source_path,
            output_path=target_path,
        )
        _verify_conversion_fingerprint(conversion_result, source)
    else:
        shutil.copy2(source_path, target_path)

    result = {
        "operation": "download",
        "status": "applied",
        "path": str(target_path),
    }
    if backup_path is not None:
        result["backup_path"] = str(backup_path)
    if conversion_result is not None:
        result["conversion"] = _conversion_summary(conversion_result)
    return result


def _rename_remote(
    store: LocalObjectStore | S3ObjectStore,
    action: dict[str, Any],
) -> dict[str, Any]:
    source_key = action["source"]["sync_key"]
    target_key = action["target"]["sync_key"]
    _verify_store_object_fingerprint(store, target_key, action["target"], role="target")
    store.rename_object(target_key, source_key)
    return {
        "operation": "rename_remote",
        "status": "applied",
        "from_sync_key": target_key,
        "to_sync_key": source_key,
    }


def _rename_local(
    action: dict[str, Any],
    target_root: Path,
    config: dict[str, Any] | None,
    target_device: str,
) -> dict[str, Any]:
    old_path = target_root / action["target"].get(
        "native_content_path",
        action["target"]["content_path"],
    )
    new_path = _target_path_from_source(
        action["source"],
        target_root,
        config,
        target_device,
    )
    if not old_path.exists():
        raise ApplyError(f"target file not found for rename: {old_path}")
    _verify_file_fingerprint(old_path, action["target"], role="target")
    _rename_path_case_aware(old_path, new_path)
    _prune_empty_dirs(old_path.parent, target_root)
    return {
        "operation": "rename_local",
        "status": "applied",
        "from_path": str(old_path),
        "to_path": str(new_path),
    }


def _trash_remote(
    store: LocalObjectStore | S3ObjectStore,
    action: dict[str, Any],
    origin_device: str,
    timestamp_utc: str | None,
) -> dict[str, Any]:
    target_key = action["target"]["sync_key"]
    _verify_store_object_fingerprint(store, target_key, action["target"], role="target")
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
    source_path = target_root / target.get("native_content_path", target["content_path"])
    if not source_path.exists():
        raise ApplyError(f"target file not found for trash: {source_path}")
    _verify_file_fingerprint(source_path, target, role="target")
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
    if action.get("rename_target_before_copy"):
        return action["source"]["sync_key"]
    if "target" in action:
        return action["target"]["sync_key"]
    return action["source"]["sync_key"]


def _target_path_for_download(
    action: dict[str, Any],
    target_root: Path,
    config: dict[str, Any] | None,
    target_device: str,
) -> Path:
    source = action["source"]
    if _use_native_save_path(source, config, target_device):
        return target_root / native_save_content_path(
            config=config or {},
            system=source["system"],
            device=target_device,
            canonical_content_path=source["content_path"],
        )
    if action.get("rename_target_before_copy"):
        return target_root / action["source"]["content_path"]
    if "target" in action:
        return target_root / action["target"].get(
            "native_content_path",
            action["target"]["content_path"],
        )
    return target_root / action["source"]["content_path"]


def _target_path_from_source(
    source: dict[str, Any],
    target_root: Path,
    config: dict[str, Any] | None,
    target_device: str,
) -> Path:
    if _use_native_save_path(source, config, target_device):
        return target_root / native_save_content_path(
            config=config or {},
            system=source["system"],
            device=target_device,
            canonical_content_path=source["content_path"],
        )
    return target_root / source["content_path"]


def _existing_target_path_for_download(action: dict[str, Any], target_root: Path) -> Path:
    if "target" in action:
        return target_root / action["target"].get(
            "native_content_path",
            action["target"]["content_path"],
        )
    return target_root / action["source"]["content_path"]


def _use_native_save_path(
    item: dict[str, Any],
    config: dict[str, Any] | None,
    target_device: str,
) -> bool:
    if config is None:
        return False
    if target_device not in config.get("devices", {}):
        return False
    return is_convertible_save(
        config=config,
        system=item["system"],
        content_type=item["type"],
    )


def _should_convert_for_target(
    item: dict[str, Any],
    config: dict[str, Any] | None,
    target_device: str,
) -> bool:
    return target_device == "thor" and _use_native_save_path(item, config, target_device)


def _should_convert_for_store(
    item: dict[str, Any],
    config: dict[str, Any] | None,
    source_device: str,
) -> bool:
    return source_device == "thor" and _use_native_save_path(item, config, source_device)


def _convert_save(
    config: dict[str, Any] | None,
    system: str,
    direction: str,
    source_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    if config is None:
        raise ApplyError("save conversion requires config")
    try:
        return convert_save_file(
            config=config,
            system=system,
            direction=direction,
            source_path=source_path,
            output_path=output_path,
        )
    except ConversionError as exc:
        raise ApplyError(str(exc)) from exc


def _conversion_summary(result: dict[str, Any]) -> dict[str, Any]:
    summary = {
        "strategy": result["strategy"],
        "direction": result["direction"],
        "size": result["size"],
    }
    for key in ("input_format", "output_format", "canonical_format", "canonical_sha256"):
        if key in result:
            summary[key] = result[key]
    return summary


def _verify_file_fingerprint(path: Path, item: dict[str, Any], role: str) -> None:
    expected_sha = item.get("native_sha256")
    expected_size = item.get("native_size")
    if expected_sha is None and expected_size is None:
        return
    actual_size = path.stat().st_size
    if expected_size is not None and actual_size != int(expected_size):
        raise ApplyError(
            f"{role} file changed since plan: {path}; "
            f"size {actual_size} != expected {expected_size}"
        )
    if expected_sha is not None:
        actual_sha = _sha256(path)
        if actual_sha != expected_sha:
            raise ApplyError(
                f"{role} file changed since plan: {path}; "
                f"sha256 {actual_sha} != expected {expected_sha}"
            )


def _verify_store_object_fingerprint(
    store: LocalObjectStore | S3ObjectStore,
    sync_key: str,
    item: dict[str, Any],
    role: str,
) -> None:
    try:
        store.verify_object_fingerprint(sync_key, item, role)
    except Exception as exc:
        raise ApplyError(str(exc)) from exc


def _verify_conversion_fingerprint(
    conversion_result: dict[str, Any],
    source_item: dict[str, Any],
) -> None:
    expected_sha = source_item.get("canonical_sha256")
    expected_size = source_item.get("canonical_size")
    if "canonical_size" not in conversion_result and "canonical_sha256" not in conversion_result:
        return
    if expected_size is not None and conversion_result.get("canonical_size") != int(expected_size):
        raise ApplyError(
            "converted save canonical size does not match source manifest: "
            f"{conversion_result.get('canonical_size')} != {expected_size}"
        )
    if expected_sha is not None and conversion_result.get("canonical_sha256") != expected_sha:
        raise ApplyError(
            "converted save canonical hash does not match source manifest: "
            f"{conversion_result.get('canonical_sha256')} != {expected_sha}"
        )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _rename_path_case_aware(old_path: Path, new_path: Path) -> None:
    new_path.parent.mkdir(parents=True, exist_ok=True)
    if _same_filesystem_entry(old_path, new_path):
        temp_path = old_path.with_name(
            f".{old_path.name}.case-rename-{uuid.uuid4().hex}.tmp"
        )
        old_path.rename(temp_path)
        temp_path.rename(new_path)
        return
    shutil.move(str(old_path), str(new_path))


def _same_filesystem_entry(old_path: Path, new_path: Path) -> bool:
    try:
        return old_path.samefile(new_path)
    except FileNotFoundError:
        return False


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
