"""Action planning for upload and download flows."""

from __future__ import annotations

from typing import Any

from .compare import compare_manifests
from .psx_memory_card import EMPTY_CARD_SHA256ES
from .save_paths import is_convertible_save, native_save_content_path


class PlanError(Exception):
    """Raised when a sync plan cannot be created."""


def build_plan(
    source: dict[str, Any],
    target: dict[str, Any],
    mode: str,
    source_name: str = "source",
    target_name: str = "target",
    config: dict[str, Any] | None = None,
    target_device: str | None = None,
) -> dict[str, Any]:
    if mode not in {"upload", "download"}:
        raise PlanError(f"unsupported plan mode: {mode}")

    comparison = compare_manifests(
        source=source,
        target=target,
        source_name=source_name,
        target_name=target_name,
    )
    actions = [
        _plan_action(
            action,
            mode=mode,
            source_name=source_name,
            target_name=target_name,
            config=config,
            target_device=target_device,
        )
        for action in comparison["actions"]
    ]
    actions = _protect_download_native_path_collisions(
        actions=actions,
        target=target,
        mode=mode,
        config=config,
        target_device=target_device,
    )
    actions = _protect_psx_nonempty_targets(actions)

    return {
        "mode": mode,
        "source": source_name,
        "target": target_name,
        "dry_run": True,
        "actions": actions,
        "summary": _summary(actions),
        "comparison_summary": comparison["summary"],
    }


def _plan_action(
    action: dict[str, Any],
    mode: str,
    source_name: str,
    target_name: str,
    config: dict[str, Any] | None,
    target_device: str | None,
) -> dict[str, Any]:
    status = action["status"]

    if status == "unchanged":
        native_rename = _native_path_rename_action(
            action,
            mode=mode,
            config=config,
            target_device=target_device,
        )
        if native_rename is not None:
            return native_rename
        return {
            "operation": "noop",
            "reason": "unchanged",
            "source": action["source"],
            "target": action["target"],
        }
    if status == "modified":
        planned_action = {
            "operation": _copy_operation(mode),
            "reason": "modified",
            "source": action["source"],
            "target": action["target"],
            "backup_target_before_apply": True,
        }
        native_paths = _native_path_mismatch(
            action,
            mode=mode,
            config=config,
            target_device=target_device,
        )
        if native_paths is not None:
            planned_action["reason"] = "modified_native_path_mismatch"
            planned_action["from_content_path"] = native_paths["current"]
            planned_action["to_content_path"] = native_paths["expected"]
            planned_action["rename_target_before_copy"] = True
        return planned_action
    if status == "modified_renamed":
        if _native_target_path_matches_source(
            action,
            mode=mode,
            config=config,
            target_device=target_device,
        ):
            return {
                "operation": _copy_operation(mode),
                "reason": "modified",
                "source": action["source"],
                "target": action["target"],
                "backup_target_before_apply": True,
            }
        return {
            "operation": _copy_operation(mode),
            "reason": "modified_renamed",
            "source": action["source"],
            "target": action["target"],
            "from_content_path": action["target"]["content_path"],
            "to_content_path": action["source"]["content_path"],
            "backup_target_before_apply": True,
            "rename_target_before_copy": True,
        }
    if status in {"renamed", "moved", "renamed_moved"}:
        if _native_target_path_matches_source(
            action,
            mode=mode,
            config=config,
            target_device=target_device,
        ):
            return {
                "operation": "noop",
                "reason": "unchanged_native_path",
                "source": action["source"],
                "target": action["target"],
            }
        if _is_hash_only_convertible_save_match(action, config=config):
            return {
                "operation": _copy_operation(mode),
                "reason": "added",
                "source": action["source"],
            }
        return {
            "operation": _rename_operation(mode),
            "reason": status,
            "source": action["source"],
            "target": action["target"],
            "from_content_path": action["target"]["content_path"],
            "to_content_path": action["source"]["content_path"],
            "copy_delete_required": target_name == "s3",
        }
    if status == "added":
        return {
            "operation": _copy_operation(mode),
            "reason": "added",
            "source": action["source"],
        }
    if status == "deleted":
        return {
            "operation": _trash_operation(mode),
            "reason": "missing_from_source_after_rename_detection",
            "target": action["target"],
            "trash_target": True,
            "hard_delete": False,
        }
    if status == "ambiguous_rename":
        return {
            "operation": "conflict",
            "reason": "ambiguous_rename",
            "source": action["source"],
            "candidates": action["candidates"],
            "requires_manual_resolution": True,
        }
    if status == "case_conflict":
        return {
            "operation": "conflict",
            "reason": "case_conflict",
            "source": action["source"],
            "candidates": action["candidates"],
            "requires_manual_resolution": True,
        }

    raise PlanError(f"unsupported comparison status: {status}")


def _copy_operation(mode: str) -> str:
    return "upload" if mode == "upload" else "download"


def _rename_operation(mode: str) -> str:
    return "rename_remote" if mode == "upload" else "rename_local"


def _trash_operation(mode: str) -> str:
    return "trash_remote" if mode == "upload" else "trash_local"


def _native_path_rename_action(
    action: dict[str, Any],
    mode: str,
    config: dict[str, Any] | None,
    target_device: str | None,
) -> dict[str, Any] | None:
    native_paths = _native_path_mismatch(
        action,
        mode=mode,
        config=config,
        target_device=target_device,
    )
    if native_paths is None:
        return None
    return {
        "operation": _rename_operation(mode),
        "reason": "native_path_mismatch",
        "source": action["source"],
        "target": action["target"],
        "from_content_path": native_paths["current"],
        "to_content_path": native_paths["expected"],
        "copy_delete_required": False,
    }


def _native_path_mismatch(
    action: dict[str, Any],
    mode: str,
    config: dict[str, Any] | None,
    target_device: str | None,
) -> dict[str, str] | None:
    if mode != "download" or config is None or target_device is None:
        return None
    if "source" not in action or "target" not in action:
        return None

    source = action["source"]
    target = action["target"]
    if not is_convertible_save(
        config=config,
        system=source["system"],
        content_type=source["type"],
    ):
        return None

    expected = native_save_content_path(
        config=config,
        system=source["system"],
        device=target_device,
        canonical_content_path=source["content_path"],
    )
    current = target.get("native_content_path", target["content_path"])
    if current == expected:
        return None
    return {
        "current": current,
        "expected": expected,
    }


def _native_target_path_matches_source(
    action: dict[str, Any],
    mode: str,
    config: dict[str, Any] | None,
    target_device: str | None,
) -> bool:
    if mode != "download" or config is None or target_device is None:
        return False
    if "source" not in action or "target" not in action:
        return False

    source = action["source"]
    target = action["target"]
    if not is_convertible_save(
        config=config,
        system=source["system"],
        content_type=source["type"],
    ):
        return False

    expected = native_save_content_path(
        config=config,
        system=source["system"],
        device=target_device,
        canonical_content_path=source["content_path"],
    )
    current = target.get("native_content_path", target["content_path"])
    return current == expected


def _is_hash_only_convertible_save_match(
    action: dict[str, Any],
    config: dict[str, Any] | None,
) -> bool:
    if action.get("match_reason") != "same_hash" or config is None:
        return False
    source = action.get("source")
    if source is None:
        return False
    if source["system"] != "psx":
        return False
    return is_convertible_save(
        config=config,
        system=source["system"],
        content_type=source["type"],
    )


def _protect_download_native_path_collisions(
    actions: list[dict[str, Any]],
    target: dict[str, Any],
    mode: str,
    config: dict[str, Any] | None,
    target_device: str | None,
) -> list[dict[str, Any]]:
    if mode != "download" or config is None or target_device is None:
        return actions

    protected_actions = list(actions)
    occupied_paths = {_item_native_path(item): item for item in target.get("items", [])}
    pending_writes: dict[str, list[int]] = {}

    for index, action in enumerate(actions):
        output_path = _download_native_output_path(
            action=action,
            config=config,
            target_device=target_device,
        )
        if output_path is None:
            continue

        pending_writes.setdefault(output_path, []).append(index)
        occupied = occupied_paths.get(output_path)
        source = action["source"]
        if occupied is not None and occupied["content_path"] != source["content_path"]:
            protected_actions[index] = _native_path_conflict(
                source=source,
                native_content_path=output_path,
                candidates=[occupied],
            )

    for output_path, indexes in pending_writes.items():
        if len(indexes) <= 1:
            continue
        candidates = [actions[index]["source"] for index in indexes]
        for index in indexes:
            protected_actions[index] = _native_path_conflict(
                source=actions[index]["source"],
                native_content_path=output_path,
                candidates=candidates,
            )

    return protected_actions


def _download_native_output_path(
    action: dict[str, Any],
    config: dict[str, Any],
    target_device: str,
) -> str | None:
    if action["operation"] != "download" or "source" not in action:
        return None

    source = action["source"]
    if not is_convertible_save(
        config=config,
        system=source["system"],
        content_type=source["type"],
    ):
        return None

    return native_save_content_path(
        config=config,
        system=source["system"],
        device=target_device,
        canonical_content_path=source["content_path"],
    )


def _item_native_path(item: dict[str, Any]) -> str:
    return item.get("native_content_path", item["content_path"])


def _native_path_conflict(
    source: dict[str, Any],
    native_content_path: str,
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "operation": "conflict",
        "reason": "native_path_conflict",
        "source": source,
        "native_content_path": native_content_path,
        "candidates": candidates,
        "requires_manual_resolution": True,
    }


def _protect_psx_nonempty_targets(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    protected_actions = list(actions)
    for index, action in enumerate(actions):
        if _psx_empty_source_would_overwrite_nonempty_target(action):
            protected_actions[index] = _psx_card_content_conflict(
                reason="empty_psx_source_would_overwrite_save",
                source=action["source"],
                target=action["target"],
            )
            continue
        if _psx_nonempty_target_would_be_deleted(action):
            protected_actions[index] = _psx_card_content_conflict(
                reason="nonempty_psx_target_would_be_deleted",
                source=None,
                target=action["target"],
            )
    return protected_actions


def _psx_empty_source_would_overwrite_nonempty_target(action: dict[str, Any]) -> bool:
    if action["operation"] not in {"upload", "download"}:
        return False
    source = action.get("source")
    target = action.get("target")
    if source is None or target is None:
        return False
    if not _is_psx_save(source) or not _is_psx_save(target):
        return False
    return _is_empty_psx_card(source) and not _is_empty_psx_card(target)


def _psx_nonempty_target_would_be_deleted(action: dict[str, Any]) -> bool:
    if action["operation"] not in {"trash_remote", "trash_local"}:
        return False
    target = action.get("target")
    if target is None or not _is_psx_save(target):
        return False
    return not _is_empty_psx_card(target)


def _is_psx_save(item: dict[str, Any]) -> bool:
    return item.get("system") == "psx" and item.get("type") == "saves"


def _is_empty_psx_card(item: dict[str, Any]) -> bool:
    native_sha256 = item.get("native_sha256")
    if native_sha256:
        return native_sha256 in EMPTY_CARD_SHA256ES

    sha256 = item.get("sha256")
    canonical_sha256 = item.get("canonical_sha256")
    hashes = {value for value in {sha256, canonical_sha256} if value}
    return bool(hashes) and hashes.issubset(EMPTY_CARD_SHA256ES)


def _psx_card_content_conflict(
    reason: str,
    source: dict[str, Any] | None,
    target: dict[str, Any],
) -> dict[str, Any]:
    conflict = {
        "operation": "conflict",
        "reason": reason,
        "target": target,
        "requires_manual_resolution": True,
    }
    if source is not None:
        conflict["source"] = source
    return conflict


def _summary(actions: list[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for action in actions:
        operation = action["operation"]
        summary[operation] = summary.get(operation, 0) + 1
    summary["total"] = len(actions)
    return summary
