"""Action planning for upload and download flows."""

from __future__ import annotations

from typing import Any

from .compare import compare_manifests


class PlanError(Exception):
    """Raised when a sync plan cannot be created."""


def build_plan(
    source: dict[str, Any],
    target: dict[str, Any],
    mode: str,
    source_name: str = "source",
    target_name: str = "target",
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
        _plan_action(action, mode=mode, source_name=source_name, target_name=target_name)
        for action in comparison["actions"]
    ]

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
) -> dict[str, Any]:
    status = action["status"]

    if status == "unchanged":
        return {
            "operation": "noop",
            "reason": "unchanged",
            "source": action["source"],
            "target": action["target"],
        }
    if status == "modified":
        return {
            "operation": _copy_operation(mode),
            "reason": "modified",
            "source": action["source"],
            "target": action["target"],
            "backup_target_before_apply": True,
        }
    if status in {"renamed", "moved", "renamed_moved"}:
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

    raise PlanError(f"unsupported comparison status: {status}")


def _copy_operation(mode: str) -> str:
    return "upload" if mode == "upload" else "download"


def _rename_operation(mode: str) -> str:
    return "rename_remote" if mode == "upload" else "rename_local"


def _trash_operation(mode: str) -> str:
    return "trash_remote" if mode == "upload" else "trash_local"


def _summary(actions: list[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for action in actions:
        operation = action["operation"]
        summary[operation] = summary.get(operation, 0) + 1
    summary["total"] = len(actions)
    return summary
