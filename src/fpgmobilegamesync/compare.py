"""Manifest comparison and rename detection."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any


class CompareError(Exception):
    """Raised when manifests cannot be compared."""


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise CompareError(f"manifest not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict) or not isinstance(data.get("items"), list):
        raise CompareError(f"invalid manifest: {path}")
    return data


def compare_manifests(
    source: dict[str, Any],
    target: dict[str, Any],
    source_name: str = "source",
    target_name: str = "target",
) -> dict[str, Any]:
    source_items = [_normalise_item(item) for item in source.get("items", [])]
    target_items = [_normalise_item(item) for item in target.get("items", [])]

    target_by_path = {_path_key(item): item for item in target_items}
    target_by_hash: dict[tuple[str, str, str, int], list[dict[str, Any]]] = defaultdict(list)
    for item in target_items:
        target_by_hash[_hash_key(item)].append(item)

    matched_target_paths: set[tuple[str, str, str]] = set()
    actions: list[dict[str, Any]] = []

    for source_item in source_items:
        source_path_key = _path_key(source_item)
        same_path_target = target_by_path.get(source_path_key)
        if same_path_target is not None:
            matched_target_paths.add(source_path_key)
            if _same_content(source_item, same_path_target):
                actions.append(_action("unchanged", source_item, same_path_target))
            else:
                actions.append(_action("modified", source_item, same_path_target))
            continue

        same_hash_targets = [
            item
            for item in target_by_hash.get(_hash_key(source_item), [])
            if _path_key(item) not in matched_target_paths
        ]
        if len(same_hash_targets) == 1:
            target_item = same_hash_targets[0]
            matched_target_paths.add(_path_key(target_item))
            actions.append(
                _action(
                    _rename_status(
                        old_path=target_item["content_path"],
                        new_path=source_item["content_path"],
                    ),
                    source_item,
                    target_item,
                )
            )
            continue
        if len(same_hash_targets) > 1:
            actions.append(
                {
                    "status": "ambiguous_rename",
                    "source": source_item,
                    "candidates": same_hash_targets,
                }
            )
            continue

        actions.append({"status": "added", "source": source_item})

    for target_item in target_items:
        if _path_key(target_item) not in matched_target_paths:
            actions.append({"status": "deleted", "target": target_item})

    return {
        "source": source_name,
        "target": target_name,
        "actions": sorted(actions, key=_action_sort_key),
        "summary": _summary(actions),
    }


def _normalise_item(item: dict[str, Any]) -> dict[str, Any]:
    required = ("system", "type", "sha256", "size")
    missing = [key for key in required if key not in item]
    if missing:
        raise CompareError(f"manifest item missing required keys: {', '.join(missing)}")

    normalised = dict(item)
    if "content_path" not in normalised:
        if "relative_path" not in normalised:
            raise CompareError("manifest item needs content_path or relative_path")
        normalised["content_path"] = normalised["relative_path"]
    return normalised


def _path_key(item: dict[str, Any]) -> tuple[str, str, str]:
    return (item["system"], item["type"], item["content_path"])


def _hash_key(item: dict[str, Any]) -> tuple[str, str, str, int]:
    return (item["system"], item["type"], item["sha256"], int(item["size"]))


def _same_content(source_item: dict[str, Any], target_item: dict[str, Any]) -> bool:
    return (
        source_item["sha256"] == target_item["sha256"]
        and int(source_item["size"]) == int(target_item["size"])
    )


def _rename_status(old_path: str, new_path: str) -> str:
    old = Path(old_path)
    new = Path(new_path)
    same_name = old.name == new.name
    same_parent = old.parent == new.parent
    if same_parent and not same_name:
        return "renamed"
    if same_name and not same_parent:
        return "moved"
    return "renamed_moved"


def _action(
    status: str,
    source_item: dict[str, Any],
    target_item: dict[str, Any],
) -> dict[str, Any]:
    return {
        "status": status,
        "source": source_item,
        "target": target_item,
    }


def _summary(actions: list[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for action in actions:
        status = action["status"]
        summary[status] = summary.get(status, 0) + 1
    summary["total"] = len(actions)
    return summary


def _action_sort_key(action: dict[str, Any]) -> tuple[str, str, str, str]:
    item = action.get("source") or action.get("target") or {}
    return (
        action["status"],
        item.get("system", ""),
        item.get("type", ""),
        item.get("content_path", ""),
    )
