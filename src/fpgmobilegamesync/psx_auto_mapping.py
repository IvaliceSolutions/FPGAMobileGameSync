"""Infer PSX save-name mappings from game and save manifests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .converter import PSX_GAME_EXTENSIONS, retroarch_game_file_stem


def infer_psx_save_mappings(
    config: dict[str, Any],
    manifests: list[dict[str, Any]],
) -> dict[str, Any]:
    """Infer MiSTer-folder <-> SwanStation-save-stem mappings.

    SwanStation names saves after the launched game file. MiSTer names saves
    after the game folder. When Thor games are organized inside folders that
    match MiSTer folders, an existing Thor save can tell us which disc/file stem
    should be used for that MiSTer folder.
    """

    explicit_folders = {
        str(mapping["mister_game_folder"]).casefold()
        for mapping in config.get("save_mappings", {}).get("psx", [])
        if "mister_game_folder" in mapping
    }
    mister_folders = _mister_game_folders(manifests)
    thor_games = _thor_game_stems_by_folder(manifests, mister_folders)
    candidates = _thor_save_candidates_by_folder(manifests, thor_games)

    mappings = []
    skipped = []
    for folder_key, folder_candidates in sorted(candidates.items()):
        folder = folder_candidates[0]["mister_game_folder"]
        if folder.casefold() in explicit_folders:
            skipped.append(
                {
                    "mister_game_folder": folder,
                    "reason": "explicit_mapping_exists",
                    "candidates": _candidate_report(folder_candidates),
                }
            )
            continue
        chosen = max(
            folder_candidates,
            key=lambda item: (int(item.get("modified_ns", 0)), item["retroarch_game_file_stem"]),
        )
        mappings.append(
            {
                "mister_game_folder": chosen["mister_game_folder"],
                "retroarch_game_file_stem": chosen["retroarch_game_file_stem"],
                "inferred_from": chosen["native_content_path"],
            }
        )
        if len({item["retroarch_game_file_stem"] for item in folder_candidates}) > 1:
            skipped.append(
                {
                    "mister_game_folder": folder,
                    "reason": "multiple_thor_saves_for_folder_chose_newest",
                    "chosen": chosen["retroarch_game_file_stem"],
                    "candidates": _candidate_report(folder_candidates),
                }
            )

    return {
        "mappings": mappings,
        "skipped": skipped,
        "summary": {
            "inferred": len(mappings),
            "skipped": len(skipped),
        },
    }


def merge_inferred_psx_save_mappings(
    config: dict[str, Any],
    inferred: dict[str, Any],
) -> None:
    mappings = inferred.get("mappings", [])
    if not mappings:
        return

    save_mappings = config.setdefault("save_mappings", {})
    psx_mappings = save_mappings.setdefault("psx", [])
    explicit_folders = {
        str(mapping["mister_game_folder"]).casefold()
        for mapping in psx_mappings
        if "mister_game_folder" in mapping
    }
    for mapping in mappings:
        if str(mapping["mister_game_folder"]).casefold() in explicit_folders:
            continue
        psx_mappings.append(
            {
                "mister_game_folder": mapping["mister_game_folder"],
                "retroarch_game_file_stem": mapping["retroarch_game_file_stem"],
            }
        )
        explicit_folders.add(str(mapping["mister_game_folder"]).casefold())


def _mister_game_folders(manifests: list[dict[str, Any]]) -> dict[str, str]:
    folders: dict[str, str] = {}
    for item in _manifest_items(manifests, device="mister", content_type="games"):
        folder = _mister_folder_from_game_path(item["content_path"])
        folders.setdefault(folder.casefold(), folder)
    return folders


def _thor_game_stems_by_folder(
    manifests: list[dict[str, Any]],
    mister_folders: dict[str, str],
) -> dict[str, str]:
    games: dict[str, str] = {}
    for item in _manifest_items(manifests, device="thor", content_type="games"):
        path = Path(item["content_path"])
        if path.suffix.casefold() not in PSX_GAME_EXTENSIONS:
            continue
        folder = _mister_folder_from_thor_game_path(path)
        if folder is None:
            continue
        known_folder = mister_folders.get(folder.casefold())
        if known_folder is None:
            continue
        stem = retroarch_game_file_stem(path.name)
        games.setdefault(stem.casefold(), known_folder)
    return games


def _thor_save_candidates_by_folder(
    manifests: list[dict[str, Any]],
    thor_games: dict[str, str],
) -> dict[str, list[dict[str, Any]]]:
    candidates: dict[str, list[dict[str, Any]]] = {}
    for item in _manifest_items(manifests, device="thor", content_type="saves"):
        native_stem = Path(item.get("native_content_path", item["content_path"])).stem
        folder = thor_games.get(native_stem.casefold())
        if folder is None:
            continue
        candidates.setdefault(folder.casefold(), []).append(
            {
                "mister_game_folder": folder,
                "retroarch_game_file_stem": native_stem,
                "native_content_path": item.get("native_content_path", item["content_path"]),
                "modified_ns": int(item.get("modified_ns", 0)),
            }
        )
    return candidates


def _manifest_items(
    manifests: list[dict[str, Any]],
    device: str,
    content_type: str,
) -> list[dict[str, Any]]:
    return [
        item
        for manifest in manifests
        for item in manifest.get("items", [])
        if item.get("device") == device
        and item.get("system") == "psx"
        and item.get("type") == content_type
    ]


def _mister_folder_from_game_path(content_path: str) -> str:
    path = Path(content_path)
    if path.parent != Path("."):
        return path.parts[0]
    return path.stem


def _mister_folder_from_thor_game_path(path: Path) -> str | None:
    if path.parent == Path("."):
        return path.stem
    return path.parts[0]


def _candidate_report(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "retroarch_game_file_stem": candidate["retroarch_game_file_stem"],
            "native_content_path": candidate["native_content_path"],
            "modified_ns": candidate["modified_ns"],
        }
        for candidate in candidates
    ]
