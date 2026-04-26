"""Save path normalization between device-native and canonical store names."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .converter import retroarch_game_file_stem


CANONICAL_SAVE_EXTENSION = ".sav"


def is_convertible_save(config: dict[str, Any], system: str, content_type: str) -> bool:
    return (
        content_type == "saves"
        and system in config.get("systems", {})
        and bool(config["systems"][system].get("save_conversion"))
    )


def canonical_save_content_path(
    config: dict[str, Any],
    system: str,
    device: str,
    native_content_path: str,
) -> str:
    conversion = config["systems"][system].get("save_conversion", {})
    path = Path(native_content_path)
    canonical_stem = _canonical_save_stem(config, system, device, path.stem)
    suffix = _canonical_save_suffix(conversion)
    return _with_stem_and_suffix(path, canonical_stem, suffix)


def native_save_content_path(
    config: dict[str, Any],
    system: str,
    device: str,
    canonical_content_path: str,
) -> str:
    conversion = config["systems"][system].get("save_conversion", {})
    path = Path(canonical_content_path)
    native_stem = _native_save_stem(config, system, device, path.stem)
    suffix = _native_save_suffix(conversion, device) or path.suffix
    return _with_stem_and_suffix(path, native_stem, suffix)


def _canonical_save_suffix(conversion: dict[str, Any]) -> str:
    strategy = conversion.get("strategy")
    if strategy == "raw_same_content":
        return str(
            conversion.get("thor_to_mister", {}).get(
                "rename_extension_to",
                CANONICAL_SAVE_EXTENSION,
            )
        )
    if strategy == "psx_raw_memory_card":
        return str(
            conversion.get("thor_to_mister", {}).get(
                "output_extension",
                CANONICAL_SAVE_EXTENSION,
            )
        )
    return CANONICAL_SAVE_EXTENSION


def _native_save_suffix(conversion: dict[str, Any], device: str) -> str | None:
    strategy = conversion.get("strategy")
    if strategy == "raw_same_content":
        if device == "thor":
            return conversion.get("mister_to_thor", {}).get("rename_extension_to")
        if device == "mister":
            return conversion.get("thor_to_mister", {}).get("rename_extension_to")
    if strategy == "psx_raw_memory_card":
        if device == "thor":
            return conversion.get("mister_to_thor", {}).get("output_extension")
        if device == "mister":
            return conversion.get("thor_to_mister", {}).get("output_extension")
    return None


def _canonical_save_stem(
    config: dict[str, Any],
    system: str,
    device: str,
    native_stem: str,
) -> str:
    if system != "psx":
        return native_stem

    mapping = _find_psx_mapping_by_device_stem(config, device, native_stem)
    if mapping:
        return str(mapping["mister_game_folder"])
    return native_stem


def _native_save_stem(
    config: dict[str, Any],
    system: str,
    device: str,
    canonical_stem: str,
) -> str:
    if system != "psx":
        return canonical_stem

    mapping = _find_psx_mapping_by_mister_folder(config, canonical_stem)
    if not mapping:
        return canonical_stem
    if device == "thor":
        return _mapping_retroarch_stem(mapping)
    return str(mapping["mister_game_folder"])


def _find_psx_mapping_by_device_stem(
    config: dict[str, Any],
    device: str,
    native_stem: str,
) -> dict[str, Any] | None:
    folded_stem = native_stem.casefold()
    for mapping in _psx_mappings(config):
        if device == "mister" and str(mapping["mister_game_folder"]).casefold() == folded_stem:
            return mapping
        if device == "thor" and _mapping_retroarch_stem(mapping).casefold() == folded_stem:
            return mapping
    return None


def _find_psx_mapping_by_mister_folder(
    config: dict[str, Any],
    mister_game_folder: str,
) -> dict[str, Any] | None:
    folded = mister_game_folder.casefold()
    for mapping in _psx_mappings(config):
        if str(mapping["mister_game_folder"]).casefold() == folded:
            return mapping
    return None


def _psx_mappings(config: dict[str, Any]) -> list[dict[str, Any]]:
    mappings = config.get("save_mappings", {}).get("psx", [])
    return [mapping for mapping in mappings if "mister_game_folder" in mapping]


def _mapping_retroarch_stem(mapping: dict[str, Any]) -> str:
    if "retroarch_game_file_stem" in mapping:
        return str(mapping["retroarch_game_file_stem"])
    if "retroarch_game_file" in mapping:
        return retroarch_game_file_stem(str(mapping["retroarch_game_file"]))
    return str(mapping["mister_game_folder"])


def _with_stem_and_suffix(path: Path, stem: str, suffix: str) -> str:
    parent = path.parent
    filename = f"{stem}{suffix}"
    if str(parent) == ".":
        return filename
    return str(parent / filename)
