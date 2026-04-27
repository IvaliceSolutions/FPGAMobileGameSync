"""Save-file conversion helpers."""

from __future__ import annotations

import shutil
import re
from pathlib import Path
from typing import Any

from .psx_memory_card import (
    PsxMemoryCardError,
    convert_psx_memory_card,
    inspect_psx_memory_card,
)


class ConversionError(Exception):
    """Raised when a save file cannot be converted safely."""


PSX_GAME_EXTENSIONS = {".iso", ".bin", ".chd", ".cue", ".m3u"}


def retroarch_game_file_stem(value: str) -> str:
    """Return the RetroArch save stem without assuming unknown game extensions."""
    name = Path(value).name
    suffix = Path(name).suffix
    if suffix.lower() in PSX_GAME_EXTENSIONS:
        return name[: -len(suffix)]
    return name


def infer_psx_retroarch_game_file(game_folder: Path) -> dict[str, Any]:
    if not game_folder.exists():
        raise ConversionError(f"PSX game folder not found: {game_folder}")
    if not game_folder.is_dir():
        raise ConversionError(f"PSX game folder is not a directory: {game_folder}")

    candidates = sorted(
        [
            path
            for path in game_folder.iterdir()
            if path.is_file() and path.suffix.lower() in PSX_GAME_EXTENSIONS
        ],
        key=lambda path: path.name.lower(),
    )
    if not candidates:
        raise ConversionError(f"no PSX game files found in: {game_folder}")
    if len(candidates) == 1:
        return {
            "strategy": "single_disc",
            "path": str(candidates[0]),
            "candidates": [str(path) for path in candidates],
        }

    for strategy, pattern in _psx_first_disc_patterns():
        matches = [path for path in candidates if pattern.search(path.stem)]
        if len(matches) == 1:
            return {
                "strategy": strategy,
                "path": str(matches[0]),
                "candidates": [str(path) for path in candidates],
            }
        if len(matches) > 1:
            raise ConversionError(
                f"ambiguous PSX first-disc match in {game_folder}: "
                + ", ".join(path.name for path in matches)
            )

    raise ConversionError(
        f"cannot infer first PSX disc in {game_folder}; provide --retroarch-game-file"
    )


def _psx_first_disc_patterns() -> list[tuple[str, re.Pattern[str]]]:
    return [
        ("disc_space_1", re.compile(r"(?i)(?:^|[^a-z0-9])disc\s+0?1(?:[^a-z0-9]|$)")),
        ("disc1", re.compile(r"(?i)(?:^|[^a-z0-9])disc0?1(?:[^a-z0-9]|$)")),
        ("one_of", re.compile(r"(?i)(?:^|[^a-z0-9])0?1\s+of(?:[^a-z0-9]|$)")),
        ("cd_space_1", re.compile(r"(?i)(?:^|[^a-z0-9])cd\s+0?1(?:[^a-z0-9]|$)")),
        ("cd1", re.compile(r"(?i)(?:^|[^a-z0-9])cd0?1(?:[^a-z0-9]|$)")),
        ("isolated_1", re.compile(r"(?i)(?:^|[^a-z0-9])0?1(?:[^a-z0-9]|$)")),
    ]


def convert_save_file(
    config: dict[str, Any],
    system: str,
    direction: str,
    source_path: Path,
    output_path: Path,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if direction not in {"mister-to-thor", "thor-to-mister"}:
        raise ConversionError(f"unsupported conversion direction: {direction}")
    if system not in config.get("systems", {}):
        raise ConversionError(f"unknown system: {system}")
    if not source_path.exists():
        raise ConversionError(f"source save not found: {source_path}")
    if not source_path.is_file():
        raise ConversionError(f"source save is not a file: {source_path}")

    conversion = config["systems"][system].get("save_conversion", {})
    strategy = conversion.get("strategy", "none")
    direction_key = direction.replace("-", "_")

    if strategy == "none":
        _validate_extension(source_path, [conversion.get("extension")] if conversion.get("extension") else [])
        return _copy_save(
            strategy=strategy,
            source_path=source_path,
            output_path=output_path,
            direction=direction,
            metadata=metadata,
        )
    if strategy == "raw_same_content":
        rules = conversion.get(direction_key, {})
        _validate_raw_same_content(source_path, rules)
        return _copy_save(
            strategy=strategy,
            source_path=source_path,
            output_path=output_path,
            direction=direction,
            metadata=metadata,
        )
    if strategy == "psx_raw_memory_card":
        rules = conversion.get(direction_key, {})
        _validate_psx_memory_card(source_path, rules, conversion)
        try:
            conversion_result = convert_psx_memory_card(source_path, output_path)
        except PsxMemoryCardError as exc:
            raise ConversionError(str(exc)) from exc
        return _converted_psx_result(
            strategy=strategy,
            source_path=source_path,
            output_path=output_path,
            direction=direction,
            metadata=metadata,
            conversion_result=conversion_result,
        )

    raise ConversionError(f"unsupported save conversion strategy: {strategy}")


def expected_output_suffix(config: dict[str, Any], system: str, direction: str) -> str | None:
    conversion = config["systems"][system].get("save_conversion", {})
    strategy = conversion.get("strategy", "none")
    direction_key = direction.replace("-", "_")
    if strategy == "none":
        return conversion.get("extension")
    rules = conversion.get(direction_key, {})
    if strategy == "raw_same_content":
        return rules.get("rename_extension_to")
    if strategy == "psx_raw_memory_card":
        return rules.get("output_extension")
    return None


def _copy_save(
    strategy: str,
    source_path: Path,
    output_path: Path,
    direction: str,
    metadata: dict[str, Any] | None,
) -> dict[str, Any]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, output_path)
    result = {
        "strategy": strategy,
        "direction": direction,
        "source": str(source_path),
        "output": str(output_path),
        "size": output_path.stat().st_size,
    }
    if metadata:
        result["metadata"] = metadata
    return result


def _converted_psx_result(
    strategy: str,
    source_path: Path,
    output_path: Path,
    direction: str,
    metadata: dict[str, Any] | None,
    conversion_result: dict[str, Any],
) -> dict[str, Any]:
    result = {
        "strategy": strategy,
        "direction": direction,
        "source": str(source_path),
        "output": str(output_path),
        "size": output_path.stat().st_size,
        "input_format": conversion_result["input_format"],
        "output_format": conversion_result["output_format"],
        "canonical_format": conversion_result["canonical_format"],
        "canonical_sha256": conversion_result["canonical_sha256"],
        "canonical_size": conversion_result["canonical_size"],
        "structural_validation": {
            "header_valid": conversion_result["structural_validation"]["header_valid"],
            "system_frame_checksums_valid": conversion_result[
                "structural_validation"
            ]["system_frame_checksums_valid"],
            "used_entry_count": conversion_result["structural_validation"][
                "used_entry_count"
            ],
            "used_block_count": conversion_result["structural_validation"][
                "used_block_count"
            ],
        },
    }
    if metadata:
        result["metadata"] = metadata
    return result


def _validate_raw_same_content(source_path: Path, rules: dict[str, Any]) -> None:
    allowed_inputs = _as_list(rules.get("rename_extension_from"))
    preserve_extensions = {
        extension.casefold()
        for extension in _as_list(rules.get("preserve_extensions"))
    }
    allowed_inputs.extend(preserve_extensions)
    _validate_extension(source_path, allowed_inputs)
    if source_path.suffix.casefold() in preserve_extensions:
        return
    sizes = [int(size) for size in rules.get("validate_sizes", [])]
    if sizes and source_path.stat().st_size not in sizes:
        raise ConversionError(
            f"unexpected raw save size for {source_path.name}: "
            f"{source_path.stat().st_size}; expected one of {sizes}"
        )


def _validate_psx_memory_card(
    source_path: Path,
    rules: dict[str, Any],
    conversion: dict[str, Any],
) -> None:
    _validate_extension(source_path, _as_list(rules.get("accepted_input_extensions")))
    if rules.get("validate_raw_card_size"):
        expected = int(conversion.get("expected_raw_card_size", 131072))
        actual = source_path.stat().st_size
        if actual != expected:
            raise ConversionError(
                f"unexpected PSX memory card size for {source_path.name}: "
                f"{actual}; expected {expected}"
            )


def inspect_save_file(config: dict[str, Any], system: str, source_path: Path) -> dict[str, Any]:
    if system not in config.get("systems", {}):
        raise ConversionError(f"unknown system: {system}")
    strategy = config["systems"][system].get("save_conversion", {}).get("strategy")
    if strategy == "psx_raw_memory_card":
        try:
            return inspect_psx_memory_card(source_path)
        except PsxMemoryCardError as exc:
            raise ConversionError(str(exc)) from exc
    if not source_path.exists():
        raise ConversionError(f"source save not found: {source_path}")
    if not source_path.is_file():
        raise ConversionError(f"source save is not a file: {source_path}")
    return {
        "path": str(source_path),
        "format": strategy or "unknown",
        "size": source_path.stat().st_size,
    }


def _validate_extension(source_path: Path, allowed: list[str]) -> None:
    allowed = [suffix.lower() for suffix in allowed if suffix]
    if allowed and source_path.suffix.lower() not in allowed:
        raise ConversionError(
            f"unexpected extension for {source_path.name}: "
            f"{source_path.suffix}; expected one of {allowed}"
        )


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]
