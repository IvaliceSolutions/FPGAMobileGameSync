"""Save-file conversion helpers."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any


class ConversionError(Exception):
    """Raised when a save file cannot be converted safely."""


def convert_save_file(
    config: dict[str, Any],
    system: str,
    direction: str,
    source_path: Path,
    output_path: Path,
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
        )
    if strategy == "raw_same_content":
        rules = conversion.get(direction_key, {})
        _validate_raw_same_content(source_path, rules)
        return _copy_save(
            strategy=strategy,
            source_path=source_path,
            output_path=output_path,
            direction=direction,
        )
    if strategy == "psx_raw_memory_card":
        rules = conversion.get(direction_key, {})
        _validate_psx_memory_card(source_path, rules, conversion)
        return _copy_save(
            strategy=strategy,
            source_path=source_path,
            output_path=output_path,
            direction=direction,
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
) -> dict[str, Any]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, output_path)
    return {
        "strategy": strategy,
        "direction": direction,
        "source": str(source_path),
        "output": str(output_path),
        "size": output_path.stat().st_size,
    }


def _validate_raw_same_content(source_path: Path, rules: dict[str, Any]) -> None:
    allowed_inputs = _as_list(rules.get("rename_extension_from"))
    _validate_extension(source_path, allowed_inputs)
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
