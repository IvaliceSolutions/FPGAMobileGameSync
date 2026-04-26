"""Configuration loading."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class ConfigError(Exception):
    """Raised when the sync configuration cannot be loaded."""


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"config file not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".json":
        return _load_json(path)
    if suffix in {".yaml", ".yml"}:
        return _load_yaml(path)

    raise ConfigError(f"unsupported config format: {path.suffix}")


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ConfigError("top-level JSON config must be an object")
    return data


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:
        raise ConfigError(
            "YAML config requires PyYAML. Use mister-thor-sync.json or install "
            "the yaml extra."
        ) from exc

    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ConfigError("top-level YAML config must be a mapping")
    return data

