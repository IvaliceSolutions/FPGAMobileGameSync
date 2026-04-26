"""Filesystem scanner for configured sync roots."""

from __future__ import annotations

import fnmatch
import hashlib
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable


class ScanError(Exception):
    """Raised when a scan cannot be completed."""


@dataclass(frozen=True)
class ScanItem:
    device: str
    system: str
    type: str
    absolute_path: str
    relative_path: str
    size: int
    modified_ns: int
    sha256: str


def scan(
    config: dict[str, Any],
    device: str,
    systems: list[str] | None = None,
    types: list[str] | None = None,
) -> dict[str, Any]:
    _require_device(config, device)

    selected_systems = systems or list(config["defaults"]["systems"])
    selected_types = types or list(config["defaults"]["types"])
    items: list[ScanItem] = []
    skipped: list[dict[str, str]] = []

    for system in selected_systems:
        if system not in config["systems"]:
            raise ScanError(f"unknown system: {system}")
        for content_type in selected_types:
            if content_type not in config["defaults"]["types"]:
                raise ScanError(f"unknown content type: {content_type}")
            found, missing = _scan_system_type(config, device, system, content_type)
            items.extend(found)
            skipped.extend(missing)

    return {
        "device": device,
        "systems": selected_systems,
        "types": selected_types,
        "items": [asdict(item) for item in sorted(items, key=lambda item: item.absolute_path)],
        "skipped": skipped,
        "summary": {
            "item_count": len(items),
            "skipped_count": len(skipped),
            "total_size": sum(item.size for item in items),
        },
    }


def _scan_system_type(
    config: dict[str, Any],
    device: str,
    system: str,
    content_type: str,
) -> tuple[list[ScanItem], list[dict[str, str]]]:
    paths = config["systems"][system]["paths"][device].get(content_type)
    if paths is None:
        return [], [
            {
                "device": device,
                "system": system,
                "type": content_type,
                "reason": "not_configured",
            }
        ]

    root = Path(config["devices"][device]["local"]["root"])
    rel_paths = paths if isinstance(paths, list) else [paths]
    extensions = _extensions_for(config, device, system, content_type)
    items: list[ScanItem] = []
    skipped: list[dict[str, str]] = []

    for rel_path in rel_paths:
        absolute = root / rel_path
        if not absolute.exists():
            skipped.append(
                {
                    "device": device,
                    "system": system,
                    "type": content_type,
                    "path": str(absolute),
                    "reason": "missing",
                }
            )
            continue
        if absolute.is_file():
            if _matches_extensions(absolute, extensions):
                items.append(_scan_file(device, system, content_type, root, absolute))
            continue
        if absolute.is_dir():
            for file_path in _walk_files(config, absolute):
                if _matches_extensions(file_path, extensions):
                    items.append(_scan_file(device, system, content_type, root, file_path))
            continue
        skipped.append(
            {
                "device": device,
                "system": system,
                "type": content_type,
                "path": str(absolute),
                "reason": "not_file_or_directory",
            }
        )

    return items, skipped


def _scan_file(
    device: str,
    system: str,
    content_type: str,
    root: Path,
    path: Path,
) -> ScanItem:
    stat = path.stat()
    return ScanItem(
        device=device,
        system=system,
        type=content_type,
        absolute_path=str(path),
        relative_path=str(path.relative_to(root)),
        size=stat.st_size,
        modified_ns=stat.st_mtime_ns,
        sha256=_sha256(path),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _walk_files(config: dict[str, Any], root: Path) -> Iterable[Path]:
    excluded_dirs = set(config.get("exclusions", {}).get("global", {}).get("directories", []))
    patterns = config.get("exclusions", {}).get("global", {}).get("filename_patterns", [])

    for current_root, dirnames, filenames in os.walk(root):
        dirnames[:] = [
            dirname for dirname in dirnames if dirname not in excluded_dirs
        ]
        for filename in filenames:
            if any(fnmatch.fnmatch(filename, pattern) for pattern in patterns):
                continue
            yield Path(current_root) / filename


def _extensions_for(
    config: dict[str, Any],
    device: str,
    system: str,
    content_type: str,
) -> set[str]:
    configured = config["systems"][system]["file_extensions"].get(content_type, [])
    if isinstance(configured, dict):
        configured = configured.get(device, [])
    return {extension.lower() for extension in configured}


def _matches_extensions(path: Path, extensions: set[str]) -> bool:
    if not extensions:
        return True
    return path.suffix.lower() in extensions


def _require_device(config: dict[str, Any], device: str) -> None:
    if device not in config.get("devices", {}):
        raise ScanError(f"unknown device: {device}")
