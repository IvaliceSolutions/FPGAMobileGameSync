"""SFTP-backed scanner for configured sync roots."""

from __future__ import annotations

import fnmatch
import hashlib
import posixpath
from dataclasses import asdict
from typing import Any, Iterable

from .psx_memory_card import PsxMemoryCardError, canonical_psx_memory_card_bytes_from_data
from .save_paths import canonical_save_content_path, is_convertible_save
from .scanner import ScanError, ScanItem
from .sftp_client import RemoteDirEntry, SftpDeviceClient, SftpError


def scan_remote(
    config: dict[str, Any],
    device: str,
    systems: list[str] | None = None,
    types: list[str] | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    _require_device(config, device)
    selected_systems = systems or list(config["defaults"]["systems"])
    selected_types = types or list(config["defaults"]["types"])
    owns_client = client is None
    remote_client = client or SftpDeviceClient.from_config(config, device)

    items: list[ScanItem] = []
    skipped: list[dict[str, str]] = []
    try:
        for system in selected_systems:
            if system not in config["systems"]:
                raise ScanError(f"unknown system: {system}")
            for content_type in selected_types:
                if content_type not in config["defaults"]["types"]:
                    raise ScanError(f"unknown content type: {content_type}")
                found, missing = _scan_remote_system_type(
                    config=config,
                    client=remote_client,
                    device=device,
                    system=system,
                    content_type=content_type,
                )
                items.extend(found)
                skipped.extend(missing)
    finally:
        if owns_client:
            remote_client.close()

    return {
        "device": device,
        "backend": "sftp",
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


def _scan_remote_system_type(
    config: dict[str, Any],
    client: Any,
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

    root = _remote_root(config, device)
    rel_paths = paths if isinstance(paths, list) else [paths]
    extensions = _extensions_for(config, device, system, content_type)
    items: list[ScanItem] = []
    skipped: list[dict[str, str]] = []

    for rel_path in rel_paths:
        absolute = _join(root, rel_path)
        try:
            stat = client.stat(absolute)
        except SftpError:
            skipped.append(
                {
                    "device": device,
                    "system": system,
                    "type": content_type,
                    "path": absolute,
                    "reason": "missing",
                }
            )
            continue
        if stat.is_file:
            if _matches_extensions(absolute, extensions):
                items.append(
                    _scan_remote_file(
                        config=config,
                        client=client,
                        device=device,
                        system=system,
                        content_type=content_type,
                        device_root=root,
                        content_root=posixpath.dirname(absolute),
                        path=absolute,
                        stat=stat,
                    )
                )
            continue
        if stat.is_dir:
            for entry_path, entry_stat in _walk_remote_files(config, client, absolute):
                if _matches_extensions(entry_path, extensions):
                    items.append(
                        _scan_remote_file(
                            config=config,
                            client=client,
                            device=device,
                            system=system,
                            content_type=content_type,
                            device_root=root,
                            content_root=absolute,
                            path=entry_path,
                            stat=entry_stat,
                        )
                    )
            continue
        skipped.append(
            {
                "device": device,
                "system": system,
                "type": content_type,
                "path": absolute,
                "reason": "not_file_or_directory",
            }
        )

    return items, skipped


def _scan_remote_file(
    config: dict[str, Any],
    client: Any,
    device: str,
    system: str,
    content_type: str,
    device_root: str,
    content_root: str,
    path: str,
    stat: Any,
) -> ScanItem:
    data = client.read_file(path)
    native_size = stat.size
    native_sha256 = hashlib.sha256(data).hexdigest()
    canonical_size = native_size
    canonical_sha256 = native_sha256
    native_content_path = _relpath(path, content_root)
    content_path = native_content_path
    if is_convertible_save(config=config, system=system, content_type=content_type):
        content_path = canonical_save_content_path(
            config=config,
            system=system,
            device=device,
            native_content_path=native_content_path,
        )
        if system == "psx":
            try:
                canonical_bytes = canonical_psx_memory_card_bytes_from_data(data, path)
            except PsxMemoryCardError as exc:
                raise ScanError(str(exc)) from exc
            canonical_size = len(canonical_bytes)
            canonical_sha256 = hashlib.sha256(canonical_bytes).hexdigest()
    return ScanItem(
        device=device,
        system=system,
        type=content_type,
        absolute_path=path,
        relative_path=_relpath(path, device_root),
        content_path=content_path,
        native_content_path=native_content_path,
        sync_key=f"systems/{system}/{content_type}/{content_path}",
        size=canonical_size,
        native_size=native_size,
        canonical_size=canonical_size,
        modified_ns=stat.modified_ns,
        sha256=canonical_sha256,
        native_sha256=native_sha256,
        canonical_sha256=canonical_sha256,
    )


def _walk_remote_files(config: dict[str, Any], client: Any, root: str) -> Iterable[tuple[str, Any]]:
    excluded_dirs = set(config.get("exclusions", {}).get("global", {}).get("directories", []))
    patterns = config.get("exclusions", {}).get("global", {}).get("filename_patterns", [])
    stack = [root]
    while stack:
        current = stack.pop()
        entries: list[RemoteDirEntry] = client.listdir(current)
        for entry in sorted(entries, key=lambda item: item.name, reverse=True):
            path = _join(current, entry.name)
            if entry.stat.is_dir:
                if entry.name not in excluded_dirs:
                    stack.append(path)
                continue
            if not entry.stat.is_file:
                continue
            if any(fnmatch.fnmatch(entry.name, pattern) for pattern in patterns):
                continue
            yield path, entry.stat


def _remote_root(config: dict[str, Any], device: str) -> str:
    remote_root = config["devices"][device].get("remote", {}).get("root")
    if isinstance(remote_root, str) and remote_root:
        return _normalize(remote_root)
    local_root = config["devices"][device].get("local", {}).get("root")
    if isinstance(local_root, str) and local_root:
        return _normalize(local_root)
    raise ScanError(f"device has no root: {device}")


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


def _matches_extensions(path: str, extensions: set[str]) -> bool:
    if not extensions:
        return True
    return posixpath.splitext(path)[1].lower() in extensions


def _require_device(config: dict[str, Any], device: str) -> None:
    if device not in config.get("devices", {}):
        raise ScanError(f"unknown device: {device}")


def _join(*parts: str) -> str:
    return _normalize(posixpath.join(*parts))


def _normalize(path: str) -> str:
    normalized = posixpath.normpath(path)
    if path.startswith("/") and not normalized.startswith("/"):
        return f"/{normalized}"
    return normalized


def _relpath(path: str, start: str) -> str:
    value = posixpath.relpath(path, start)
    return "" if value == "." else value
