"""Configuration pre-flight checks."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


class DoctorError(Exception):
    """Raised when doctor options are invalid."""


@dataclass(frozen=True)
class DoctorCheck:
    severity: str
    code: str
    message: str
    context: dict[str, Any]


def run_doctor(
    config: dict[str, Any],
    devices: list[str] | None = None,
    systems: list[str] | None = None,
    types: list[str] | None = None,
    backend: str = "local",
    check_paths: bool = False,
    check_env: bool = False,
) -> dict[str, Any]:
    if backend not in {"local", "s3"}:
        raise DoctorError(f"unsupported doctor backend: {backend}")

    selected_devices = devices or sorted(config.get("devices", {}).keys())
    selected_systems = systems or list(config.get("defaults", {}).get("systems", []))
    selected_types = types or list(config.get("defaults", {}).get("types", []))
    checks: list[DoctorCheck] = []

    _check_top_level(config, checks)
    _check_devices(config, selected_devices, check_paths, checks)
    _check_systems(config, selected_devices, selected_systems, selected_types, check_paths, checks)
    _check_sync_modes(config, checks)
    if backend == "s3" or check_env:
        _check_s3(config, checks)

    summary = _summary(checks)
    return {
        "status": _status(summary),
        "backend": backend,
        "devices": selected_devices,
        "systems": selected_systems,
        "types": selected_types,
        "checks": [asdict(check) for check in checks],
        "summary": summary,
    }


def _check_top_level(config: dict[str, Any], checks: list[DoctorCheck]) -> None:
    _require_mapping(config, "defaults", checks, "missing_defaults")
    _require_mapping(config, "devices", checks, "missing_devices")
    _require_mapping(config, "systems", checks, "missing_systems")
    _require_mapping(config, "sync_modes", checks, "missing_sync_modes")
    defaults = config.get("defaults", {})
    if isinstance(defaults, dict):
        _require_list(defaults, "systems", checks, "missing_default_systems")
        _require_list(defaults, "types", checks, "missing_default_types")


def _check_devices(
    config: dict[str, Any],
    devices: list[str],
    check_paths: bool,
    checks: list[DoctorCheck],
) -> None:
    configured_devices = config.get("devices", {})
    if not isinstance(configured_devices, dict):
        return

    for device in devices:
        device_config = configured_devices.get(device)
        if not isinstance(device_config, dict):
            checks.append(
                DoctorCheck("error", "unknown_device", f"unknown device: {device}", {"device": device})
            )
            continue
        local = device_config.get("local")
        if not isinstance(local, dict):
            checks.append(
                DoctorCheck(
                    "error",
                    "missing_device_local",
                    f"device has no local configuration: {device}",
                    {"device": device},
                )
            )
            continue
        root = local.get("root")
        if not isinstance(root, str) or not root:
            checks.append(
                DoctorCheck(
                    "error",
                    "missing_device_root",
                    f"device has no local root: {device}",
                    {"device": device},
                )
            )
        elif check_paths:
            _check_path_exists(Path(root), "device_root", {"device": device}, checks)
        trash = local.get("trash")
        if not isinstance(trash, str) or not trash:
            checks.append(
                DoctorCheck(
                    "warning",
                    "missing_device_trash",
                    f"device has no local trash path: {device}",
                    {"device": device},
                )
            )


def _check_systems(
    config: dict[str, Any],
    devices: list[str],
    systems: list[str],
    types: list[str],
    check_paths: bool,
    checks: list[DoctorCheck],
) -> None:
    configured_systems = config.get("systems", {})
    configured_devices = config.get("devices", {})
    if not isinstance(configured_systems, dict) or not isinstance(configured_devices, dict):
        return

    for system in systems:
        system_config = configured_systems.get(system)
        if not isinstance(system_config, dict):
            checks.append(
                DoctorCheck("error", "unknown_system", f"unknown system: {system}", {"system": system})
            )
            continue
        paths = system_config.get("paths")
        extensions = system_config.get("file_extensions")
        if not isinstance(paths, dict):
            checks.append(
                DoctorCheck(
                    "error",
                    "missing_system_paths",
                    f"system has no paths mapping: {system}",
                    {"system": system},
                )
            )
            continue
        if not isinstance(extensions, dict):
            checks.append(
                DoctorCheck(
                    "error",
                    "missing_system_extensions",
                    f"system has no file_extensions mapping: {system}",
                    {"system": system},
                )
            )
            continue
        for device in devices:
            device_paths = paths.get(device)
            if not isinstance(device_paths, dict):
                checks.append(
                    DoctorCheck(
                        "error",
                        "missing_device_system_paths",
                        f"system paths missing for {device}/{system}",
                        {"device": device, "system": system},
                    )
                )
                continue
            root = _device_root(config, device)
            for content_type in types:
                _check_system_type_path(
                    root=root,
                    device=device,
                    system=system,
                    content_type=content_type,
                    path_config=device_paths.get(content_type),
                    check_paths=check_paths,
                    checks=checks,
                )
                _check_extensions(
                    extensions=extensions,
                    device=device,
                    system=system,
                    content_type=content_type,
                    checks=checks,
                )


def _check_system_type_path(
    root: Path | None,
    device: str,
    system: str,
    content_type: str,
    path_config: Any,
    check_paths: bool,
    checks: list[DoctorCheck],
) -> None:
    context = {"device": device, "system": system, "type": content_type}
    if path_config is None:
        checks.append(
            DoctorCheck(
                "info",
                "path_not_configured",
                f"path not configured for {device}/{system}/{content_type}",
                context,
            )
        )
        return
    if not isinstance(path_config, (str, list)):
        checks.append(
            DoctorCheck(
                "error",
                "invalid_path_config",
                f"invalid path config for {device}/{system}/{content_type}",
                context,
            )
        )
        return
    rel_paths = path_config if isinstance(path_config, list) else [path_config]
    if not rel_paths:
        checks.append(
            DoctorCheck(
                "info",
                "path_not_configured",
                f"path not configured for {device}/{system}/{content_type}",
                context,
            )
        )
        return
    for rel_path in rel_paths:
        if not isinstance(rel_path, str) or not rel_path:
            checks.append(
                DoctorCheck(
                    "error",
                    "invalid_path_config",
                    f"invalid path config for {device}/{system}/{content_type}",
                    context,
                )
            )
            continue
        if check_paths and root is not None:
            _check_path_exists(root / rel_path, "content_path", {**context, "path": rel_path}, checks)


def _check_extensions(
    extensions: dict[str, Any],
    device: str,
    system: str,
    content_type: str,
    checks: list[DoctorCheck],
) -> None:
    configured = extensions.get(content_type)
    if isinstance(configured, dict):
        configured = configured.get(device)
    if configured is None:
        checks.append(
            DoctorCheck(
                "warning",
                "missing_extensions",
                f"extensions missing for {device}/{system}/{content_type}",
                {"device": device, "system": system, "type": content_type},
            )
        )
        return
    if not isinstance(configured, list) or not all(
        isinstance(extension, str) and extension.startswith(".") for extension in configured
    ):
        checks.append(
            DoctorCheck(
                "error",
                "invalid_extensions",
                f"invalid extensions for {device}/{system}/{content_type}",
                {"device": device, "system": system, "type": content_type},
            )
        )


def _check_sync_modes(config: dict[str, Any], checks: list[DoctorCheck]) -> None:
    devices = config.get("devices", {})
    sync_modes = config.get("sync_modes", {})
    if not isinstance(devices, dict) or not isinstance(sync_modes, dict):
        return
    for name, mode in sync_modes.items():
        if not isinstance(mode, dict):
            checks.append(
                DoctorCheck("error", "invalid_sync_mode", f"invalid sync mode: {name}", {"mode": name})
            )
            continue
        source = mode.get("source")
        target = mode.get("target")
        if source not in devices:
            checks.append(
                DoctorCheck(
                    "error",
                    "invalid_sync_source",
                    f"sync mode source is not configured: {name}",
                    {"mode": name, "source": source},
                )
            )
        if target not in devices:
            checks.append(
                DoctorCheck(
                    "error",
                    "invalid_sync_target",
                    f"sync mode target is not configured: {name}",
                    {"mode": name, "target": target},
                )
            )


def _check_s3(config: dict[str, Any], checks: list[DoctorCheck]) -> None:
    s3 = config.get("s3")
    if not isinstance(s3, dict):
        checks.append(DoctorCheck("error", "missing_s3", "missing S3 configuration", {}))
        return
    for key in ("bucket", "endpoint_url_env", "access_key_id_env", "secret_access_key_env"):
        value = s3.get(key)
        if not isinstance(value, str) or not value:
            checks.append(
                DoctorCheck(
                    "error",
                    "missing_s3_config_value",
                    f"missing S3 config value: {key}",
                    {"key": key},
                )
            )
            continue
        if key.endswith("_env") and not os.environ.get(value):
            checks.append(
                DoctorCheck(
                    "error",
                    "missing_s3_env",
                    f"missing required environment variable: {value}",
                    {"env": value},
                )
            )


def _device_root(config: dict[str, Any], device: str) -> Path | None:
    root = config.get("devices", {}).get(device, {}).get("local", {}).get("root")
    if isinstance(root, str) and root:
        return Path(root)
    return None


def _check_path_exists(
    path: Path,
    code_suffix: str,
    context: dict[str, Any],
    checks: list[DoctorCheck],
) -> None:
    if path.exists():
        checks.append(
            DoctorCheck(
                "info",
                f"{code_suffix}_exists",
                f"path exists: {path}",
                {**context, "absolute_path": str(path)},
            )
        )
    else:
        checks.append(
            DoctorCheck(
                "warning",
                f"{code_suffix}_missing",
                f"path does not exist: {path}",
                {**context, "absolute_path": str(path)},
            )
        )


def _require_mapping(
    config: dict[str, Any],
    key: str,
    checks: list[DoctorCheck],
    code: str,
) -> None:
    if not isinstance(config.get(key), dict):
        checks.append(DoctorCheck("error", code, f"missing or invalid config section: {key}", {}))


def _require_list(
    config: dict[str, Any],
    key: str,
    checks: list[DoctorCheck],
    code: str,
) -> None:
    if not isinstance(config.get(key), list):
        checks.append(DoctorCheck("error", code, f"missing or invalid config list: {key}", {}))


def _summary(checks: list[DoctorCheck]) -> dict[str, int]:
    summary = {"error": 0, "warning": 0, "info": 0, "total": len(checks)}
    for check in checks:
        summary[check.severity] = summary.get(check.severity, 0) + 1
    return summary


def _status(summary: dict[str, int]) -> str:
    if summary.get("error", 0):
        return "error"
    if summary.get("warning", 0):
        return "warning"
    return "ok"
