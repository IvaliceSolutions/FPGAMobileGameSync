"""Generate small shell launchers for configured sync profiles."""

from __future__ import annotations

import os
import re
import shlex
from pathlib import Path
from typing import Any


class ScriptGenerationError(Exception):
    """Raised when profile launch scripts cannot be generated."""


PROFILE_GROUPS = {
    "thor": ("thor-pull", "thor-push"),
    "mister": ("mister-push", "mister-pull"),
    "third": ("third-mister-to-thor", "third-thor-to-mister"),
}


def generate_profile_scripts(
    config: dict[str, Any],
    output_dir: Path,
    profiles: list[str] | None = None,
    project_root: Path | None = None,
    config_path: Path | None = None,
    python_bin: str = "python3",
    apply: bool = False,
    pretty: bool = False,
) -> dict[str, Any]:
    configured_profiles = config.get("sync_profiles", {})
    if not isinstance(configured_profiles, dict) or not configured_profiles:
        raise ScriptGenerationError("configuration has no sync_profiles")

    selected_profiles = profiles or sorted(configured_profiles)
    missing = [profile for profile in selected_profiles if profile not in configured_profiles]
    if missing:
        raise ScriptGenerationError(f"unknown sync profile(s): {', '.join(missing)}")

    root = (project_root or Path.cwd()).expanduser().resolve()
    cfg = (config_path or (root / "mister-thor-sync.json")).expanduser()
    if not cfg.is_absolute():
        cfg = root / cfg
    output_dir.mkdir(parents=True, exist_ok=True)

    scripts = []
    for profile in selected_profiles:
        filename = f"fpgms-{_safe_filename(profile)}.sh"
        path = output_dir / filename
        content = _render_script(
            profile=profile,
            project_root=root,
            config_path=cfg,
            python_bin=python_bin,
            apply=apply,
            pretty=pretty,
        )
        path.write_text(content, encoding="utf-8")
        path.chmod(path.stat().st_mode | 0o755)
        scripts.append(
            {
                "profile": profile,
                "path": str(path),
                "apply": apply,
                "pretty": pretty,
            }
        )

    return {
        "status": "ok",
        "output_dir": str(output_dir),
        "project_root": str(root),
        "config_path": str(cfg),
        "scripts": scripts,
        "summary": {
            "script_count": len(scripts),
        },
    }


def generate_launcher_bundle(
    config: dict[str, Any],
    output_dir: Path,
    target: str = "all",
    profiles: list[str] | None = None,
    project_root: Path | None = None,
    config_path: Path | None = None,
    python_bin: str = "python3",
    apply: bool = False,
    pretty: bool = False,
    include_env: bool = True,
) -> dict[str, Any]:
    selected_profiles = _select_bundle_profiles(config, target, profiles)
    scripts = generate_profile_scripts(
        config=config,
        output_dir=output_dir,
        profiles=selected_profiles,
        project_root=project_root,
        config_path=config_path,
        python_bin=python_bin,
        apply=apply,
        pretty=pretty,
    )
    env_template = None
    if include_env:
        env_template = generate_env_template(
            config=config,
            output_path=output_dir / "fpgms.env",
        )
    readme_path = output_dir / "README.txt"
    readme_path.write_text(
        _render_bundle_readme(
            profiles=selected_profiles,
            include_env=include_env,
            apply=apply,
            pretty=pretty,
        ),
        encoding="utf-8",
    )
    return {
        "status": "ok",
        "target": target,
        "output_dir": str(output_dir),
        "scripts": scripts["scripts"],
        "env_template": env_template,
        "readme": str(readme_path),
        "summary": {
            "script_count": scripts["summary"]["script_count"],
            "env_template": include_env,
            "readme": True,
        },
    }


def generate_env_template(
    config: dict[str, Any],
    output_path: Path,
    include_launcher_vars: bool = True,
) -> dict[str, Any]:
    env_names = _collect_env_names(config)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# FPGAMobileGameSync environment template.",
        "# Fill the values locally; do not commit secrets.",
        "",
    ]
    if include_launcher_vars:
        lines.extend(
            [
                "# Optional launcher overrides.",
                'export FPGMS_PROJECT_ROOT=""',
                'export FPGMS_CONFIG=""',
                'export FPGMS_PYTHON=""',
                "",
            ]
        )
    if env_names:
        lines.append("# S3 and SFTP credentials referenced by the sync config.")
        lines.extend(f'export {name}=""' for name in env_names)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {
        "status": "ok",
        "path": str(output_path),
        "env": env_names,
        "summary": {
            "env_count": len(env_names),
            "launcher_env_count": 3 if include_launcher_vars else 0,
        },
    }


def _render_script(
    profile: str,
    project_root: Path,
    config_path: Path,
    python_bin: str,
    apply: bool,
    pretty: bool,
) -> str:
    command = [
        '"${PYTHON_BIN}"',
        "-m",
        "fpgmobilegamesync.cli",
        "--config",
        '"${CONFIG_PATH}"',
        "sync",
        "--profile",
        shlex.quote(profile),
    ]
    if apply:
        command.append("--apply")
    if pretty:
        command.append("--pretty")
    command.append('"$@"')
    return "\n".join(
        [
            "#!/usr/bin/env sh",
            "set -eu",
            "",
            f"PROJECT_ROOT=${{FPGMS_PROJECT_ROOT:-{shlex.quote(os.fspath(project_root))}}}",
            f"CONFIG_PATH=${{FPGMS_CONFIG:-{shlex.quote(os.fspath(config_path))}}}",
            f"PYTHON_BIN=${{FPGMS_PYTHON:-{shlex.quote(python_bin)}}}",
            "",
            'cd "${PROJECT_ROOT}"',
            'PYTHONPATH="${PROJECT_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \\',
            "  exec " + " ".join(command),
            "",
        ]
    )


def _select_bundle_profiles(
    config: dict[str, Any],
    target: str,
    profiles: list[str] | None,
) -> list[str]:
    configured_profiles = config.get("sync_profiles", {})
    if not isinstance(configured_profiles, dict) or not configured_profiles:
        raise ScriptGenerationError("configuration has no sync_profiles")
    if profiles:
        return profiles
    if target == "all":
        return sorted(configured_profiles)
    if target not in PROFILE_GROUPS:
        valid = ", ".join(["all", *sorted(PROFILE_GROUPS)])
        raise ScriptGenerationError(f"unknown launcher target {target!r}; expected one of: {valid}")
    return list(PROFILE_GROUPS[target])


def _render_bundle_readme(
    profiles: list[str],
    include_env: bool,
    apply: bool,
    pretty: bool,
) -> str:
    example_profile = _safe_filename(profiles[0]) if profiles else "profile"
    example_args = ["--system gba", "--type saves"]
    if not apply:
        example_args.append("--apply")
    if not pretty:
        example_args.append("--pretty")
    lines = [
        "FPGAMobileGameSync launchers",
        "============================",
        "",
    ]
    if include_env:
        lines.extend(
            [
                "1. Edit fpgms.env and fill local secrets.",
                "2. Load the environment:",
                "",
                "   . ./fpgms.env",
                "",
                "3. Run a launcher:",
            ]
        )
    else:
        lines.append("Run a launcher:")
    lines.extend(
        [
            "",
            f"   ./fpgms-{example_profile}.sh {' '.join(example_args)}",
            "",
            "Generated profiles:",
            *[f"- {profile}" for profile in profiles],
            "",
            "Extra CLI flags passed to a launcher are forwarded to the sync command.",
        ]
    )
    return "\n".join(lines) + "\n"


def _safe_filename(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    return safe.strip(".-") or "profile"


def _collect_env_names(config: dict[str, Any]) -> list[str]:
    names: set[str] = set()
    s3 = config.get("s3", {})
    if isinstance(s3, dict):
        for key in ("endpoint_url_env", "access_key_id_env", "secret_access_key_env"):
            _add_env_name(names, s3.get(key))
    devices = config.get("devices", {})
    if isinstance(devices, dict):
        for device in devices.values():
            if not isinstance(device, dict):
                continue
            remote = device.get("remote", {})
            if not isinstance(remote, dict):
                continue
            for key in ("username_env", "password_env", "private_key_env"):
                _add_env_name(names, remote.get(key))
    return sorted(names)


def _add_env_name(names: set[str], value: object) -> None:
    if isinstance(value, str) and value:
        names.add(value)
