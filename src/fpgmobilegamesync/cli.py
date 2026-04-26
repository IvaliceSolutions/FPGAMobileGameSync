"""Command line interface for FPGAMobileGameSync."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .compare import CompareError, compare_manifests, load_manifest
from .config import ConfigError, load_config
from .converter import (
    ConversionError,
    convert_save_file,
    expected_output_suffix,
    infer_psx_retroarch_game_file,
    inspect_save_file,
    retroarch_game_file_stem,
)
from .doctor import DoctorError, run_doctor
from .executor import (
    ApplyError,
    apply_plan_from_s3_to_local_target,
    apply_plan_to_s3_store,
    apply_plan_to_local_store,
    apply_plan_to_local_target,
    load_plan,
)
from .object_store import LocalObjectStore, ObjectStoreError
from .planner import PlanError, build_plan
from .remote_scanner import scan_remote
from .s3_store import S3ObjectStore
from .scanner import ScanError, scan
from .sftp_client import SftpError
from .sync_engine import SyncError, run_local_sync, run_s3_sync


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fpgms",
        description="Manual MiSTer FPGA and Android RetroArch sync tooling.",
    )
    parser.add_argument(
        "--config",
        default="mister-thor-sync.json",
        help="Path to the YAML or JSON sync configuration.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_parser = subparsers.add_parser("scan", help="Scan configured files.")
    scan_parser.add_argument(
        "--backend",
        choices=("local", "sftp"),
        default="local",
        help="Scan backend. Local reads mounted paths; sftp reads the configured remote block.",
    )
    scan_parser.add_argument(
        "--device",
        required=True,
        choices=("mister", "thor"),
        help="Device profile to scan.",
    )
    scan_parser.add_argument(
        "--system",
        action="append",
        choices=("gba", "snes", "psx"),
        help="System to scan. Can be repeated. Defaults to all configured systems.",
    )
    scan_parser.add_argument(
        "--type",
        action="append",
        choices=("games", "saves", "bios", "thumbnails"),
        help="Content type to scan. Can be repeated. Defaults to all configured types.",
    )
    scan_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )

    compare_parser = subparsers.add_parser(
        "compare",
        help="Compare two scan manifests and detect changes.",
    )
    compare_parser.add_argument(
        "--source",
        required=True,
        help="Path to the source manifest JSON.",
    )
    compare_parser.add_argument(
        "--target",
        required=True,
        help="Path to the target manifest JSON.",
    )
    compare_parser.add_argument(
        "--source-name",
        default="source",
        help="Label for the source side.",
    )
    compare_parser.add_argument(
        "--target-name",
        default="target",
        help="Label for the target side.",
    )
    compare_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )

    plan_parser = subparsers.add_parser(
        "plan",
        help="Build an upload or download plan from two manifests.",
    )
    plan_parser.add_argument(
        "--mode",
        required=True,
        choices=("upload", "download"),
        help="Plan direction. Upload means source -> S3, download means S3 -> target.",
    )
    plan_parser.add_argument(
        "--source",
        required=True,
        help="Path to the source manifest JSON.",
    )
    plan_parser.add_argument(
        "--target",
        required=True,
        help="Path to the target manifest JSON.",
    )
    plan_parser.add_argument(
        "--source-name",
        default="source",
        help="Label for the source side.",
    )
    plan_parser.add_argument(
        "--target-name",
        default="target",
        help="Label for the target side.",
    )
    plan_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )

    store_parser = subparsers.add_parser(
        "store",
        help="Inspect a local object-store directory that simulates S3.",
    )
    store_subparsers = store_parser.add_subparsers(dest="store_command", required=True)
    store_scan_parser = store_subparsers.add_parser(
        "scan",
        help="Scan object-store contents.",
    )
    store_scan_parser.add_argument(
        "--backend",
        choices=("local", "s3"),
        default="local",
        help="Store backend to scan.",
    )
    store_scan_parser.add_argument(
        "--root",
        help="Local object-store root directory.",
    )
    store_scan_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )
    store_trash_parser = store_subparsers.add_parser(
        "trash",
        help="List or restore local object-store trash entries.",
    )
    store_trash_subparsers = store_trash_parser.add_subparsers(
        dest="store_trash_command",
        required=True,
    )
    store_trash_list_parser = store_trash_subparsers.add_parser(
        "list",
        help="List logical deletes in the object-store trash.",
    )
    store_trash_list_parser.add_argument(
        "--backend",
        choices=("local", "s3"),
        default="local",
        help="Store backend to inspect.",
    )
    store_trash_list_parser.add_argument(
        "--root",
        help="Local object-store root directory.",
    )
    store_trash_list_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )
    store_trash_restore_parser = store_trash_subparsers.add_parser(
        "restore",
        help="Restore one object from the object-store trash.",
    )
    store_trash_restore_parser.add_argument(
        "--backend",
        choices=("local", "s3"),
        default="local",
        help="Store backend to restore from.",
    )
    store_trash_restore_parser.add_argument(
        "--root",
        help="Local object-store root directory.",
    )
    store_trash_restore_parser.add_argument(
        "--trash-key",
        required=True,
        help="Trash key to restore, as returned by store trash list.",
    )
    store_trash_restore_parser.add_argument(
        "--to-key",
        help="Optional destination sync key. Defaults to the original sync key.",
    )
    store_trash_restore_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow restore to replace an existing object at the destination key.",
    )
    store_trash_restore_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )
    store_lock_parser = store_subparsers.add_parser(
        "lock",
        help="Inspect or clear S3 sync locks.",
    )
    store_lock_subparsers = store_lock_parser.add_subparsers(
        dest="store_lock_command",
        required=True,
    )
    store_lock_list_parser = store_lock_subparsers.add_parser(
        "list",
        help="List S3 sync locks.",
    )
    store_lock_list_parser.add_argument(
        "--backend",
        choices=("s3",),
        default="s3",
        help="Store backend to inspect. Lock maintenance is only available for S3.",
    )
    store_lock_list_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )
    store_lock_clear_parser = store_lock_subparsers.add_parser(
        "clear",
        help="Clear one S3 sync lock.",
    )
    store_lock_clear_parser.add_argument(
        "--backend",
        choices=("s3",),
        default="s3",
        help="Store backend to update. Lock maintenance is only available for S3.",
    )
    store_lock_clear_parser.add_argument(
        "--name",
        default="sync",
        help="Lock name to clear. Defaults to sync.",
    )
    store_lock_clear_parser.add_argument(
        "--force",
        action="store_true",
        help="Clear an active lock. Use only after verifying no sync is running.",
    )
    store_lock_clear_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )

    apply_parser = subparsers.add_parser(
        "apply",
        help="Apply a plan. Currently supports remote operations on the local object-store backend.",
    )
    apply_parser.add_argument(
        "--plan",
        required=True,
        help="Path to plan JSON.",
    )
    apply_parser.add_argument(
        "--backend",
        required=True,
        choices=("local", "s3"),
        help="Backend to apply against.",
    )
    apply_parser.add_argument(
        "--store-root",
        required=False,
        help="Local object-store root directory.",
    )
    apply_parser.add_argument(
        "--target-root",
        help="Local target root directory for download plans.",
    )
    apply_parser.add_argument(
        "--trash-root",
        help="Local trash root directory for download plans.",
    )
    apply_parser.add_argument(
        "--timestamp-utc",
        help="Fixed UTC timestamp for deterministic trash/backup paths.",
    )
    apply_parser.add_argument(
        "--allow-conflicts",
        action="store_true",
        help="Skip conflict actions instead of refusing the plan.",
    )
    apply_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )

    convert_parser = subparsers.add_parser(
        "convert-save",
        help="Convert or validate one save file for a sync direction.",
    )
    convert_parser.add_argument(
        "--system",
        required=True,
        choices=("gba", "snes", "psx"),
        help="System save format to convert.",
    )
    convert_parser.add_argument(
        "--direction",
        required=True,
        choices=("mister-to-thor", "thor-to-mister"),
        help="Conversion direction.",
    )
    convert_parser.add_argument(
        "--source",
        required=True,
        help="Source save file.",
    )
    convert_parser.add_argument(
        "--output",
        required=True,
        help="Output save file or output directory.",
    )
    convert_parser.add_argument(
        "--output-stem",
        help="Output filename stem to use when --output is a directory.",
    )
    convert_parser.add_argument(
        "--game-folder",
        help="Deprecated alias for --mister-game-folder.",
    )
    convert_parser.add_argument(
        "--mister-game-folder",
        help="MiSTer PSX game folder name/path to keep in conversion metadata.",
    )
    convert_parser.add_argument(
        "--retroarch-game-file",
        help="RetroArch PSX game file path; its filename stem is used for Thor save naming.",
    )
    convert_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )

    inspect_parser = subparsers.add_parser(
        "inspect-save",
        help="Inspect one save file and report its detected format.",
    )
    inspect_parser.add_argument(
        "--system",
        required=True,
        choices=("gba", "snes", "psx"),
        help="System save format to inspect.",
    )
    inspect_parser.add_argument(
        "--source",
        required=True,
        help="Source save file.",
    )
    inspect_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Run configuration pre-flight checks.",
    )
    doctor_parser.add_argument(
        "--backend",
        choices=("local", "s3"),
        default="local",
        help="Backend to validate. S3 also checks required environment variables.",
    )
    doctor_parser.add_argument(
        "--device",
        action="append",
        choices=("mister", "thor"),
        help="Device profile to check. Can be repeated. Defaults to all devices.",
    )
    doctor_parser.add_argument(
        "--system",
        action="append",
        choices=("gba", "snes", "psx"),
        help="System to check. Can be repeated. Defaults to configured defaults.",
    )
    doctor_parser.add_argument(
        "--type",
        action="append",
        choices=("games", "saves", "bios", "thumbnails"),
        help="Content type to check. Can be repeated. Defaults to configured defaults.",
    )
    doctor_parser.add_argument(
        "--check-paths",
        action="store_true",
        help="Check whether configured local paths currently exist.",
    )
    doctor_parser.add_argument(
        "--check-env",
        action="store_true",
        help="Check S3 environment variables even when using the local backend.",
    )
    doctor_parser.add_argument(
        "--check-remote",
        action="store_true",
        help="Check configured SFTP remote blocks and their environment variables.",
    )
    doctor_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )

    sync_parser = subparsers.add_parser(
        "sync",
        help="Run a full source -> store -> target sync workflow.",
    )
    sync_parser.add_argument(
        "--direction",
        required=True,
        choices=("mister-to-thor", "thor-to-mister"),
        help="Configured sync direction to run.",
    )
    sync_parser.add_argument(
        "--backend",
        required=True,
        choices=("local", "s3"),
        help="Sync backend. The local backend uses a filesystem object-store; s3 uses Garage/S3.",
    )
    sync_parser.add_argument(
        "--scan-backend",
        choices=("local", "sftp"),
        default="local",
        help="Device scan/apply backend for sync. Use sftp from a third controller device.",
    )
    sync_parser.add_argument(
        "--source-backend",
        choices=("local", "sftp"),
        help="Override scan/apply backend for the source device.",
    )
    sync_parser.add_argument(
        "--target-backend",
        choices=("local", "sftp"),
        help="Override scan/apply backend for the target device.",
    )
    sync_parser.add_argument(
        "--store-root",
        help="Local object-store root directory. Required for --backend local.",
    )
    sync_parser.add_argument(
        "--source-root",
        help="Override the configured source device root.",
    )
    sync_parser.add_argument(
        "--target-root",
        help="Override the configured target device root.",
    )
    sync_parser.add_argument(
        "--system",
        action="append",
        choices=("gba", "snes", "psx"),
        help="System to sync. Can be repeated. Defaults to all configured systems.",
    )
    sync_parser.add_argument(
        "--type",
        action="append",
        choices=("games", "saves", "bios", "thumbnails"),
        help="Content type to sync. Can be repeated. Defaults to all configured types.",
    )
    sync_parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the generated plans. Without this flag the command is a dry run.",
    )
    sync_parser.add_argument(
        "--timestamp-utc",
        help="Fixed UTC timestamp for deterministic trash/backup paths.",
    )
    sync_parser.add_argument(
        "--report-dir",
        help="Directory where sync manifests, plans, apply results, and summary are written.",
    )
    sync_parser.add_argument(
        "--allow-conflicts",
        action="store_true",
        help="Skip conflict actions instead of refusing the plan during apply.",
    )
    sync_parser.add_argument(
        "--no-lock",
        action="store_true",
        help="Disable the S3 sync lock during --apply. Intended only for recovery/debugging.",
    )
    sync_parser.add_argument(
        "--lock-ttl-seconds",
        type=int,
        default=1800,
        help="S3 sync lock TTL in seconds. Defaults to 1800.",
    )
    sync_parser.add_argument(
        "--lock-owner",
        help="Optional owner label recorded in the S3 sync lock.",
    )
    sync_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "scan":
            config = load_config(Path(args.config))
            if args.backend == "local":
                manifest = scan(
                    config=config,
                    device=args.device,
                    systems=args.system,
                    types=args.type,
                )
            elif args.backend == "sftp":
                manifest = scan_remote(
                    config=config,
                    device=args.device,
                    systems=args.system,
                    types=args.type,
                )
            else:
                raise ScanError(f"unsupported scan backend: {args.backend}")
            json.dump(
                manifest,
                sys.stdout,
                indent=2 if args.pretty else None,
                sort_keys=True,
            )
            sys.stdout.write("\n")
            return 0
        if args.command == "compare":
            plan = compare_manifests(
                source=load_manifest(Path(args.source)),
                target=load_manifest(Path(args.target)),
                source_name=args.source_name,
                target_name=args.target_name,
            )
            json.dump(
                plan,
                sys.stdout,
                indent=2 if args.pretty else None,
                sort_keys=True,
            )
            sys.stdout.write("\n")
            return 0
        if args.command == "plan":
            plan = build_plan(
                source=load_manifest(Path(args.source)),
                target=load_manifest(Path(args.target)),
                mode=args.mode,
                source_name=args.source_name,
                target_name=args.target_name,
            )
            json.dump(
                plan,
                sys.stdout,
                indent=2 if args.pretty else None,
                sort_keys=True,
            )
            sys.stdout.write("\n")
            return 0
        if args.command == "store" and args.store_command == "scan":
            config = load_config(Path(args.config))
            manifest = _store_from_args(args, config).scan()
            json.dump(
                manifest,
                sys.stdout,
                indent=2 if args.pretty else None,
                sort_keys=True,
            )
            sys.stdout.write("\n")
            return 0
        if args.command == "store" and args.store_command == "trash":
            config = load_config(Path(args.config))
            store = _store_from_args(args, config)
            if args.store_trash_command == "list":
                result = store.list_trash()
            elif args.store_trash_command == "restore":
                result = store.restore_trash_object(
                    trash_key=args.trash_key,
                    to_sync_key=args.to_key,
                    overwrite=args.overwrite,
                )
            else:
                raise ObjectStoreError(f"unknown trash command: {args.store_trash_command}")
            json.dump(
                result,
                sys.stdout,
                indent=2 if args.pretty else None,
                sort_keys=True,
            )
            sys.stdout.write("\n")
            return 0
        if args.command == "store" and args.store_command == "lock":
            config = load_config(Path(args.config))
            store = _store_from_args(args, config)
            if not isinstance(store, S3ObjectStore):
                raise ObjectStoreError("store lock commands require --backend s3")
            if args.store_lock_command == "list":
                result = store.list_locks()
            elif args.store_lock_command == "clear":
                result = store.clear_lock(name=args.name, force=args.force)
            else:
                raise ObjectStoreError(f"unknown lock command: {args.store_lock_command}")
            json.dump(
                result,
                sys.stdout,
                indent=2 if args.pretty else None,
                sort_keys=True,
            )
            sys.stdout.write("\n")
            return 0
        if args.command == "apply":
            config = load_config(Path(args.config))
            plan = load_plan(Path(args.plan))
            if args.backend == "s3":
                if plan.get("mode") == "download" or args.target_root:
                    if not args.target_root:
                        raise ApplyError("--target-root is required for S3 download apply")
                    result = apply_plan_from_s3_to_local_target(
                        plan=plan,
                        config=config,
                        target_root=Path(args.target_root),
                        trash_root=Path(args.trash_root) if args.trash_root else None,
                        timestamp_utc=args.timestamp_utc,
                        allow_conflicts=args.allow_conflicts,
                        target_device=str(plan.get("target", "target")),
                    )
                else:
                    result = apply_plan_to_s3_store(
                        plan=plan,
                        config=config,
                        timestamp_utc=args.timestamp_utc,
                        allow_conflicts=args.allow_conflicts,
                        source_device=str(plan.get("source", "source")),
                    )
            elif args.backend == "local" and (plan.get("mode") == "download" or args.target_root):
                if not args.target_root:
                    raise ApplyError("--target-root is required for local target apply")
                result = apply_plan_to_local_target(
                    plan=plan,
                    target_root=Path(args.target_root),
                    trash_root=Path(args.trash_root) if args.trash_root else None,
                    timestamp_utc=args.timestamp_utc,
                    allow_conflicts=args.allow_conflicts,
                    config=config,
                    target_device=str(plan.get("target", "target")),
                )
            elif args.backend == "local":
                if not args.store_root:
                    raise ApplyError("--store-root is required for local object-store apply")
                result = apply_plan_to_local_store(
                    plan=plan,
                    store_root=Path(args.store_root),
                    timestamp_utc=args.timestamp_utc,
                    allow_conflicts=args.allow_conflicts,
                    config=config,
                    source_device=str(plan.get("source", "source")),
                )
            else:
                raise ApplyError(f"unsupported apply backend: {args.backend}")
            json.dump(
                result,
                sys.stdout,
                indent=2 if args.pretty else None,
                sort_keys=True,
            )
            sys.stdout.write("\n")
            return 0
        if args.command == "convert-save":
            config = load_config(Path(args.config))
            source_path = Path(args.source)
            inferred_retroarch_game_file = _infer_retroarch_game_file(
                system=args.system,
                direction=args.direction,
                mister_game_folder=args.mister_game_folder or args.game_folder,
                retroarch_game_file=args.retroarch_game_file,
            )
            output_path = _resolve_save_output_path(
                config=config,
                system=args.system,
                direction=args.direction,
                source_path=source_path,
                output=Path(args.output),
                output_stem=_save_output_stem(
                    output_stem=args.output_stem,
                    game_folder=args.game_folder,
                    mister_game_folder=args.mister_game_folder,
                    retroarch_game_file=inferred_retroarch_game_file["path"]
                    if inferred_retroarch_game_file
                    else args.retroarch_game_file,
                    direction=args.direction,
                ),
            )
            result = convert_save_file(
                config=config,
                system=args.system,
                direction=args.direction,
                source_path=source_path,
                output_path=output_path,
                metadata=_save_metadata(
                    mister_game_folder=args.mister_game_folder or args.game_folder,
                    retroarch_game_file=inferred_retroarch_game_file["path"]
                    if inferred_retroarch_game_file
                    else args.retroarch_game_file,
                    retroarch_inference=inferred_retroarch_game_file,
                ),
            )
            json.dump(
                result,
                sys.stdout,
                indent=2 if args.pretty else None,
                sort_keys=True,
            )
            sys.stdout.write("\n")
            return 0
        if args.command == "inspect-save":
            config = load_config(Path(args.config))
            result = inspect_save_file(
                config=config,
                system=args.system,
                source_path=Path(args.source),
            )
            json.dump(
                result,
                sys.stdout,
                indent=2 if args.pretty else None,
                sort_keys=True,
            )
            sys.stdout.write("\n")
            return 0
        if args.command == "doctor":
            config = load_config(Path(args.config))
            result = run_doctor(
                config=config,
                devices=args.device,
                systems=args.system,
                types=args.type,
                backend=args.backend,
                check_paths=args.check_paths,
                check_env=args.check_env,
                check_remote=args.check_remote,
            )
            json.dump(
                result,
                sys.stdout,
                indent=2 if args.pretty else None,
                sort_keys=True,
            )
            sys.stdout.write("\n")
            return 1 if result["status"] == "error" else 0
        if args.command == "sync":
            config = load_config(Path(args.config))
            source_backend = args.source_backend or args.scan_backend
            target_backend = args.target_backend or args.scan_backend
            if args.backend == "local":
                if source_backend != "local" or target_backend != "local":
                    raise SyncError("SFTP device backends require --backend s3")
                if not args.store_root:
                    raise SyncError("--store-root is required for local sync")
                result = run_local_sync(
                    config=config,
                    direction=args.direction,
                    store_root=Path(args.store_root),
                    source_root=Path(args.source_root) if args.source_root else None,
                    target_root=Path(args.target_root) if args.target_root else None,
                    systems=args.system,
                    types=args.type,
                    apply=args.apply,
                    timestamp_utc=args.timestamp_utc,
                    allow_conflicts=args.allow_conflicts,
                    report_dir=Path(args.report_dir) if args.report_dir else None,
                )
            elif args.backend == "s3":
                result = run_s3_sync(
                    config=config,
                    direction=args.direction,
                    source_root=Path(args.source_root) if args.source_root else None,
                    target_root=Path(args.target_root) if args.target_root else None,
                    systems=args.system,
                    types=args.type,
                    apply=args.apply,
                    timestamp_utc=args.timestamp_utc,
                    allow_conflicts=args.allow_conflicts,
                    report_dir=Path(args.report_dir) if args.report_dir else None,
                    scan_backend=args.scan_backend,
                    use_lock=not args.no_lock,
                    lock_ttl_seconds=args.lock_ttl_seconds,
                    lock_owner=args.lock_owner,
                    source_scan_backend=source_backend,
                    target_scan_backend=target_backend,
                )
            else:
                raise SyncError(f"unsupported sync backend: {args.backend}")
            json.dump(
                result,
                sys.stdout,
                indent=2 if args.pretty else None,
                sort_keys=True,
            )
            sys.stdout.write("\n")
            return 0
    except (
        ApplyError,
        CompareError,
        ConfigError,
        ConversionError,
        DoctorError,
        ObjectStoreError,
        PlanError,
        ScanError,
        SftpError,
        SyncError,
    ) as exc:
        parser.exit(2, f"error: {exc}\n")

    parser.exit(2, "error: unknown command\n")
    return 2


def _resolve_save_output_path(
    config: dict,
    system: str,
    direction: str,
    source_path: Path,
    output: Path,
    output_stem: str | None = None,
) -> Path:
    if output.exists() and output.is_dir():
        suffix = expected_output_suffix(config, system, direction) or source_path.suffix
        stem = output_stem or source_path.stem
        return output / f"{stem}{suffix}"
    return output


def _save_output_stem(
    output_stem: str | None,
    game_folder: str | None,
    mister_game_folder: str | None,
    retroarch_game_file: str | None,
    direction: str,
) -> str | None:
    if output_stem:
        return output_stem
    if retroarch_game_file and direction == "mister-to-thor":
        return retroarch_game_file_stem(retroarch_game_file)
    folder = mister_game_folder or game_folder
    if folder:
        return Path(folder).name
    return None


def _save_metadata(
    mister_game_folder: str | None,
    retroarch_game_file: str | None,
    retroarch_inference: dict | None = None,
) -> dict | None:
    metadata = {}
    if mister_game_folder:
        metadata["mister_game_folder"] = str(mister_game_folder)
    if retroarch_game_file:
        metadata["retroarch_game_file"] = str(retroarch_game_file)
        metadata["retroarch_game_file_stem"] = retroarch_game_file_stem(retroarch_game_file)
    if retroarch_inference:
        metadata["retroarch_game_file_inference"] = {
            "strategy": retroarch_inference["strategy"],
            "candidates": retroarch_inference["candidates"],
        }
    return metadata or None


def _infer_retroarch_game_file(
    system: str,
    direction: str,
    mister_game_folder: str | None,
    retroarch_game_file: str | None,
) -> dict | None:
    if retroarch_game_file or system != "psx" or direction != "mister-to-thor":
        return None
    if not mister_game_folder:
        return None
    return infer_psx_retroarch_game_file(Path(mister_game_folder))


def _store_from_args(args: argparse.Namespace, config: dict) -> LocalObjectStore | S3ObjectStore:
    backend = getattr(args, "backend", "local")
    if backend == "local":
        if not args.root:
            raise ObjectStoreError("--root is required for the local store backend")
        return LocalObjectStore(Path(args.root))
    if backend == "s3":
        return S3ObjectStore.from_config(config)
    raise ObjectStoreError(f"unsupported store backend: {backend}")


if __name__ == "__main__":
    raise SystemExit(main())
