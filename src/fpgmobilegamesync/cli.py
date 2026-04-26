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
    retroarch_game_file_stem,
)
from .executor import (
    ApplyError,
    apply_plan_to_local_store,
    apply_plan_to_local_target,
    load_plan,
)
from .object_store import LocalObjectStore, ObjectStoreError
from .planner import PlanError, build_plan
from .scanner import ScanError, scan


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
        help="Scan local object-store contents.",
    )
    store_scan_parser.add_argument(
        "--root",
        required=True,
        help="Object-store root directory.",
    )
    store_scan_parser.add_argument(
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
        choices=("local",),
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

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "scan":
            config = load_config(Path(args.config))
            manifest = scan(
                config=config,
                device=args.device,
                systems=args.system,
                types=args.type,
            )
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
            manifest = LocalObjectStore(Path(args.root)).scan()
            json.dump(
                manifest,
                sys.stdout,
                indent=2 if args.pretty else None,
                sort_keys=True,
            )
            sys.stdout.write("\n")
            return 0
        if args.command == "apply":
            config = load_config(Path(args.config))
            plan = load_plan(Path(args.plan))
            if plan.get("mode") == "download" or args.target_root:
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
            else:
                if not args.store_root:
                    raise ApplyError("--store-root is required for local object-store apply")
                result = apply_plan_to_local_store(
                    plan=plan,
                    store_root=Path(args.store_root),
                    timestamp_utc=args.timestamp_utc,
                    allow_conflicts=args.allow_conflicts,
                )
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
    except (
        ApplyError,
        CompareError,
        ConfigError,
        ConversionError,
        ObjectStoreError,
        PlanError,
        ScanError,
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


if __name__ == "__main__":
    raise SystemExit(main())
