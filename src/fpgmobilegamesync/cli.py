"""Command line interface for FPGAMobileGameSync."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .compare import CompareError, compare_manifests, load_manifest
from .config import ConfigError, load_config
from .executor import ApplyError, apply_plan_to_local_store, load_plan
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
        required=True,
        help="Local object-store root directory.",
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
            result = apply_plan_to_local_store(
                plan=load_plan(Path(args.plan)),
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
    except (
        ApplyError,
        CompareError,
        ConfigError,
        ObjectStoreError,
        PlanError,
        ScanError,
    ) as exc:
        parser.exit(2, f"error: {exc}\n")

    parser.exit(2, "error: unknown command\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
