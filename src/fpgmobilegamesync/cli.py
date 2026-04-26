"""Command line interface for FPGAMobileGameSync."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .compare import CompareError, compare_manifests, load_manifest
from .config import ConfigError, load_config
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
    except (CompareError, ConfigError, PlanError, ScanError) as exc:
        parser.exit(2, f"error: {exc}\n")

    parser.exit(2, "error: unknown command\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
