"""Command line interface for FPGAMobileGameSync."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import ConfigError, load_config
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

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        config = load_config(Path(args.config))
        if args.command == "scan":
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
    except (ConfigError, ScanError) as exc:
        parser.exit(2, f"error: {exc}\n")

    parser.exit(2, "error: unknown command\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

