from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fpgmobilegamesync.scanner import scan


class ScannerTests(unittest.TestCase):
    def test_scan_hashes_matching_files_and_skips_unconfigured_thumbnails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            game_dir = root / "games" / "GBA"
            save_dir = root / "saves" / "GBA"
            game_dir.mkdir(parents=True)
            save_dir.mkdir(parents=True)
            (game_dir / "Game.gba").write_bytes(b"rom")
            (game_dir / "Ignored.txt").write_text("nope", encoding="utf-8")
            (save_dir / "Game.sav").write_bytes(b"save")

            config = _config(root)
            manifest = scan(
                config=config,
                device="mister",
                systems=["gba"],
                types=["games", "saves", "thumbnails"],
            )

            relative_paths = {item["relative_path"] for item in manifest["items"]}
            self.assertEqual(relative_paths, {"games/GBA/Game.gba", "saves/GBA/Game.sav"})
            self.assertEqual(manifest["summary"]["item_count"], 2)
            self.assertEqual(manifest["summary"]["skipped_count"], 1)
            self.assertEqual(manifest["skipped"][0]["reason"], "not_configured")

    def test_scan_normalizes_thor_raw_save_extension_to_canonical_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            save_dir = root / "RetroArch" / "saves" / "mGBA"
            save_dir.mkdir(parents=True)
            (save_dir / "Golden Sun.srm").write_bytes(b"save")

            config = _config(root)
            config["devices"]["thor"] = {"local": {"root": str(root)}}
            config["systems"]["gba"]["paths"]["thor"] = {
                "games": "RetroArch/games/GBA",
                "saves": "RetroArch/saves/mGBA",
                "bios": [],
                "thumbnails": None,
            }
            config["systems"]["gba"]["file_extensions"]["saves"]["thor"] = [".srm"]
            config["systems"]["gba"]["save_conversion"] = {
                "strategy": "raw_same_content",
                "mister_to_thor": {"rename_extension_to": ".srm"},
                "thor_to_mister": {"rename_extension_to": ".sav"},
            }

            manifest = scan(config=config, device="thor", systems=["gba"], types=["saves"])
            item = manifest["items"][0]

            self.assertEqual(item["native_content_path"], "Golden Sun.srm")
            self.assertEqual(item["content_path"], "Golden Sun.sav")
            self.assertEqual(item["sync_key"], "systems/gba/saves/Golden Sun.sav")
            self.assertEqual(item["sha256"], item["canonical_sha256"])
            self.assertEqual(item["native_sha256"], item["canonical_sha256"])
            self.assertEqual(item["size"], item["canonical_size"])
            self.assertEqual(item["native_size"], item["canonical_size"])


def _config(root: Path) -> dict:
    return {
        "defaults": {
            "systems": ["gba"],
            "types": ["games", "saves", "bios", "thumbnails"],
        },
        "devices": {
            "mister": {
                "local": {
                    "root": str(root),
                }
            }
        },
        "exclusions": {
            "global": {
                "directories": ["states"],
                "filename_patterns": ["*.tmp"],
            }
        },
        "systems": {
            "gba": {
                "paths": {
                    "mister": {
                        "games": "games/GBA",
                        "saves": "saves/GBA",
                        "bios": [],
                        "thumbnails": None,
                    }
                },
                "file_extensions": {
                    "games": [".gba"],
                    "saves": {
                        "mister": [".sav"],
                    },
                    "bios": [".rom"],
                    "thumbnails": [".png"],
                },
            }
        },
    }


if __name__ == "__main__":
    unittest.main()
