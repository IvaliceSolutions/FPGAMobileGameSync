from __future__ import annotations

import unittest

from fpgmobilegamesync.save_paths import (
    canonical_save_content_path,
    native_save_content_path,
)


class SavePathTests(unittest.TestCase):
    def test_raw_save_paths_use_store_canonical_extension(self) -> None:
        config = _config()

        self.assertEqual(
            canonical_save_content_path(config, "gba", "thor", "Golden Sun.srm"),
            "Golden Sun.sav",
        )
        self.assertEqual(
            native_save_content_path(config, "gba", "thor", "Golden Sun.sav"),
            "Golden Sun.srm",
        )
        self.assertEqual(
            native_save_content_path(config, "gba", "mister", "Golden Sun.sav"),
            "Golden Sun.sav",
        )

    def test_psx_mapping_links_mister_folder_to_retroarch_stem(self) -> None:
        config = _config()
        config["systems"]["psx"] = {
            "save_conversion": {
                "strategy": "psx_raw_memory_card",
                "mister_to_thor": {"output_extension": ".srm"},
                "thor_to_mister": {"output_extension": ".sav"},
            }
        }
        config["save_mappings"] = {
            "psx": [
                {
                    "mister_game_folder": "Final Fantasy 9 (FR)",
                    "retroarch_game_file": "Final Fantasy IX.chd",
                }
            ]
        }

        self.assertEqual(
            canonical_save_content_path(config, "psx", "thor", "Final Fantasy IX.srm"),
            "Final Fantasy 9 (FR).sav",
        )
        self.assertEqual(
            native_save_content_path(config, "psx", "thor", "Final Fantasy 9 (FR).sav"),
            "Final Fantasy IX.srm",
        )


def _config() -> dict:
    return {
        "devices": {"mister": {}, "thor": {}},
        "systems": {
            "gba": {
                "save_conversion": {
                    "strategy": "raw_same_content",
                    "mister_to_thor": {"rename_extension_to": ".srm"},
                    "thor_to_mister": {"rename_extension_to": ".sav"},
                }
            }
        },
    }


if __name__ == "__main__":
    unittest.main()
