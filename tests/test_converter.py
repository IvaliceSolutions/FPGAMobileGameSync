from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fpgmobilegamesync.config import load_config
from fpgmobilegamesync.converter import ConversionError, convert_save_file
from fpgmobilegamesync.cli import _resolve_save_output_path, _save_output_stem


class ConverterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config(Path("mister-thor-sync.json"))

    def test_gba_mister_save_converts_to_retroarch_srm_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "Golden Sun (FR).sav"
            output_dir = root / "out"
            output_dir.mkdir()
            source.write_bytes(b"x" * 65536)

            output = _resolve_save_output_path(
                self.config,
                "gba",
                "mister-to-thor",
                source,
                output_dir,
            )
            result = convert_save_file(self.config, "gba", "mister-to-thor", source, output)

            self.assertEqual(output.name, "Golden Sun (FR).srm")
            self.assertEqual(result["size"], 65536)
            self.assertEqual(output.read_bytes(), source.read_bytes())

    def test_snes_mister_sav_converts_to_retroarch_srm_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "Chrono Trigger.sav"
            output_dir = root / "out"
            output_dir.mkdir()
            source.write_bytes(b"x" * 8192)

            output = _resolve_save_output_path(
                self.config,
                "snes",
                "mister-to-thor",
                source,
                output_dir,
            )
            result = convert_save_file(self.config, "snes", "mister-to-thor", source, output)

            self.assertEqual(output.name, "Chrono Trigger.srm")
            self.assertEqual(result["size"], 8192)

    def test_psx_save_uses_retroarch_game_file_stem_for_thor_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "Final Fantasy 9 (FR).sav"
            output_dir = root / "out"
            mister_game_folder = root / "mister" / "Final Fantasy 9 (FR)"
            retroarch_game_file = root / "retroarch" / "Final Fantasy IX.chd"
            output_dir.mkdir()
            mister_game_folder.mkdir(parents=True)
            retroarch_game_file.parent.mkdir(parents=True)
            retroarch_game_file.write_bytes(b"fake chd")
            source.write_bytes(b"x" * 131072)

            output = _resolve_save_output_path(
                self.config,
                "psx",
                "mister-to-thor",
                source,
                output_dir,
                output_stem=_save_output_stem(
                    output_stem=None,
                    game_folder=None,
                    mister_game_folder=str(mister_game_folder),
                    retroarch_game_file=str(retroarch_game_file),
                    direction="mister-to-thor",
                ),
            )
            result = convert_save_file(
                self.config,
                "psx",
                "mister-to-thor",
                source,
                output,
                metadata={
                    "mister_game_folder": str(mister_game_folder),
                    "retroarch_game_file": str(retroarch_game_file),
                    "retroarch_game_file_stem": retroarch_game_file.stem,
                },
            )

            self.assertEqual(output.name, "Final Fantasy IX.srm")
            self.assertEqual(result["size"], 131072)
            self.assertEqual(
                result["metadata"]["mister_game_folder"],
                str(mister_game_folder),
            )
            self.assertEqual(
                result["metadata"]["retroarch_game_file_stem"],
                "Final Fantasy IX",
            )

    def test_psx_rejects_non_raw_memory_card_size(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "Bad.sav"
            output = Path(tmp) / "Bad.srm"
            source.write_bytes(b"x" * 8192)

            with self.assertRaises(ConversionError):
                convert_save_file(self.config, "psx", "mister-to-thor", source, output)


if __name__ == "__main__":
    unittest.main()
