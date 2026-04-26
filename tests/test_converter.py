from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fpgmobilegamesync.config import load_config
from fpgmobilegamesync.converter import (
    ConversionError,
    convert_save_file,
    infer_psx_retroarch_game_file,
)
from fpgmobilegamesync.cli import (
    _infer_retroarch_game_file,
    _resolve_save_output_path,
    _save_metadata,
    _save_output_stem,
)


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

    def test_psx_infers_single_disc_game_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            game_folder = Path(tmp) / "Vagrant Story (France)"
            game_folder.mkdir()
            (game_folder / "Vagrant Story (France).chd").write_bytes(b"fake")

            result = infer_psx_retroarch_game_file(game_folder)

            self.assertEqual(result["strategy"], "single_disc")
            self.assertEqual(Path(result["path"]).name, "Vagrant Story (France).chd")

    def test_psx_infers_cd1_before_other_discs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            game_folder = Path(tmp) / "Final Fantasy 9 (FR)"
            game_folder.mkdir()
            (game_folder / "Final Fantasy IX CD 2.chd").write_bytes(b"fake")
            (game_folder / "Final Fantasy IX CD 1.chd").write_bytes(b"fake")

            result = infer_psx_retroarch_game_file(game_folder)

            self.assertEqual(result["strategy"], "cd_space_1")
            self.assertEqual(Path(result["path"]).name, "Final Fantasy IX CD 1.chd")

    def test_psx_disc_patterns_win_before_cd_patterns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            game_folder = Path(tmp) / "Legend of Dragoon"
            game_folder.mkdir()
            (game_folder / "Legend of Dragoon CD 1.chd").write_bytes(b"fake")
            (game_folder / "Legend of Dragoon Disc 1.chd").write_bytes(b"fake")

            result = infer_psx_retroarch_game_file(game_folder)

            self.assertEqual(result["strategy"], "disc_space_1")
            self.assertEqual(Path(result["path"]).name, "Legend of Dragoon Disc 1.chd")

    def test_psx_disc1_wins_before_one_of_and_cd_patterns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            game_folder = Path(tmp) / "Parasite Eve"
            game_folder.mkdir()
            (game_folder / "Parasite Eve 1 of 2.chd").write_bytes(b"fake")
            (game_folder / "Parasite Eve Disc1.chd").write_bytes(b"fake")

            result = infer_psx_retroarch_game_file(game_folder)

            self.assertEqual(result["strategy"], "disc1")
            self.assertEqual(Path(result["path"]).name, "Parasite Eve Disc1.chd")

    def test_psx_one_of_wins_before_cd_patterns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            game_folder = Path(tmp) / "Lunar"
            game_folder.mkdir()
            (game_folder / "Lunar CD 1.chd").write_bytes(b"fake")
            (game_folder / "Lunar 1 of 2.chd").write_bytes(b"fake")

            result = infer_psx_retroarch_game_file(game_folder)

            self.assertEqual(result["strategy"], "one_of")
            self.assertEqual(Path(result["path"]).name, "Lunar 1 of 2.chd")

    def test_psx_infers_cd1_without_space(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            game_folder = Path(tmp) / "Chrono Cross"
            game_folder.mkdir()
            (game_folder / "Chrono Cross CD2.chd").write_bytes(b"fake")
            (game_folder / "Chrono Cross CD1.chd").write_bytes(b"fake")

            result = infer_psx_retroarch_game_file(game_folder)

            self.assertEqual(result["strategy"], "cd1")
            self.assertEqual(Path(result["path"]).name, "Chrono Cross CD1.chd")

    def test_psx_falls_back_to_isolated_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            game_folder = Path(tmp) / "Arc the Lad Collection"
            game_folder.mkdir()
            (game_folder / "Arc the Lad 2.chd").write_bytes(b"fake")
            (game_folder / "Arc the Lad 1.chd").write_bytes(b"fake")

            result = infer_psx_retroarch_game_file(game_folder)

            self.assertEqual(result["strategy"], "isolated_1")
            self.assertEqual(Path(result["path"]).name, "Arc the Lad 1.chd")

    def test_psx_inferred_file_is_used_in_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            game_folder = Path(tmp) / "Final Fantasy 9 (FR)"
            game_folder.mkdir()
            (game_folder / "Final Fantasy IX CD 2.chd").write_bytes(b"fake")
            (game_folder / "Final Fantasy IX CD 1.chd").write_bytes(b"fake")

            inference = _infer_retroarch_game_file(
                system="psx",
                direction="mister-to-thor",
                mister_game_folder=str(game_folder),
                retroarch_game_file=None,
            )
            self.assertIsNotNone(inference)
            metadata = _save_metadata(
                mister_game_folder=str(game_folder),
                retroarch_game_file=inference["path"],
                retroarch_inference=inference,
            )

            self.assertEqual(metadata["retroarch_game_file_stem"], "Final Fantasy IX CD 1")
            self.assertEqual(
                metadata["retroarch_game_file_inference"]["strategy"],
                "cd_space_1",
            )


if __name__ == "__main__":
    unittest.main()
