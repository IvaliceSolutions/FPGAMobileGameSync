from __future__ import annotations

import unittest

from fpgmobilegamesync.psx_auto_mapping import infer_psx_save_mappings


class PsxAutoMappingTests(unittest.TestCase):
    def test_infers_mapping_from_thor_save_matching_game_file_in_mister_folder(
        self,
    ) -> None:
        result = infer_psx_save_mappings(
            config={"save_mappings": {"psx": []}},
            manifests=[
                _manifest(
                    [
                        _item(
                            "mister",
                            "games",
                            "Final Fantasy 9 (FR)/Final Fantasy 9 (FR).cue",
                        ),
                        _item(
                            "thor",
                            "games",
                            "Final Fantasy 9 (FR)/Final Fantasy IX (France) (Disc 2).chd",
                        ),
                        _item(
                            "thor",
                            "saves",
                            "Final Fantasy IX (France) (Disc 2).srm",
                            modified_ns=3,
                        ),
                    ]
                )
            ],
        )

        self.assertEqual(
            result["mappings"],
            [
                {
                    "mister_game_folder": "Final Fantasy 9 (FR)",
                    "retroarch_game_file_stem": "Final Fantasy IX (France) (Disc 2)",
                    "inferred_from": "Final Fantasy IX (France) (Disc 2).srm",
                }
            ],
        )

    def test_keeps_explicit_mapping_instead_of_inferred_mapping(self) -> None:
        result = infer_psx_save_mappings(
            config={
                "save_mappings": {
                    "psx": [
                        {
                            "mister_game_folder": "Final Fantasy 9 (FR)",
                            "retroarch_game_file_stem": "Final Fantasy IX (France) (Disc 1)",
                        }
                    ]
                }
            },
            manifests=[
                _manifest(
                    [
                        _item(
                            "mister",
                            "games",
                            "Final Fantasy 9 (FR)/Final Fantasy 9 (FR).cue",
                        ),
                        _item(
                            "thor",
                            "games",
                            "Final Fantasy 9 (FR)/Final Fantasy IX (France) (Disc 2).chd",
                        ),
                        _item(
                            "thor",
                            "saves",
                            "Final Fantasy IX (France) (Disc 2).srm",
                        ),
                    ]
                )
            ],
        )

        self.assertEqual(result["mappings"], [])
        self.assertEqual(result["skipped"][0]["reason"], "explicit_mapping_exists")

    def test_chooses_newest_save_when_multiple_discs_exist(self) -> None:
        result = infer_psx_save_mappings(
            config={"save_mappings": {"psx": []}},
            manifests=[
                _manifest(
                    [
                        _item(
                            "mister",
                            "games",
                            "Final Fantasy 9 (FR)/Final Fantasy 9 (FR).cue",
                        ),
                        _item(
                            "thor",
                            "games",
                            "Final Fantasy 9 (FR)/Final Fantasy IX (France) (Disc 1).chd",
                        ),
                        _item(
                            "thor",
                            "games",
                            "Final Fantasy 9 (FR)/Final Fantasy IX (France) (Disc 2).chd",
                        ),
                        _item(
                            "thor",
                            "saves",
                            "Final Fantasy IX (France) (Disc 1).srm",
                            modified_ns=1,
                        ),
                        _item(
                            "thor",
                            "saves",
                            "Final Fantasy IX (France) (Disc 2).srm",
                            modified_ns=2,
                        ),
                    ]
                )
            ],
        )

        self.assertEqual(
            result["mappings"][0]["retroarch_game_file_stem"],
            "Final Fantasy IX (France) (Disc 2)",
        )
        self.assertEqual(
            result["skipped"][0]["reason"],
            "multiple_thor_saves_for_folder_chose_newest",
        )


def _manifest(items: list[dict]) -> dict:
    return {"items": items}


def _item(
    device: str,
    content_type: str,
    content_path: str,
    modified_ns: int = 1,
) -> dict:
    return {
        "device": device,
        "system": "psx",
        "type": content_type,
        "absolute_path": f"/tmp/{content_path}",
        "relative_path": content_path,
        "content_path": content_path,
        "native_content_path": content_path,
        "modified_ns": modified_ns,
    }


if __name__ == "__main__":
    unittest.main()
