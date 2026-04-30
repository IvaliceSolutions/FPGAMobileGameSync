from __future__ import annotations

import unittest

from fpgmobilegamesync.planner import build_plan


class PlannerTests(unittest.TestCase):
    def test_upload_plan_maps_changes_to_remote_operations(self) -> None:
        source = _manifest(
            [
                _item("gba", "games", "Same.gba", "aaa", 3),
                _item("gba", "games", "Changed.gba", "new", 4),
                _item("gba", "games", "New.gba", "add", 3),
                _item("gba", "saves", "New Name.sav", "ren", 3),
            ]
        )
        target = _manifest(
            [
                _item("gba", "games", "Same.gba", "aaa", 3),
                _item("gba", "games", "Changed.gba", "old", 3),
                _item("gba", "games", "Deleted.gba", "del", 3),
                _item("gba", "saves", "Old Name.sav", "ren", 3),
            ]
        )

        plan = build_plan(source, target, mode="upload", source_name="mister", target_name="s3")

        self.assertEqual(
            plan["summary"],
            {
                "noop": 1,
                "rename_remote": 1,
                "trash_remote": 1,
                "total": 5,
                "upload": 2,
            },
        )
        trash_actions = [action for action in plan["actions"] if action["operation"] == "trash_remote"]
        self.assertFalse(trash_actions[0]["hard_delete"])

    def test_download_plan_maps_changes_to_local_operations(self) -> None:
        source = _manifest([_item("gba", "saves", "New Name.sav", "same", 4)])
        target = _manifest([_item("gba", "saves", "Old Name.sav", "same", 4)])

        plan = build_plan(source, target, mode="download", source_name="s3", target_name="thor")

        self.assertEqual(plan["summary"]["rename_local"], 1)
        action = plan["actions"][0]
        self.assertEqual(action["from_content_path"], "Old Name.sav")
        self.assertEqual(action["to_content_path"], "New Name.sav")
        self.assertFalse(action["copy_delete_required"])

    def test_download_plan_renames_unchanged_save_with_wrong_native_path(self) -> None:
        source = _manifest([_item("psx", "saves", "Lunar.sav", "same", 131072)])
        target = _manifest(
            [
                _item(
                    "psx",
                    "saves",
                    "Lunar.sav",
                    "same",
                    131072,
                    native_content_path="Lunar.srm",
                )
            ]
        )

        plan = build_plan(
            source,
            target,
            mode="download",
            source_name="s3",
            target_name="thor",
            config=_psx_mapping_config(),
            target_device="thor",
        )

        self.assertEqual(plan["summary"]["rename_local"], 1)
        action = plan["actions"][0]
        self.assertEqual(action["reason"], "native_path_mismatch")
        self.assertEqual(action["from_content_path"], "Lunar.srm")
        self.assertEqual(action["to_content_path"], "Lunar_fr_cd1.srm")

    def test_download_plan_renames_modified_save_before_copy_when_native_path_is_wrong(
        self,
    ) -> None:
        source = _manifest([_item("psx", "saves", "Lunar.sav", "new", 131072)])
        target = _manifest(
            [
                _item(
                    "psx",
                    "saves",
                    "Lunar.sav",
                    "old",
                    131072,
                    native_content_path="Lunar.srm",
                )
            ]
        )

        plan = build_plan(
            source,
            target,
            mode="download",
            source_name="s3",
            target_name="thor",
            config=_psx_mapping_config(),
            target_device="thor",
        )

        self.assertEqual(plan["summary"]["download"], 1)
        action = plan["actions"][0]
        self.assertEqual(action["reason"], "modified_native_path_mismatch")
        self.assertEqual(action["from_content_path"], "Lunar.srm")
        self.assertEqual(action["to_content_path"], "Lunar_fr_cd1.srm")
        self.assertTrue(action["rename_target_before_copy"])

    def test_download_plan_skips_canonical_rename_when_native_save_path_is_already_correct(
        self,
    ) -> None:
        source = _manifest([_item("psx", "saves", "Lunar_fr_cd1.sav", "same", 131072)])
        target = _manifest(
            [
                _item(
                    "psx",
                    "saves",
                    "Lunar.sav",
                    "same",
                    131072,
                    native_content_path="Lunar_fr_cd1.srm",
                )
            ]
        )

        plan = build_plan(
            source,
            target,
            mode="download",
            source_name="s3",
            target_name="thor",
            config=_psx_mapping_config(),
            target_device="thor",
        )

        self.assertEqual(plan["summary"]["noop"], 1)
        action = plan["actions"][0]
        self.assertEqual(action["reason"], "unchanged_native_path")

    def test_download_plan_does_not_rename_convertible_save_by_hash_only(self) -> None:
        source = _manifest([_item("psx", "saves", "Valkyrie.sav", "empty-card", 131072)])
        target = _manifest([_item("psx", "saves", "Xenogears.sav", "empty-card", 131072)])

        plan = build_plan(
            source,
            target,
            mode="download",
            source_name="s3",
            target_name="mister",
            config=_psx_mapping_config(),
            target_device="mister",
        )

        self.assertEqual(plan["summary"]["download"], 1)
        self.assertEqual(plan["actions"][0]["operation"], "download")
        self.assertEqual(plan["actions"][0]["reason"], "added")

    def test_download_plan_conflicts_when_two_saves_write_same_native_path(self) -> None:
        config = _psx_mapping_config()
        config["save_mappings"]["psx"] = [
            {
                "mister_game_folder": "Final Fantasy VII (FR)",
                "retroarch_game_file_stem": "Final Fantasy VII (France) (Disc 1)",
            }
        ]
        source = _manifest(
            [
                _item("psx", "saves", "Final Fantasy VII (FR).sav", "a", 131072),
                _item(
                    "psx",
                    "saves",
                    "Final Fantasy VII (France) (Disc 1).sav",
                    "b",
                    131072,
                ),
            ]
        )
        target = _manifest([])

        plan = build_plan(
            source,
            target,
            mode="download",
            source_name="s3",
            target_name="thor",
            config=config,
            target_device="thor",
        )

        self.assertEqual(plan["summary"]["conflict"], 2)
        self.assertEqual(
            {action["native_content_path"] for action in plan["actions"]},
            {"Final Fantasy VII (France) (Disc 1).srm"},
        )
        self.assertEqual(
            {action["source"]["content_path"] for action in plan["actions"]},
            {
                "Final Fantasy VII (FR).sav",
                "Final Fantasy VII (France) (Disc 1).sav",
            },
        )

    def test_plan_conflicts_when_empty_psx_card_would_overwrite_nonempty_save(
        self,
    ) -> None:
        source = _manifest(
            [
                _item(
                    "psx",
                    "saves",
                    "Final Fantasy 9 (FR).sav",
                    "0a36c94a2a96926ecd1855ab7de34841fe446d18ae0bb8b993f340a3cde02058",
                    131072,
                )
            ]
        )
        target = _manifest(
            [
                _item(
                    "psx",
                    "saves",
                    "Final Fantasy 9 (FR).sav",
                    "d5cf592ed07765438d6a41ec4e51c0fd4cdf92ca2d2b61ff55ee8bcde223fd33",
                    131072,
                )
            ]
        )

        plan = build_plan(source, target, mode="upload")

        self.assertEqual(plan["summary"]["conflict"], 1)
        self.assertEqual(
            plan["actions"][0]["reason"],
            "empty_psx_source_would_overwrite_save",
        )

    def test_plan_does_not_treat_nonempty_native_psx_card_as_empty(self) -> None:
        source = _manifest(
            [
                _item(
                    "psx",
                    "saves",
                    "Lunar.sav",
                    "0a36c94a2a96926ecd1855ab7de34841fe446d18ae0bb8b993f340a3cde02058",
                    131072,
                    native_sha256="3eb75d0be15aa48327f9ed0b8bba8cc3e38f1d5921163cfde5f1eb1a58cf8636",
                )
            ]
        )
        target = _manifest(
            [
                _item(
                    "psx",
                    "saves",
                    "Lunar.sav",
                    "3eb75d0be15aa48327f9ed0b8bba8cc3e38f1d5921163cfde5f1eb1a58cf8636",
                    131072,
                )
            ]
        )

        plan = build_plan(source, target, mode="upload")

        self.assertEqual(plan["summary"]["upload"], 1)
        self.assertEqual(plan["actions"][0]["operation"], "upload")

    def test_plan_conflicts_when_nonempty_psx_target_would_be_deleted(self) -> None:
        source = _manifest([])
        target = _manifest(
            [
                _item(
                    "psx",
                    "saves",
                    "Final Fantasy IX (France) (Disc 2).sav",
                    "d5cf592ed07765438d6a41ec4e51c0fd4cdf92ca2d2b61ff55ee8bcde223fd33",
                    131072,
                )
            ]
        )

        plan = build_plan(source, target, mode="download")

        self.assertEqual(plan["summary"]["conflict"], 1)
        self.assertEqual(
            plan["actions"][0]["reason"],
            "nonempty_psx_target_would_be_deleted",
        )

    def test_ambiguous_rename_becomes_conflict(self) -> None:
        source = _manifest([_item("gba", "saves", "Save.sav", "same", 4)])
        target = _manifest(
            [
                _item("gba", "saves", "One.sav", "same", 4),
                _item("gba", "saves", "Two.sav", "same", 4),
            ]
        )

        plan = build_plan(source, target, mode="upload")

        self.assertEqual(plan["summary"]["conflict"], 1)
        self.assertEqual(plan["summary"]["trash_remote"], 2)

    def test_modified_case_rename_is_copied_after_target_rename(self) -> None:
        source = _manifest([_item("gba", "saves", "Pokemon.sav", "new", 4)])
        target = _manifest([_item("gba", "saves", "pokemon.sav", "old", 4)])

        plan = build_plan(source, target, mode="upload", source_name="mister", target_name="s3")

        self.assertEqual(plan["summary"]["upload"], 1)
        action = plan["actions"][0]
        self.assertEqual(action["reason"], "modified_renamed")
        self.assertEqual(action["from_content_path"], "pokemon.sav")
        self.assertEqual(action["to_content_path"], "Pokemon.sav")
        self.assertTrue(action["backup_target_before_apply"])
        self.assertTrue(action["rename_target_before_copy"])

    def test_case_conflict_becomes_conflict(self) -> None:
        source = _manifest([_item("gba", "saves", "Pokemon.sav", "new", 4)])
        target = _manifest(
            [
                _item("gba", "saves", "pokemon.sav", "old", 4),
                _item("gba", "saves", "POKEMON.sav", "older", 4),
            ]
        )

        plan = build_plan(source, target, mode="download")

        self.assertEqual(plan["summary"]["conflict"], 1)


def _manifest(items: list[dict]) -> dict:
    return {
        "items": items,
    }


def _item(
    system: str,
    kind: str,
    path: str,
    sha256: str,
    size: int,
    native_content_path: str | None = None,
    native_sha256: str | None = None,
) -> dict:
    item = {
        "device": "test",
        "system": system,
        "type": kind,
        "absolute_path": f"/tmp/{path}",
        "relative_path": path,
        "content_path": path,
        "sync_key": f"systems/{system}/{kind}/{path}",
        "size": size,
        "modified_ns": 1,
        "sha256": sha256,
    }
    if native_content_path is not None:
        item["native_content_path"] = native_content_path
    if native_sha256 is not None:
        item["native_sha256"] = native_sha256
    return item


def _psx_mapping_config() -> dict:
    return {
        "devices": {"mister": {}, "thor": {}},
        "systems": {
            "psx": {
                "save_conversion": {
                    "strategy": "psx_raw_memory_card",
                    "mister_to_thor": {"output_extension": ".srm"},
                    "thor_to_mister": {"output_extension": ".sav"},
                }
            }
        },
        "save_mappings": {
            "psx": [
                {
                    "mister_game_folder": "Lunar",
                    "retroarch_game_file_stem": "Lunar_fr_cd1",
                }
            ]
        },
    }


if __name__ == "__main__":
    unittest.main()
