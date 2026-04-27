from __future__ import annotations

import unittest

from fpgmobilegamesync.planner import build_plan


class PlannerTests(unittest.TestCase):
    def test_upload_plan_maps_changes_to_remote_operations(self) -> None:
        source = _manifest(
            [
                _item("gba", "games", "Same.gba", "aaa", 3),
                _item("gba", "games", "Changed.gba", "new", 3),
                _item("gba", "games", "New.gba", "add", 3),
                _item("snes", "games", "New Name.sfc", "ren", 3),
            ]
        )
        target = _manifest(
            [
                _item("gba", "games", "Same.gba", "aaa", 3),
                _item("gba", "games", "Changed.gba", "old", 3),
                _item("gba", "games", "Deleted.gba", "del", 3),
                _item("snes", "games", "Old Name.sfc", "ren", 3),
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
        source = _manifest([_item("gba", "games", "New Name.gba", "same", 4)])
        target = _manifest([_item("gba", "games", "Old Name.gba", "same", 4)])

        plan = build_plan(source, target, mode="download", source_name="s3", target_name="thor")

        self.assertEqual(plan["summary"]["rename_local"], 1)
        action = plan["actions"][0]
        self.assertEqual(action["from_content_path"], "Old Name.gba")
        self.assertEqual(action["to_content_path"], "New Name.gba")
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
