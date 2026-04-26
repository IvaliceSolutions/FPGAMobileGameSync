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


def _manifest(items: list[dict]) -> dict:
    return {
        "items": items,
    }


def _item(system: str, kind: str, path: str, sha256: str, size: int) -> dict:
    return {
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


if __name__ == "__main__":
    unittest.main()
