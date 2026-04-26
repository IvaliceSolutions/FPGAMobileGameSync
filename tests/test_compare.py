from __future__ import annotations

import unittest

from fpgmobilegamesync.compare import compare_manifests


class CompareTests(unittest.TestCase):
    def test_compare_detects_core_statuses(self) -> None:
        source = _manifest(
            [
                _item("gba", "games", "Same.gba", "aaa", 3),
                _item("gba", "games", "Changed.gba", "new", 3),
                _item("gba", "games", "New.gba", "add", 3),
                _item("snes", "games", "New Name.sfc", "ren", 3),
                _item("snes", "games", "Folder/Moved.sfc", "mov", 3),
                _item("psx", "games", "Game New/Disc 1.cue", "both", 4),
            ]
        )
        target = _manifest(
            [
                _item("gba", "games", "Same.gba", "aaa", 3),
                _item("gba", "games", "Changed.gba", "old", 3),
                _item("gba", "games", "Deleted.gba", "del", 3),
                _item("snes", "games", "Old Name.sfc", "ren", 3),
                _item("snes", "games", "Moved.sfc", "mov", 3),
                _item("psx", "games", "Game Old/Disc A.cue", "both", 4),
            ]
        )

        result = compare_manifests(source, target)

        self.assertEqual(
            result["summary"],
            {
                "added": 1,
                "deleted": 1,
                "modified": 1,
                "moved": 1,
                "renamed": 1,
                "renamed_moved": 1,
                "total": 7,
                "unchanged": 1,
            },
        )

    def test_compare_marks_duplicate_hash_matches_as_ambiguous(self) -> None:
        source = _manifest([_item("gba", "saves", "Save.sav", "same", 4)])
        target = _manifest(
            [
                _item("gba", "saves", "One.sav", "same", 4),
                _item("gba", "saves", "Two.sav", "same", 4),
            ]
        )

        result = compare_manifests(source, target)

        self.assertEqual(result["summary"]["ambiguous_rename"], 1)
        self.assertEqual(result["summary"]["deleted"], 2)

    def test_compare_detects_case_only_rename_with_changed_content(self) -> None:
        source = _manifest([_item("gba", "saves", "Pokemon.sav", "new", 4)])
        target = _manifest([_item("gba", "saves", "pokemon.sav", "old", 4)])

        result = compare_manifests(source, target)

        self.assertEqual(result["summary"]["modified_renamed"], 1)
        action = result["actions"][0]
        self.assertEqual(action["source"]["content_path"], "Pokemon.sav")
        self.assertEqual(action["target"]["content_path"], "pokemon.sav")

    def test_compare_reports_case_conflicts(self) -> None:
        source = _manifest([_item("gba", "saves", "Pokemon.sav", "new", 4)])
        target = _manifest(
            [
                _item("gba", "saves", "pokemon.sav", "old", 4),
                _item("gba", "saves", "POKEMON.sav", "older", 4),
            ]
        )

        result = compare_manifests(source, target)

        self.assertEqual(result["summary"]["case_conflict"], 1)


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
