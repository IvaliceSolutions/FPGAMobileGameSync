from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fpgmobilegamesync.executor import (
    ApplyError,
    apply_plan_to_local_store,
    apply_plan_to_local_target,
)


class ExecutorTests(unittest.TestCase):
    def test_apply_upload_plan_to_local_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store_root = root / "store"
            source_root = root / "source"
            source_root.mkdir()

            new_file = source_root / "New.gba"
            changed_file = source_root / "Changed.gba"
            renamed_file = source_root / "Renamed.gba"
            new_file.write_bytes(b"new")
            changed_file.write_bytes(b"changed")
            renamed_file.write_bytes(b"renamed")

            (store_root / "systems/gba/games").mkdir(parents=True)
            (store_root / "systems/gba/games/Changed.gba").write_bytes(b"old")
            (store_root / "systems/gba/games/OldName.gba").write_bytes(b"renamed")
            (store_root / "systems/gba/games/Deleted.gba").write_bytes(b"deleted")

            plan = {
                "source": "mister",
                "target": "s3",
                "actions": [
                    {"operation": "noop", "reason": "unchanged"},
                    {
                        "operation": "upload",
                        "reason": "added",
                        "source": _item(new_file, "New.gba"),
                    },
                    {
                        "operation": "upload",
                        "reason": "modified",
                        "backup_target_before_apply": True,
                        "source": _item(changed_file, "Changed.gba"),
                        "target": _item(
                            store_root / "systems/gba/games/Changed.gba",
                            "Changed.gba",
                        ),
                    },
                    {
                        "operation": "rename_remote",
                        "reason": "renamed",
                        "source": _item(renamed_file, "Renamed.gba"),
                        "target": _item(
                            store_root / "systems/gba/games/OldName.gba",
                            "OldName.gba",
                        ),
                    },
                    {
                        "operation": "trash_remote",
                        "reason": "missing_from_source_after_rename_detection",
                        "target": _item(
                            store_root / "systems/gba/games/Deleted.gba",
                            "Deleted.gba",
                        ),
                    },
                ],
            }

            result = apply_plan_to_local_store(
                plan,
                store_root=store_root,
                timestamp_utc="2026-04-26T20-30-00Z",
            )

            self.assertTrue((store_root / "systems/gba/games/New.gba").exists())
            self.assertEqual(
                (store_root / "systems/gba/games/Changed.gba").read_bytes(),
                b"changed",
            )
            self.assertTrue(
                (
                    store_root
                    / "backups/2026-04-26T20-30-00Z/mister/systems/gba/games/Changed.gba"
                ).exists()
            )
            self.assertFalse((store_root / "systems/gba/games/OldName.gba").exists())
            self.assertTrue((store_root / "systems/gba/games/Renamed.gba").exists())
            self.assertFalse((store_root / "systems/gba/games/Deleted.gba").exists())
            self.assertTrue(
                (
                    store_root
                    / "trash/2026-04-26T20-30-00Z/mister/systems/gba/games/Deleted.gba"
                ).exists()
            )
            self.assertTrue((store_root / "manifests/s3.json").exists())
            self.assertEqual(result["summary"]["upload:applied"], 2)
            self.assertEqual(result["summary"]["noop:skipped"], 1)

    def test_apply_refuses_conflicts_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            plan = {
                "actions": [
                    {
                        "operation": "conflict",
                        "reason": "ambiguous_rename",
                    }
                ]
            }

            with self.assertRaises(ApplyError):
                apply_plan_to_local_store(plan, store_root=Path(tmp))

    def test_apply_download_plan_to_local_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store_root = root / "store"
            target_root = root / "target"
            trash_root = root / "trash"
            store_games = store_root / "systems/gba/games"
            store_games.mkdir(parents=True)
            target_root.mkdir()

            new_file = store_games / "New.gba"
            changed_file = store_games / "Changed.gba"
            renamed_file = store_games / "Renamed.gba"
            new_file.write_bytes(b"new")
            changed_file.write_bytes(b"changed")
            renamed_file.write_bytes(b"renamed")

            (target_root / "Changed.gba").write_bytes(b"old")
            (target_root / "OldName.gba").write_bytes(b"renamed")
            (target_root / "Deleted.gba").write_bytes(b"deleted")

            plan = {
                "mode": "download",
                "source": "s3",
                "target": "thor",
                "actions": [
                    {"operation": "noop", "reason": "unchanged"},
                    {
                        "operation": "download",
                        "reason": "added",
                        "source": _item(new_file, "New.gba"),
                    },
                    {
                        "operation": "download",
                        "reason": "modified",
                        "backup_target_before_apply": True,
                        "source": _item(changed_file, "Changed.gba"),
                        "target": _item(target_root / "Changed.gba", "Changed.gba"),
                    },
                    {
                        "operation": "rename_local",
                        "reason": "renamed",
                        "source": _item(renamed_file, "Renamed.gba"),
                        "target": _item(target_root / "OldName.gba", "OldName.gba"),
                    },
                    {
                        "operation": "trash_local",
                        "reason": "missing_from_source_after_rename_detection",
                        "target": _item(target_root / "Deleted.gba", "Deleted.gba"),
                    },
                ],
            }

            result = apply_plan_to_local_target(
                plan,
                target_root=target_root,
                trash_root=trash_root,
                timestamp_utc="2026-04-26T21-00-00Z",
            )

            self.assertTrue((target_root / "New.gba").exists())
            self.assertEqual((target_root / "Changed.gba").read_bytes(), b"changed")
            self.assertTrue(
                (
                    trash_root
                    / "backups/2026-04-26T21-00-00Z/s3/Changed.gba"
                ).exists()
            )
            self.assertFalse((target_root / "OldName.gba").exists())
            self.assertTrue((target_root / "Renamed.gba").exists())
            self.assertFalse((target_root / "Deleted.gba").exists())
            self.assertTrue(
                (
                    trash_root
                    / "deleted/2026-04-26T21-00-00Z/s3/Deleted.gba"
                ).exists()
            )
            self.assertEqual(result["summary"]["download:applied"], 2)
            self.assertEqual(result["summary"]["rename_local:applied"], 1)
            self.assertEqual(result["summary"]["trash_local:applied"], 1)


def _item(path: Path, content_path: str) -> dict:
    return {
        "device": "test",
        "system": "gba",
        "type": "games",
        "absolute_path": str(path),
        "relative_path": content_path,
        "content_path": content_path,
        "sync_key": f"systems/gba/games/{content_path}",
        "size": path.stat().st_size if path.exists() else 0,
        "modified_ns": 1,
        "sha256": "test",
    }


if __name__ == "__main__":
    unittest.main()
