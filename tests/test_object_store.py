from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from fpgmobilegamesync.object_store import LocalObjectStore, ObjectStoreError


class LocalObjectStoreTests(unittest.TestCase):
    def test_put_rename_trash_and_scan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "store"
            source = Path(tmp) / "source.gba"
            source.write_bytes(b"rom")
            store = LocalObjectStore(root)

            store.put_file(source, "systems/gba/games/Old.gba")
            self.assertTrue((root / "systems/gba/games/Old.gba").exists())

            manifest = store.scan()
            self.assertEqual(manifest["summary"]["item_count"], 1)
            self.assertEqual(manifest["items"][0]["content_path"], "Old.gba")

            store.rename_object(
                "systems/gba/games/Old.gba",
                "systems/gba/games/New.gba",
            )
            self.assertFalse((root / "systems/gba/games/Old.gba").exists())
            self.assertTrue((root / "systems/gba/games/New.gba").exists())

            trash_key = store.trash_object(
                "systems/gba/games/New.gba",
                origin_device="mister",
                timestamp_utc="2026-04-26T20-00-00Z",
            )
            self.assertEqual(
                trash_key,
                "trash/2026-04-26T20-00-00Z/mister/systems/gba/games/New.gba",
            )
            self.assertTrue((root / trash_key).exists())
            self.assertEqual(store.scan()["summary"]["item_count"], 0)

    def test_write_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "store"
            store = LocalObjectStore(root)

            store.write_manifest({"items": []})

            self.assertTrue((root / "manifests/s3.json").exists())

    def test_list_and_restore_trash_object(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "store"
            source = Path(tmp) / "source.sav"
            source.write_bytes(b"save")
            store = LocalObjectStore(root)
            store.put_file(source, "systems/gba/saves/Game.sav")
            trash_key = store.trash_object(
                "systems/gba/saves/Game.sav",
                origin_device="thor",
                timestamp_utc="2026-04-26T22-00-00Z",
            )

            trash = store.list_trash()
            self.assertEqual(trash["summary"]["item_count"], 1)
            self.assertEqual(trash["items"][0]["trash_key"], trash_key)
            self.assertEqual(trash["items"][0]["original_sync_key"], "systems/gba/saves/Game.sav")
            self.assertEqual(trash["items"][0]["origin_device"], "thor")
            self.assertEqual(trash["items"][0]["trashed_at_utc"], "2026-04-26T22-00-00Z")

            result = store.restore_trash_object(trash_key)

            self.assertEqual(result["status"], "restored")
            self.assertEqual(result["restored_sync_key"], "systems/gba/saves/Game.sav")
            self.assertTrue((root / "systems/gba/saves/Game.sav").exists())
            self.assertFalse((root / trash_key).exists())
            self.assertEqual(store.list_trash()["summary"]["item_count"], 0)

    def test_restore_refuses_to_overwrite_existing_object_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "store"
            source = Path(tmp) / "source.sav"
            source.write_bytes(b"old")
            replacement = Path(tmp) / "replacement.sav"
            replacement.write_bytes(b"new")
            store = LocalObjectStore(root)
            store.put_file(source, "systems/gba/saves/Game.sav")
            trash_key = store.trash_object(
                "systems/gba/saves/Game.sav",
                origin_device="mister",
                timestamp_utc="2026-04-26T22-15-00Z",
            )
            store.put_file(replacement, "systems/gba/saves/Game.sav")

            with self.assertRaises(ObjectStoreError):
                store.restore_trash_object(trash_key)

            result = store.restore_trash_object(trash_key, overwrite=True)
            self.assertEqual(result["status"], "restored")
            self.assertIn("backup_key", result)
            self.assertEqual((root / "systems/gba/saves/Game.sav").read_bytes(), b"old")
            self.assertEqual((root / result["backup_key"]).read_bytes(), b"new")

    def test_cli_lists_and_restores_trash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "store"
            source = Path(tmp) / "source.gba"
            source.write_bytes(b"rom")
            store = LocalObjectStore(root)
            store.put_file(source, "systems/gba/games/Game.gba")
            trash_key = store.trash_object(
                "systems/gba/games/Game.gba",
                origin_device="mister",
                timestamp_utc="2026-04-26T22-30-00Z",
            )

            listed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "fpgmobilegamesync.cli",
                    "store",
                    "trash",
                    "list",
                    "--root",
                    str(root),
                ],
                check=True,
                cwd=Path.cwd(),
                text=True,
                capture_output=True,
            )
            listed_json = json.loads(listed.stdout)
            self.assertEqual(listed_json["items"][0]["trash_key"], trash_key)

            restored = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "fpgmobilegamesync.cli",
                    "store",
                    "trash",
                    "restore",
                    "--root",
                    str(root),
                    "--trash-key",
                    trash_key,
                ],
                check=True,
                cwd=Path.cwd(),
                text=True,
                capture_output=True,
            )
            restored_json = json.loads(restored.stdout)
            self.assertEqual(restored_json["restored_sync_key"], "systems/gba/games/Game.gba")
            self.assertTrue((root / "systems/gba/games/Game.gba").exists())


if __name__ == "__main__":
    unittest.main()
