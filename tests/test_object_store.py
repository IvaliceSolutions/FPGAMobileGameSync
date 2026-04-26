from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fpgmobilegamesync.object_store import LocalObjectStore


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


if __name__ == "__main__":
    unittest.main()
