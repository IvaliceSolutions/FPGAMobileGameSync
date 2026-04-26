from __future__ import annotations

import io
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fpgmobilegamesync.executor import ApplyError, apply_plan_to_s3_store
from fpgmobilegamesync.object_store import ObjectStoreError
from fpgmobilegamesync.s3_store import S3ObjectStore, s3_connection_from_config


class S3ObjectStoreTests(unittest.TestCase):
    def test_scan_reads_manifest_from_configured_prefix(self) -> None:
        client = FakeS3Client(
            {
                "fp/manifests/s3.json": json.dumps(
                    {
                        "device": "s3",
                        "items": [{"sync_key": "systems/gba/saves/Game.sav"}],
                        "summary": {"item_count": 1},
                    }
                ).encode("utf-8")
            }
        )
        store = S3ObjectStore(client=client, bucket="bucket", prefix="fp")

        manifest = store.scan()

        self.assertEqual(manifest["backend"], "s3")
        self.assertEqual(manifest["manifest_key"], "manifests/s3.json")
        self.assertEqual(manifest["items"][0]["sync_key"], "systems/gba/saves/Game.sav")

    def test_scan_live_hashes_current_system_objects(self) -> None:
        client = FakeS3Client(
            {
                "systems/gba/saves/Game.sav": b"save",
                "trash/2026/mister/systems/gba/saves/Old.sav": b"old",
            }
        )
        store = S3ObjectStore(client=client, bucket="bucket")

        manifest = store.scan_live()

        self.assertEqual(manifest["summary"]["item_count"], 1)
        self.assertEqual(manifest["items"][0]["sync_key"], "systems/gba/saves/Game.sav")
        self.assertEqual(
            manifest["items"][0]["sha256"],
            hashlib.sha256(b"save").hexdigest(),
        )

    def test_scan_returns_empty_manifest_when_manifest_is_missing(self) -> None:
        store = S3ObjectStore(client=FakeS3Client({}), bucket="bucket")

        manifest = store.scan()

        self.assertEqual(manifest["summary"]["item_count"], 0)
        self.assertEqual(manifest["skipped"][0]["reason"], "missing_manifest")

    def test_list_and_restore_trash_object(self) -> None:
        trash_key = "trash/2026-04-26T22-00-00Z/mister/systems/gba/saves/Game.sav"
        client = FakeS3Client({f"root/{trash_key}": b"old-save"})
        store = S3ObjectStore(client=client, bucket="bucket", prefix="root")

        listed = store.list_trash()

        self.assertEqual(listed["summary"]["item_count"], 1)
        self.assertEqual(listed["items"][0]["trash_key"], trash_key)
        self.assertEqual(listed["items"][0]["original_sync_key"], "systems/gba/saves/Game.sav")

        result = store.restore_trash_object(trash_key)

        self.assertEqual(result["status"], "restored")
        self.assertEqual(client.objects["root/systems/gba/saves/Game.sav"], b"old-save")
        self.assertNotIn(f"root/{trash_key}", client.objects)

    def test_restore_refuses_then_backs_up_overwrite_target(self) -> None:
        trash_key = "trash/2026-04-26T22-00-00Z/mister/systems/gba/saves/Game.sav"
        client = FakeS3Client(
            {
                trash_key: b"old-save",
                "systems/gba/saves/Game.sav": b"new-save",
            }
        )
        store = S3ObjectStore(client=client, bucket="bucket")

        with self.assertRaises(ObjectStoreError):
            store.restore_trash_object(trash_key)

        result = store.restore_trash_object(trash_key, overwrite=True)

        self.assertEqual(client.objects["systems/gba/saves/Game.sav"], b"old-save")
        self.assertIn("backup_key", result)
        self.assertEqual(client.objects[result["backup_key"]], b"new-save")

    def test_connection_uses_environment_variables_from_config(self) -> None:
        config = {
            "s3": {
                "bucket": "bucket",
                "endpoint_url_env": "FPGMS_ENDPOINT",
                "access_key_id_env": "FPGMS_KEY",
                "secret_access_key_env": "FPGMS_SECRET",
                "region": "garage",
                "prefix": "fp",
            }
        }

        with patch.dict(
            os.environ,
            {
                "FPGMS_ENDPOINT": "https://garage.example",
                "FPGMS_KEY": "key",
                "FPGMS_SECRET": "secret",
            },
        ):
            connection = s3_connection_from_config(config)

        self.assertEqual(connection.bucket, "bucket")
        self.assertEqual(connection.endpoint_url, "https://garage.example")
        self.assertEqual(connection.prefix, "fp")

    def test_apply_upload_plan_to_s3_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_root = root / "source"
            source_root.mkdir()
            new_file = source_root / "New.gba"
            changed_file = source_root / "Changed.gba"
            renamed_file = source_root / "Renamed.gba"
            new_file.write_bytes(b"new")
            changed_file.write_bytes(b"changed")
            renamed_file.write_bytes(b"renamed")
            client = FakeS3Client(
                {
                    "systems/gba/games/Changed.gba": b"old",
                    "systems/gba/games/OldName.gba": b"renamed",
                    "systems/gba/games/Deleted.gba": b"deleted",
                }
            )
            store = S3ObjectStore(client=client, bucket="bucket")
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
                        "target": _s3_item("Changed.gba", b"old"),
                    },
                    {
                        "operation": "rename_remote",
                        "reason": "renamed",
                        "source": _item(renamed_file, "Renamed.gba"),
                        "target": _s3_item("OldName.gba", b"renamed"),
                    },
                    {
                        "operation": "trash_remote",
                        "reason": "missing_from_source_after_rename_detection",
                        "target": _s3_item("Deleted.gba", b"deleted"),
                    },
                ],
            }

            result = apply_plan_to_s3_store(
                plan=plan,
                config={},
                timestamp_utc="2026-04-26T23-00-00Z",
                store=store,
            )

            self.assertEqual(client.objects["systems/gba/games/New.gba"], b"new")
            self.assertEqual(client.objects["systems/gba/games/Changed.gba"], b"changed")
            self.assertEqual(client.objects["systems/gba/games/Renamed.gba"], b"renamed")
            self.assertNotIn("systems/gba/games/OldName.gba", client.objects)
            self.assertNotIn("systems/gba/games/Deleted.gba", client.objects)
            self.assertEqual(
                client.objects[
                    "backups/2026-04-26T23-00-00Z/mister/systems/gba/games/Changed.gba"
                ],
                b"old",
            )
            self.assertEqual(
                client.objects[
                    "trash/2026-04-26T23-00-00Z/mister/systems/gba/games/Deleted.gba"
                ],
                b"deleted",
            )
            self.assertIn("manifests/s3.json", client.objects)
            self.assertEqual(result["summary"]["upload:applied"], 2)
            self.assertEqual(result["summary"]["noop:skipped"], 1)

    def test_apply_refuses_when_s3_target_changed_since_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "Changed.gba"
            source.write_bytes(b"new")
            client = FakeS3Client({"systems/gba/games/Changed.gba": b"mutated"})
            store = S3ObjectStore(client=client, bucket="bucket")
            plan = {
                "source": "mister",
                "target": "s3",
                "actions": [
                    {
                        "operation": "upload",
                        "reason": "modified",
                        "source": _item(source, "Changed.gba"),
                        "target": _s3_item("Changed.gba", b"old"),
                    }
                ],
            }

            with self.assertRaises(ApplyError):
                apply_plan_to_s3_store(plan=plan, config={}, store=store)


class FakeS3Client:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = dict(objects)

    def get_object(self, Bucket: str, Key: str) -> dict:
        self._require(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket: str, Key: str, Body: bytes, **_kwargs: object) -> None:
        self.objects[Key] = Body

    def head_object(self, Bucket: str, Key: str) -> dict:
        self._require(Key)
        return {"ContentLength": len(self.objects[Key])}

    def copy_object(self, Bucket: str, CopySource: dict, Key: str) -> None:
        source_key = CopySource["Key"]
        self._require(source_key)
        self.objects[Key] = self.objects[source_key]

    def delete_object(self, Bucket: str, Key: str) -> None:
        self.objects.pop(Key, None)

    def list_objects_v2(self, Bucket: str, Prefix: str, **_kwargs: object) -> dict:
        contents = [
            {
                "Key": key,
                "Size": len(value),
                "ETag": f'"etag-{key}"',
            }
            for key, value in sorted(self.objects.items())
            if key.startswith(Prefix)
        ]
        return {"Contents": contents, "IsTruncated": False}

    def _require(self, key: str) -> None:
        if key not in self.objects:
            raise FakeNotFound(key)


class FakeNotFound(Exception):
    def __init__(self, key: str) -> None:
        super().__init__(key)
        self.response = {"Error": {"Code": "NoSuchKey"}}


def _item(path: Path, content_path: str, kind: str = "games", system: str = "gba") -> dict:
    data = path.read_bytes()
    sha256 = hashlib.sha256(data).hexdigest()
    return {
        "device": "test",
        "system": system,
        "type": kind,
        "absolute_path": str(path),
        "relative_path": content_path,
        "content_path": content_path,
        "sync_key": f"systems/{system}/{kind}/{content_path}",
        "size": len(data),
        "native_size": len(data),
        "canonical_size": len(data),
        "modified_ns": 1,
        "sha256": sha256,
        "native_sha256": sha256,
        "canonical_sha256": sha256,
    }


def _s3_item(content_path: str, data: bytes, kind: str = "games", system: str = "gba") -> dict:
    sha256 = hashlib.sha256(data).hexdigest()
    return {
        "device": "s3",
        "system": system,
        "type": kind,
        "absolute_path": f"systems/{system}/{kind}/{content_path}",
        "relative_path": f"systems/{system}/{kind}/{content_path}",
        "content_path": content_path,
        "native_content_path": content_path,
        "sync_key": f"systems/{system}/{kind}/{content_path}",
        "size": len(data),
        "native_size": len(data),
        "canonical_size": len(data),
        "modified_ns": 1,
        "sha256": sha256,
        "native_sha256": sha256,
        "canonical_sha256": sha256,
    }


if __name__ == "__main__":
    unittest.main()
