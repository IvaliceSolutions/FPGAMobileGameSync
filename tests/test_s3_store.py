from __future__ import annotations

import io
import json
import os
import unittest
from unittest.mock import patch

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


if __name__ == "__main__":
    unittest.main()
