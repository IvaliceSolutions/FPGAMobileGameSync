from __future__ import annotations

import io
import hashlib
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from fpgmobilegamesync.executor import (
    ApplyError,
    apply_plan_from_s3_to_local_target,
    apply_plan_to_s3_store,
)
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

    def test_acquire_and_release_lock(self) -> None:
        client = FakeS3Client({})
        store = S3ObjectStore(client=client, bucket="bucket", prefix="fp")
        now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)

        lock = store.acquire_lock(
            name="sync",
            owner="thor",
            ttl_seconds=60,
            now_utc=now,
        )

        self.assertEqual(lock["owner"], "thor")
        self.assertEqual(lock["acquired_at_utc"], "2026-04-26T12:00:00Z")
        self.assertEqual(lock["expires_at_utc"], "2026-04-26T12:01:00Z")
        self.assertIn("token", lock)
        self.assertIn("fp/locks/sync.json", client.objects)

        result = store.release_lock(lock)

        self.assertEqual(result["status"], "released")
        self.assertNotIn("fp/locks/sync.json", client.objects)

    def test_acquire_lock_refuses_active_lock(self) -> None:
        client = FakeS3Client({})
        store = S3ObjectStore(client=client, bucket="bucket")
        now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)
        store.acquire_lock(name="sync", owner="mister", ttl_seconds=60, now_utc=now)

        with self.assertRaises(ObjectStoreError):
            store.acquire_lock(
                name="sync",
                owner="thor",
                ttl_seconds=60,
                now_utc=now + timedelta(seconds=10),
            )

    def test_acquire_lock_replaces_expired_lock(self) -> None:
        client = FakeS3Client({})
        store = S3ObjectStore(client=client, bucket="bucket")
        now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)
        first = store.acquire_lock(name="sync", owner="mister", ttl_seconds=1, now_utc=now)

        second = store.acquire_lock(
            name="sync",
            owner="thor",
            ttl_seconds=60,
            now_utc=now + timedelta(seconds=2),
        )

        self.assertNotEqual(first["token"], second["token"])
        self.assertEqual(second["owner"], "thor")

    def test_release_lock_refuses_token_mismatch(self) -> None:
        client = FakeS3Client({})
        store = S3ObjectStore(client=client, bucket="bucket")
        lock = store.acquire_lock(name="sync", owner="mister")
        wrong_lock = {**lock, "token": "wrong"}

        with self.assertRaises(ObjectStoreError):
            store.release_lock(wrong_lock)

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

    def test_apply_download_plan_from_s3_to_local_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target_root = root / "target"
            trash_root = root / "trash"
            target_root.mkdir()
            client = FakeS3Client(
                {
                    "systems/gba/games/New.gba": b"new",
                    "systems/gba/games/Changed.gba": b"changed",
                    "systems/gba/games/Renamed.gba": b"renamed",
                }
            )
            store = S3ObjectStore(client=client, bucket="bucket")
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
                        "source": _s3_item("New.gba", b"new"),
                    },
                    {
                        "operation": "download",
                        "reason": "modified",
                        "backup_target_before_apply": True,
                        "source": _s3_item("Changed.gba", b"changed"),
                        "target": _local_item(target_root / "Changed.gba", "Changed.gba"),
                    },
                    {
                        "operation": "rename_local",
                        "reason": "renamed",
                        "source": _s3_item("Renamed.gba", b"renamed"),
                        "target": _local_item(target_root / "OldName.gba", "OldName.gba"),
                    },
                    {
                        "operation": "trash_local",
                        "reason": "missing_from_source_after_rename_detection",
                        "target": _local_item(target_root / "Deleted.gba", "Deleted.gba"),
                    },
                ],
            }

            result = apply_plan_from_s3_to_local_target(
                plan=plan,
                config={},
                target_root=target_root,
                trash_root=trash_root,
                timestamp_utc="2026-04-26T23-30-00Z",
                store=store,
            )

            self.assertEqual((target_root / "New.gba").read_bytes(), b"new")
            self.assertEqual((target_root / "Changed.gba").read_bytes(), b"changed")
            self.assertEqual((target_root / "Renamed.gba").read_bytes(), b"renamed")
            self.assertFalse((target_root / "OldName.gba").exists())
            self.assertFalse((target_root / "Deleted.gba").exists())
            self.assertTrue(
                (trash_root / "backups/2026-04-26T23-30-00Z/s3/Changed.gba").exists()
            )
            self.assertTrue(
                (trash_root / "deleted/2026-04-26T23-30-00Z/s3/Deleted.gba").exists()
            )
            self.assertEqual(result["summary"]["download:applied"], 2)
            self.assertEqual(result["summary"]["rename_local:applied"], 1)

    def test_apply_download_refuses_when_s3_source_changed_since_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target_root = Path(tmp) / "target"
            target_root.mkdir()
            client = FakeS3Client({"systems/gba/games/Game.gba": b"mutated"})
            store = S3ObjectStore(client=client, bucket="bucket")
            plan = {
                "mode": "download",
                "source": "s3",
                "target": "thor",
                "actions": [
                    {
                        "operation": "download",
                        "reason": "modified",
                        "source": _s3_item("Game.gba", b"planned"),
                    }
                ],
            }

            with self.assertRaises(ApplyError):
                apply_plan_from_s3_to_local_target(
                    plan=plan,
                    config={},
                    target_root=target_root,
                    store=store,
                )


class FakeS3Client:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = dict(objects)

    def get_object(self, Bucket: str, Key: str) -> dict:
        self._require(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket: str, Key: str, Body: bytes, **kwargs: object) -> None:
        if kwargs.get("IfNoneMatch") == "*" and Key in self.objects:
            raise FakePreconditionFailed(Key)
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


class FakePreconditionFailed(Exception):
    def __init__(self, key: str) -> None:
        super().__init__(key)
        self.response = {"Error": {"Code": "PreconditionFailed"}}


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


def _local_item(path: Path, content_path: str, kind: str = "games", system: str = "gba") -> dict:
    return _item(path, content_path, kind=kind, system=system)


if __name__ == "__main__":
    unittest.main()
