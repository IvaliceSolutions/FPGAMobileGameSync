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
    apply_plan_from_s3_to_sftp_target,
    apply_plan_from_sftp_to_s3_store,
    apply_plan_to_s3_store,
)
from fpgmobilegamesync.object_store import ObjectStoreError
from fpgmobilegamesync.s3_store import S3ObjectStore, s3_connection_from_config
from fpgmobilegamesync.sftp_client import RemoteStat, SftpError


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
                "systems/gba/games/Game.gba": b"rom",
                "trash/2026/mister/systems/gba/saves/Old.sav": b"old",
            }
        )
        store = S3ObjectStore(client=client, bucket="bucket")

        manifest = store.scan_live()

        self.assertEqual(manifest["summary"]["item_count"], 2)
        save_item = next(item for item in manifest["items"] if item["type"] == "saves")
        game_item = next(item for item in manifest["items"] if item["type"] == "games")
        self.assertEqual(
            save_item["sha256"],
            hashlib.sha256(b"save").hexdigest(),
        )
        self.assertEqual(game_item["fingerprint_type"], "size")
        self.assertEqual(game_item["sha256"], "size:game.gba:3")
        self.assertNotIn("systems/gba/games/Game.gba", client.get_object_keys)

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

    def test_list_locks_redacts_token_and_marks_expired(self) -> None:
        client = FakeS3Client({})
        store = S3ObjectStore(client=client, bucket="bucket", prefix="fp")
        now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)
        store.acquire_lock(name="sync", owner="mister", ttl_seconds=60, now_utc=now)

        listed = store.list_locks(now_utc=now + timedelta(seconds=61))

        self.assertEqual(listed["summary"]["item_count"], 1)
        self.assertEqual(listed["summary"]["expired_count"], 1)
        self.assertEqual(listed["items"][0]["sync_key"], "locks/sync.json")
        self.assertEqual(listed["items"][0]["owner"], "mister")
        self.assertTrue(listed["items"][0]["expired"])
        self.assertNotIn("token", listed["items"][0])

    def test_clear_lock_refuses_active_lock_without_force(self) -> None:
        client = FakeS3Client({})
        store = S3ObjectStore(client=client, bucket="bucket")
        now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)
        store.acquire_lock(name="sync", owner="mister", ttl_seconds=60, now_utc=now)

        with self.assertRaises(ObjectStoreError):
            store.clear_lock(name="sync", now_utc=now + timedelta(seconds=10))

        self.assertIn("locks/sync.json", client.objects)

    def test_clear_lock_removes_expired_or_forced_lock(self) -> None:
        client = FakeS3Client({})
        store = S3ObjectStore(client=client, bucket="bucket")
        now = datetime(2026, 4, 26, 12, 0, tzinfo=timezone.utc)
        store.acquire_lock(name="sync", owner="mister", ttl_seconds=1, now_utc=now)

        expired_result = store.clear_lock(
            name="sync",
            now_utc=now + timedelta(seconds=2),
        )

        self.assertEqual(expired_result["status"], "cleared")
        self.assertFalse(expired_result["forced"])
        self.assertNotIn("locks/sync.json", client.objects)

        store.acquire_lock(name="sync", owner="thor", ttl_seconds=60, now_utc=now)
        forced_result = store.clear_lock(
            name="sync",
            force=True,
            now_utc=now + timedelta(seconds=10),
        )

        self.assertEqual(forced_result["status"], "cleared")
        self.assertTrue(forced_result["forced"])
        self.assertNotIn("locks/sync.json", client.objects)

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

    def test_apply_sftp_upload_skips_game_already_present_in_s3(self) -> None:
        client = FakeS3Client({"systems/gba/games/Game.gba": b"rom"})
        store = S3ObjectStore(client=client, bucket="bucket")
        source = _s3_item("Game.gba", b"rom")
        source["device"] = "mister"
        source["absolute_path"] = "/media/fat/games/GBA/Game.gba"
        source["fingerprint_type"] = "size"
        source["sha256"] = "size:game.gba:3"
        source["native_sha256"] = "size:game.gba:3"
        source["canonical_sha256"] = "size:game.gba:3"
        plan = {
            "source": "mister",
            "target": "s3",
            "actions": [
                {
                    "operation": "upload",
                    "reason": "modified",
                    "source": source,
                }
            ],
        }
        remote = FakeRemoteClient()

        result = apply_plan_from_sftp_to_s3_store(
            plan=plan,
            config={},
            client=remote,
            store=store,
        )

        self.assertEqual(result["summary"]["upload:skipped"], 1)
        self.assertEqual(result["applied"][0]["reason"], "already_uploaded")
        self.assertFalse(remote.read_attempted)

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

    def test_apply_s3_download_skips_game_already_present_on_sftp_target(self) -> None:
        client = FakeS3Client({"systems/gba/games/Game.gba": b"corrupt-on-read"})
        store = S3ObjectStore(client=client, bucket="bucket")
        source = _s3_item("Game.gba", b"rom")
        source["fingerprint_type"] = "size"
        source["sha256"] = "size:game.gba:3"
        source["native_sha256"] = "size:game.gba:3"
        source["canonical_sha256"] = "size:game.gba:3"
        plan = {
            "source": "s3",
            "target": "thor",
            "actions": [
                {
                    "operation": "download",
                    "reason": "modified",
                    "source": source,
                }
            ],
        }
        remote = FakeRemoteClient({"/target/Game.gba": b"rom"})

        result = apply_plan_from_s3_to_sftp_target(
            plan=plan,
            config={},
            client=remote,
            target_root="/target",
            store=store,
        )

        self.assertEqual(result["summary"]["download:skipped"], 1)
        self.assertEqual(result["applied"][0]["reason"], "already_downloaded")
        self.assertNotIn("systems/gba/games/Game.gba", client.get_object_keys)

    def test_apply_s3_download_replaces_partial_sftp_target_atomically(self) -> None:
        client = FakeS3Client({"systems/gba/games/Game.gba": b"complete-rom"})
        store = S3ObjectStore(client=client, bucket="bucket")
        source = _s3_item("Game.gba", b"complete-rom")
        target = _s3_item("Game.gba", b"part")
        target["absolute_path"] = "/target/Game.gba"
        plan = {
            "source": "s3",
            "target": "thor",
            "actions": [
                {
                    "operation": "download",
                    "reason": "modified",
                    "backup_target_before_apply": True,
                    "source": source,
                    "target": target,
                }
            ],
        }
        remote = FakeRemoteClient({"/target/Game.gba": b"part"})

        result = apply_plan_from_s3_to_sftp_target(
            plan=plan,
            config={},
            client=remote,
            target_root="/target",
            trash_root="/trash",
            timestamp_utc="2026-04-30T12-00-00Z",
            store=store,
        )

        self.assertEqual(result["summary"]["download:applied"], 1)
        self.assertEqual(remote.files["/target/Game.gba"], b"complete-rom")
        self.assertFalse(any(path.startswith("/trash/backups/") for path in remote.files))
        self.assertFalse(any(".fpgms-tmp" in path for path in remote.files))
        self.assertIn(("remove", "/target/Game.gba"), remote.operations)
        self.assertTrue(
            any(
                operation[0] == "rename" and operation[2] == "/target/Game.gba"
                for operation in remote.operations
            )
        )

    def test_apply_s3_download_resumes_staged_sftp_target(self) -> None:
        client = FakeS3Client({"systems/gba/games/Game.gba": b"complete-rom"})
        store = S3ObjectStore(client=client, bucket="bucket")
        source = _s3_item("Game.gba", b"complete-rom")
        plan = {
            "source": "s3",
            "target": "thor",
            "actions": [
                {
                    "operation": "download",
                    "reason": "added",
                    "source": source,
                }
            ],
        }
        remote = FakeRemoteClient({"/target/.Game.gba.fpgms-tmp": b"complete"})

        result = apply_plan_from_s3_to_sftp_target(
            plan=plan,
            config={},
            client=remote,
            target_root="/target",
            store=store,
        )

        self.assertEqual(result["summary"]["download:applied"], 1)
        self.assertEqual(result["applied"][0]["resumed_from_bytes"], 8)
        self.assertEqual(remote.files["/target/Game.gba"], b"complete-rom")
        self.assertEqual(client.get_object_ranges[-1], "bytes=8-")

    def test_apply_sftp_upload_resumes_multipart_upload(self) -> None:
        with patch("fpgmobilegamesync.executor.DEFAULT_MULTIPART_PART_SIZE", 4):
            client = FakeS3Client({})
            store = S3ObjectStore(client=client, bucket="bucket")
            source = _s3_item("Game.gba", b"complete-rom")
            source["device"] = "mister"
            source["absolute_path"] = "/source/Game.gba"
            source["fingerprint_type"] = "size"
            source["sha256"] = "size:game.gba:12"
            source["native_sha256"] = "size:game.gba:12"
            source["canonical_sha256"] = "size:game.gba:12"
            upload = client.create_multipart_upload(
                Bucket="bucket",
                Key="systems/gba/games/Game.gba",
                Metadata={"sha256": source["native_sha256"], "size": "12"},
            )
            client.upload_part(
                Bucket="bucket",
                Key="systems/gba/games/Game.gba",
                UploadId=upload["UploadId"],
                PartNumber=1,
                Body=b"comp",
            )
            state_key = store._multipart_state_key("systems/gba/games/Game.gba")
            client.put_object(
                Bucket="bucket",
                Key=state_key,
                Body=json.dumps(
                    {
                        "sync_key": "systems/gba/games/Game.gba",
                        "upload_id": upload["UploadId"],
                        "expected_size": 12,
                        "part_size": 4,
                    }
                ).encode("utf-8"),
            )
            plan = {
                "source": "mister",
                "target": "s3",
                "actions": [
                    {
                        "operation": "upload",
                        "reason": "added",
                        "source": source,
                    }
                ],
            }
            remote = FakeRemoteClient({"/source/Game.gba": b"complete-rom"})

            result = apply_plan_from_sftp_to_s3_store(
                plan=plan,
                config={},
                client=remote,
                store=store,
            )

        self.assertEqual(result["summary"]["upload:applied"], 1)
        self.assertEqual(result["applied"][0]["resumed_from_bytes"], 4)
        self.assertEqual(client.objects["systems/gba/games/Game.gba"], b"complete-rom")
        self.assertNotIn(state_key, client.objects)

    def test_failed_s3_download_write_keeps_existing_sftp_target(self) -> None:
        client = FakeS3Client({"systems/gba/games/Game.gba": b"complete-rom"})
        store = S3ObjectStore(client=client, bucket="bucket")
        source = _s3_item("Game.gba", b"complete-rom")
        target = _s3_item("Game.gba", b"part")
        target["absolute_path"] = "/target/Game.gba"
        plan = {
            "source": "s3",
            "target": "thor",
            "actions": [
                {
                    "operation": "download",
                    "reason": "modified",
                    "source": source,
                    "target": target,
                }
            ],
        }
        remote = FakeRemoteClient({"/target/Game.gba": b"part"})
        remote.fail_writes_matching = ".fpgms-tmp"

        with self.assertRaises(ApplyError):
            apply_plan_from_s3_to_sftp_target(
                plan=plan,
                config={},
                client=remote,
                target_root="/target",
                store=store,
            )

        self.assertEqual(remote.files["/target/Game.gba"], b"part")

    def test_apply_download_refuses_when_s3_source_changed_since_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target_root = Path(tmp) / "target"
            target_root.mkdir()
            client = FakeS3Client({"systems/gba/games/Game.gba": b"mutated-long"})
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
        self.get_object_keys: list[str] = []
        self.get_object_ranges: list[str | None] = []
        self.multipart_uploads: dict[str, dict[str, object]] = {}
        self.next_upload_id = 1

    def get_object(self, Bucket: str, Key: str, Range: str | None = None) -> dict:
        self._require(Key)
        self.get_object_keys.append(Key)
        self.get_object_ranges.append(Range)
        data = self.objects[Key]
        if Range:
            start_text = Range.removeprefix("bytes=").split("-", 1)[0]
            data = data[int(start_text) :]
        return {"Body": io.BytesIO(data), "ContentLength": len(data)}

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

    def create_multipart_upload(self, Bucket: str, Key: str, **_kwargs: object) -> dict:
        upload_id = f"upload-{self.next_upload_id}"
        self.next_upload_id += 1
        self.multipart_uploads[upload_id] = {"key": Key, "parts": {}}
        return {"UploadId": upload_id}

    def upload_part(
        self,
        Bucket: str,
        Key: str,
        UploadId: str,
        PartNumber: int,
        Body: bytes,
    ) -> dict:
        upload = self.multipart_uploads[UploadId]
        if upload["key"] != Key:
            raise FakeNotFound(Key)
        etag = f"etag-{UploadId}-{PartNumber}"
        upload["parts"][PartNumber] = {"data": Body, "ETag": etag}
        return {"ETag": etag}

    def list_parts(self, Bucket: str, Key: str, UploadId: str, **_kwargs: object) -> dict:
        upload = self.multipart_uploads[UploadId]
        if upload["key"] != Key:
            raise FakeNotFound(Key)
        return {
            "Parts": [
                {
                    "PartNumber": part_number,
                    "ETag": part["ETag"],
                    "Size": len(part["data"]),
                }
                for part_number, part in sorted(upload["parts"].items())
            ],
            "IsTruncated": False,
        }

    def complete_multipart_upload(
        self,
        Bucket: str,
        Key: str,
        UploadId: str,
        MultipartUpload: dict,
    ) -> None:
        upload = self.multipart_uploads.pop(UploadId)
        if upload["key"] != Key:
            raise FakeNotFound(Key)
        self.objects[Key] = b"".join(
            upload["parts"][part["PartNumber"]]["data"]
            for part in MultipartUpload["Parts"]
        )

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


class FakeRemoteClient:
    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        self.files = files or {}
        self.read_attempted = False
        self.operations: list[tuple[str, ...]] = []
        self.fail_writes_matching: str | None = None

    def stat(self, path: str) -> RemoteStat:
        if path not in self.files:
            raise SftpError(f"missing: {path}")
        return RemoteStat(
            size=len(self.files[path]),
            modified_ns=1,
            is_file=True,
            is_dir=False,
        )

    def exists(self, path: str) -> bool:
        return path in self.files

    def read_file(self, path: str, **_kwargs: object) -> bytes:
        self.read_attempted = True
        if path not in self.files:
            raise SftpError(f"missing: {path}")
        return self.files[path]

    def read_file_chunks(self, path: str, start: int = 0, chunk_size: int = 1024 * 1024) -> object:
        data = self.read_file(path)
        for offset in range(start, len(data), chunk_size):
            yield data[offset : offset + chunk_size]

    def write_file(self, path: str, data: bytes, **_kwargs: object) -> None:
        self.operations.append(("write", path))
        if self.fail_writes_matching and self.fail_writes_matching in path:
            self.files[path] = data[: max(0, len(data) // 2)]
            raise SftpError(f"simulated write failure: {path}")
        self.files[path] = data

    def append_file(self, path: str, data: bytes) -> None:
        self.operations.append(("append", path))
        if self.fail_writes_matching and self.fail_writes_matching in path:
            self.files[path] = self.files.get(path, b"") + data[: max(0, len(data) // 2)]
            raise SftpError(f"simulated append failure: {path}")
        self.files[path] = self.files.get(path, b"") + data

    def remove(self, path: str) -> None:
        self.operations.append(("remove", path))
        if path not in self.files:
            raise SftpError(f"missing: {path}")
        del self.files[path]

    def rename(self, old_path: str, new_path: str) -> None:
        self.operations.append(("rename", old_path, new_path))
        if old_path not in self.files:
            raise SftpError(f"missing: {old_path}")
        self.files[new_path] = self.files.pop(old_path)


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
