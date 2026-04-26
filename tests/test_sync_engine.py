from __future__ import annotations

import json
import io
import posixpath
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from fpgmobilegamesync.s3_store import S3ObjectStore
from fpgmobilegamesync.sftp_client import RemoteDirEntry, RemoteStat, SftpError
from fpgmobilegamesync.sync_engine import run_local_sync, run_s3_sync


class SyncEngineTests(unittest.TestCase):
    def test_apply_mister_to_thor_save_sync_through_local_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mister_root = root / "mister"
            thor_root = root / "thor"
            store_root = root / "store"
            report_dir = root / "reports" / "run-1"
            (mister_root / "saves/GBA").mkdir(parents=True)
            (thor_root / "RetroArch/saves/GBA").mkdir(parents=True)
            (mister_root / "saves/GBA/Golden Sun.sav").write_bytes(b"save-data")

            result = run_local_sync(
                config=_config(mister_root, thor_root),
                direction="mister-to-thor",
                store_root=store_root,
                systems=["gba"],
                types=["saves"],
                apply=True,
                timestamp_utc="2026-04-26T20-30-00Z",
                report_dir=report_dir,
            )

            self.assertFalse(result["dry_run"])
            self.assertEqual(result["upload_plan"]["summary"]["upload"], 1)
            self.assertEqual(result["download_plan"]["summary"]["download"], 1)
            self.assertEqual(
                (store_root / "systems/gba/saves/Golden Sun.sav").read_bytes(),
                b"save-data",
            )
            self.assertEqual(
                (thor_root / "RetroArch/saves/GBA/Golden Sun.srm").read_bytes(),
                b"save-data",
            )
            self.assertTrue((report_dir / "source-manifest.json").exists())
            self.assertTrue((report_dir / "upload-plan.json").exists())
            self.assertTrue((report_dir / "upload-apply.json").exists())
            self.assertTrue((report_dir / "download-plan.json").exists())
            self.assertTrue((report_dir / "download-apply.json").exists())
            summary = json.loads((report_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["direction"], "mister-to-thor")
            self.assertEqual(summary["upload_plan_summary"]["upload"], 1)
            self.assertEqual(summary["download_plan_summary"]["download"], 1)
            self.assertIn(str(report_dir / "summary.json"), result["report_files"])

    def test_cli_sync_dry_run_reports_both_plans(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mister_root = root / "mister"
            thor_root = root / "thor"
            store_root = root / "store"
            (mister_root / "saves/GBA").mkdir(parents=True)
            (thor_root / "RetroArch/saves/GBA").mkdir(parents=True)
            (mister_root / "saves/GBA/Advance Wars.sav").write_bytes(b"save")
            config_path = root / "config.json"
            config_path.write_text(
                json.dumps(_config(mister_root, thor_root)),
                encoding="utf-8",
            )

            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "fpgmobilegamesync.cli",
                    "--config",
                    str(config_path),
                    "sync",
                    "--direction",
                    "mister-to-thor",
                    "--backend",
                    "local",
                    "--store-root",
                    str(store_root),
                    "--report-dir",
                    str(root / "reports" / "dry-run"),
                    "--system",
                    "gba",
                    "--type",
                    "saves",
                ],
                check=True,
                cwd=Path.cwd(),
                text=True,
                capture_output=True,
            )

            result = json.loads(completed.stdout)
            self.assertTrue(result["dry_run"])
            self.assertEqual(result["upload_plan"]["summary"]["upload"], 1)
            self.assertEqual(result["download_plan"]["summary"]["total"], 0)
            self.assertFalse((store_root / "systems/gba/saves/Advance Wars.sav").exists())
            self.assertTrue((root / "reports/dry-run/upload-plan.json").exists())
            self.assertFalse((root / "reports/dry-run/upload-apply.json").exists())

    def test_apply_mister_to_thor_save_sync_through_s3_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mister_root = root / "mister"
            thor_root = root / "thor"
            report_dir = root / "reports" / "s3-run"
            (mister_root / "saves/GBA").mkdir(parents=True)
            (thor_root / "RetroArch/saves/GBA").mkdir(parents=True)
            (mister_root / "saves/GBA/Golden Sun.sav").write_bytes(b"save-data")
            client = FakeS3Client({})
            store = S3ObjectStore(client=client, bucket="bucket", prefix="fp")

            result = run_s3_sync(
                config=_config(mister_root, thor_root),
                direction="mister-to-thor",
                systems=["gba"],
                types=["saves"],
                apply=True,
                timestamp_utc="2026-04-26T21-30-00Z",
                report_dir=report_dir,
                store=store,
            )

            self.assertFalse(result["dry_run"])
            self.assertEqual(result["backend"], "s3")
            self.assertEqual(result["lock"]["owner"], "mister-to-thor")
            self.assertEqual(result["lock_release"]["status"], "released")
            self.assertEqual(result["upload_plan"]["summary"]["upload"], 1)
            self.assertEqual(result["download_plan"]["summary"]["download"], 1)
            self.assertEqual(
                client.objects["fp/systems/gba/saves/Golden Sun.sav"],
                b"save-data",
            )
            self.assertIn("fp/manifests/s3.json", client.objects)
            self.assertNotIn("fp/locks/sync.json", client.objects)
            self.assertEqual(
                (thor_root / "RetroArch/saves/GBA/Golden Sun.srm").read_bytes(),
                b"save-data",
            )
            summary = json.loads((report_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["backend"], "s3")
            self.assertEqual(summary["store"]["prefix"], "fp")
            self.assertEqual(summary["lock_release"]["status"], "released")

    def test_apply_mister_to_thor_save_sync_through_s3_with_sftp_devices(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "reports" / "sftp-run"
            mister_client = FakeRemoteClient(
                {
                    "/media/fat/saves/GBA/Golden Sun.sav": b"save-data",
                }
            )
            thor_client = FakeRemoteClient({})
            s3_client = FakeS3Client({})
            store = S3ObjectStore(client=s3_client, bucket="bucket", prefix="fp")

            result = run_s3_sync(
                config=_remote_config(),
                direction="mister-to-thor",
                systems=["gba"],
                types=["saves"],
                apply=True,
                timestamp_utc="2026-04-26T22-30-00Z",
                report_dir=report_dir,
                store=store,
                scan_backend="sftp",
                sftp_clients={
                    "mister": mister_client,
                    "thor": thor_client,
                },
            )

            self.assertFalse(result["dry_run"])
            self.assertEqual(result["scan_backend"], "sftp")
            self.assertEqual(result["upload_plan"]["summary"]["upload"], 1)
            self.assertEqual(result["download_plan"]["summary"]["download"], 1)
            self.assertEqual(
                s3_client.objects["fp/systems/gba/saves/Golden Sun.sav"],
                b"save-data",
            )
            self.assertEqual(
                thor_client.files["/storage/emulated/0/RetroArch/saves/GBA/Golden Sun.srm"],
                b"save-data",
            )
            summary = json.loads((report_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["scan_backend"], "sftp")

    def test_sftp_sync_applies_remote_backup_rename_and_trash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "reports" / "sftp-actions"
            mister_client = FakeRemoteClient(
                {
                    "/media/fat/saves/GBA/Changed.sav": b"new-save!",
                    "/media/fat/saves/GBA/Renamed.sav": b"same-save",
                }
            )
            thor_client = FakeRemoteClient(
                {
                    "/storage/emulated/0/RetroArch/saves/GBA/Changed.srm": b"old-save!",
                    "/storage/emulated/0/RetroArch/saves/GBA/OldName.srm": b"same-save",
                    "/storage/emulated/0/RetroArch/saves/GBA/Deleted.srm": b"deleted",
                }
            )
            store = S3ObjectStore(client=FakeS3Client({}), bucket="bucket")

            result = run_s3_sync(
                config=_remote_config(),
                direction="mister-to-thor",
                systems=["gba"],
                types=["saves"],
                apply=True,
                timestamp_utc="2026-04-26T23-30-00Z",
                report_dir=report_dir,
                store=store,
                scan_backend="sftp",
                sftp_clients={
                    "mister": mister_client,
                    "thor": thor_client,
                },
            )

            self.assertEqual(result["download_plan"]["summary"]["download"], 1)
            self.assertEqual(result["download_plan"]["summary"]["rename_local"], 1)
            self.assertEqual(result["download_plan"]["summary"]["trash_local"], 1)
            self.assertEqual(
                thor_client.files["/storage/emulated/0/RetroArch/saves/GBA/Changed.srm"],
                b"new-save!",
            )
            self.assertEqual(
                thor_client.files[
                    "/storage/emulated/0/RetroArch/.sync_trash/backups/"
                    "2026-04-26T23-30-00Z/s3/Changed.srm"
                ],
                b"old-save!",
            )
            self.assertNotIn(
                "/storage/emulated/0/RetroArch/saves/GBA/OldName.srm",
                thor_client.files,
            )
            self.assertEqual(
                thor_client.files["/storage/emulated/0/RetroArch/saves/GBA/Renamed.srm"],
                b"same-save",
            )
            self.assertNotIn(
                "/storage/emulated/0/RetroArch/saves/GBA/Deleted.srm",
                thor_client.files,
            )
            self.assertEqual(
                thor_client.files[
                    "/storage/emulated/0/RetroArch/.sync_trash/deleted/"
                    "2026-04-26T23-30-00Z/s3/Deleted.srm"
                ],
                b"deleted",
            )


def _config(mister_root: Path, thor_root: Path) -> dict:
    return {
        "defaults": {
            "systems": ["gba"],
            "types": ["saves"],
        },
        "devices": {
            "mister": {
                "local": {
                    "root": str(mister_root),
                    "trash": str(mister_root / ".sync_trash"),
                }
            },
            "thor": {
                "local": {
                    "root": str(thor_root),
                    "trash": str(thor_root / "RetroArch/.sync_trash"),
                }
            },
        },
        "sync_modes": {
            "mister-to-thor": {
                "source": "mister",
                "target": "thor",
            },
            "thor-to-mister": {
                "source": "thor",
                "target": "mister",
            },
        },
        "exclusions": {
            "global": {
                "directories": [],
                "filename_patterns": [],
            }
        },
        "systems": {
            "gba": {
                "paths": {
                    "mister": {
                        "saves": "saves/GBA",
                    },
                    "thor": {
                        "saves": "RetroArch/saves/GBA",
                    },
                },
                "file_extensions": {
                    "saves": {
                        "mister": [".sav"],
                        "thor": [".srm"],
                    }
                },
                "save_conversion": {
                    "strategy": "raw_same_content",
                    "mister_to_thor": {
                        "rename_extension_to": ".srm",
                        "validate_sizes": [4, 9],
                    },
                    "thor_to_mister": {
                        "rename_extension_to": ".sav",
                        "validate_sizes": [4, 9],
                    },
                },
            }
        },
    }


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
        return {
            "Contents": [
                {
                    "Key": key,
                    "Size": len(value),
                    "ETag": f'"etag-{key}"',
                }
                for key, value in sorted(self.objects.items())
                if key.startswith(Prefix)
            ],
            "IsTruncated": False,
        }

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


class FakeRemoteClient:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = {_normalize(path): data for path, data in files.items()}

    def stat(self, path: str) -> RemoteStat:
        path = _normalize(path)
        if path in self.files:
            return RemoteStat(
                size=len(self.files[path]),
                modified_ns=1,
                is_file=True,
                is_dir=False,
            )
        if self._is_dir(path):
            return RemoteStat(size=0, modified_ns=1, is_file=False, is_dir=True)
        raise SftpError(f"missing: {path}")

    def listdir(self, path: str) -> list[RemoteDirEntry]:
        path = _normalize(path)
        if not self._is_dir(path):
            raise SftpError(f"missing: {path}")
        names = set()
        prefix = path.rstrip("/") + "/"
        for file_path in self.files:
            if file_path.startswith(prefix):
                remainder = file_path[len(prefix) :]
                names.add(remainder.split("/", 1)[0])
        return [
            RemoteDirEntry(name=name, stat=self.stat(posixpath.join(path, name)))
            for name in sorted(names)
        ]

    def read_file(self, path: str) -> bytes:
        path = _normalize(path)
        if path not in self.files:
            raise SftpError(f"missing: {path}")
        return self.files[path]

    def write_file(self, path: str, data: bytes) -> None:
        self.files[_normalize(path)] = data

    def exists(self, path: str) -> bool:
        try:
            self.stat(path)
            return True
        except SftpError:
            return False

    def rename(self, old_path: str, new_path: str) -> None:
        old_path = _normalize(old_path)
        new_path = _normalize(new_path)
        if old_path not in self.files:
            raise SftpError(f"missing: {old_path}")
        self.files[new_path] = self.files.pop(old_path)

    def close(self) -> None:
        return None

    def _is_dir(self, path: str) -> bool:
        prefix = path.rstrip("/") + "/"
        return any(file_path.startswith(prefix) for file_path in self.files)


def _remote_config() -> dict:
    config = _config(Path("/unused/mister"), Path("/unused/thor"))
    config["devices"]["mister"]["remote"] = {
        "protocol": "sftp",
        "host": "mister.local",
        "port": 22,
        "root": "/media/fat",
        "trash": "/media/fat/.sync_trash",
    }
    config["devices"]["thor"]["remote"] = {
        "protocol": "sftp",
        "host": "thor.local",
        "port": 2222,
        "root": "/storage/emulated/0",
        "trash": "/storage/emulated/0/RetroArch/.sync_trash",
    }
    return config


def _normalize(path: str) -> str:
    return posixpath.normpath(path)


if __name__ == "__main__":
    unittest.main()
