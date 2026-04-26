from __future__ import annotations

import hashlib
import posixpath
import unittest

from fpgmobilegamesync.remote_scanner import scan_remote
from fpgmobilegamesync.sftp_client import RemoteDirEntry, RemoteStat, SftpError


class RemoteScannerTests(unittest.TestCase):
    def test_scan_remote_hashes_matching_files(self) -> None:
        client = FakeRemoteClient(
            {
                "/media/fat/games/GBA/Game.gba": b"rom",
                "/media/fat/games/GBA/Ignored.txt": b"nope",
                "/media/fat/saves/GBA/Game.sav": b"save",
                "/media/fat/saves/GBA/states/Ignored.sav": b"state",
                "/media/fat/saves/GBA/Temp.tmp": b"tmp",
            }
        )

        manifest = scan_remote(
            config=_config("/media/fat"),
            device="mister",
            systems=["gba"],
            types=["games", "saves"],
            client=client,
        )

        relative_paths = {item["relative_path"] for item in manifest["items"]}
        self.assertEqual(relative_paths, {"games/GBA/Game.gba", "saves/GBA/Game.sav"})
        self.assertEqual(manifest["backend"], "sftp")
        self.assertEqual(manifest["summary"]["item_count"], 2)
        save_item = next(item for item in manifest["items"] if item["content_path"] == "Game.sav")
        self.assertEqual(save_item["sha256"], hashlib.sha256(b"save").hexdigest())

    def test_scan_remote_normalizes_thor_save_name(self) -> None:
        client = FakeRemoteClient(
            {
                "/storage/emulated/0/RetroArch/saves/GBA/Golden Sun.srm": b"save",
            }
        )
        config = _config("/storage/emulated/0")
        config["devices"] = {
            "thor": {
                "remote": {"root": "/storage/emulated/0"},
                "local": {"root": "/storage/emulated/0"},
            }
        }
        config["systems"]["gba"]["paths"]["thor"] = {
            "games": "RetroArch/games/GBA",
            "saves": "RetroArch/saves/GBA",
            "bios": [],
            "thumbnails": None,
        }
        config["systems"]["gba"]["file_extensions"]["saves"]["thor"] = [".srm"]
        config["systems"]["gba"]["save_conversion"] = {
            "strategy": "raw_same_content",
            "mister_to_thor": {"rename_extension_to": ".srm"},
            "thor_to_mister": {"rename_extension_to": ".sav"},
        }

        manifest = scan_remote(
            config=config,
            device="thor",
            systems=["gba"],
            types=["saves"],
            client=client,
        )
        item = manifest["items"][0]

        self.assertEqual(item["native_content_path"], "Golden Sun.srm")
        self.assertEqual(item["content_path"], "Golden Sun.sav")
        self.assertEqual(item["sync_key"], "systems/gba/saves/Golden Sun.sav")

    def test_scan_remote_skips_missing_paths(self) -> None:
        manifest = scan_remote(
            config=_config("/media/fat"),
            device="mister",
            systems=["gba"],
            types=["saves"],
            client=FakeRemoteClient({}),
        )

        self.assertEqual(manifest["summary"]["item_count"], 0)
        self.assertEqual(manifest["skipped"][0]["reason"], "missing")


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

    def close(self) -> None:
        return None

    def _is_dir(self, path: str) -> bool:
        prefix = path.rstrip("/") + "/"
        return any(file_path.startswith(prefix) for file_path in self.files)


def _config(root: str) -> dict:
    return {
        "defaults": {
            "systems": ["gba"],
            "types": ["games", "saves", "bios", "thumbnails"],
        },
        "devices": {
            "mister": {
                "remote": {"root": root},
                "local": {"root": root},
            }
        },
        "exclusions": {
            "global": {
                "directories": ["states"],
                "filename_patterns": ["*.tmp"],
            }
        },
        "systems": {
            "gba": {
                "paths": {
                    "mister": {
                        "games": "games/GBA",
                        "saves": "saves/GBA",
                        "bios": [],
                        "thumbnails": None,
                    }
                },
                "file_extensions": {
                    "games": [".gba"],
                    "saves": {
                        "mister": [".sav"],
                    },
                    "bios": [".rom"],
                    "thumbnails": [".png"],
                },
            }
        },
    }


def _normalize(path: str) -> str:
    return posixpath.normpath(path)


if __name__ == "__main__":
    unittest.main()
