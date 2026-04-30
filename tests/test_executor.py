from __future__ import annotations

import hashlib
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

    def test_apply_refuses_when_upload_source_changed_since_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store_root = root / "store"
            source_root = root / "source"
            source_root.mkdir()
            source_file = source_root / "Changed.gba"
            source_file.write_bytes(b"planned")
            source_item = _item(source_file, "Changed.gba")
            source_file.write_bytes(b"mutated-long")

            plan = {
                "source": "mister",
                "target": "s3",
                "actions": [
                    {
                        "operation": "upload",
                        "reason": "added",
                        "source": source_item,
                    }
                ],
            }

            with self.assertRaises(ApplyError):
                apply_plan_to_local_store(plan, store_root=store_root)

    def test_apply_modified_case_rename_to_local_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store_root = root / "store"
            source_root = root / "source"
            source_root.mkdir()
            source_file = source_root / "Pokemon.sav"
            source_file.write_bytes(b"new")

            (store_root / "systems/gba/saves").mkdir(parents=True)
            (store_root / "systems/gba/saves/pokemon.sav").write_bytes(b"old")

            plan = {
                "source": "mister",
                "target": "s3",
                "actions": [
                    {
                        "operation": "upload",
                        "reason": "modified_renamed",
                        "backup_target_before_apply": True,
                        "rename_target_before_copy": True,
                        "source": _item(source_file, "Pokemon.sav", kind="saves"),
                        "target": _item(
                            store_root / "systems/gba/saves/pokemon.sav",
                            "pokemon.sav",
                            kind="saves",
                        ),
                    }
                ],
            }

            apply_plan_to_local_store(
                plan,
                store_root=store_root,
                timestamp_utc="2026-04-26T21-30-00Z",
            )

            self.assertEqual(
                (store_root / "systems/gba/saves/Pokemon.sav").read_bytes(),
                b"new",
            )
            self.assertNotIn(
                "pokemon.sav",
                {path.name for path in (store_root / "systems/gba/saves").iterdir()},
            )
            self.assertTrue(
                (
                    store_root
                    / "backups/2026-04-26T21-30-00Z/mister/systems/gba/saves/pokemon.sav"
                ).exists()
            )

    def test_apply_refuses_when_remote_trash_target_changed_since_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store_root = root / "store"
            deleted_file = store_root / "systems/gba/games/Deleted.gba"
            deleted_file.parent.mkdir(parents=True)
            deleted_file.write_bytes(b"planned-delete")
            target_item = _item(deleted_file, "Deleted.gba")
            deleted_file.write_bytes(b"remote-change")

            plan = {
                "source": "mister",
                "target": "s3",
                "actions": [
                    {
                        "operation": "trash_remote",
                        "reason": "missing_from_source_after_rename_detection",
                        "target": target_item,
                    }
                ],
            }

            with self.assertRaises(ApplyError):
                apply_plan_to_local_store(plan, store_root=store_root)

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

    def test_apply_refuses_when_download_target_changed_since_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store_root = root / "store"
            target_root = root / "target"
            store_games = store_root / "systems/gba/games"
            store_games.mkdir(parents=True)
            target_root.mkdir()

            source_file = store_games / "Changed.gba"
            target_file = target_root / "Changed.gba"
            source_file.write_bytes(b"new")
            target_file.write_bytes(b"planned-old")
            target_item = _item(target_file, "Changed.gba")
            target_file.write_bytes(b"user-progress")

            plan = {
                "mode": "download",
                "source": "s3",
                "target": "thor",
                "actions": [
                    {
                        "operation": "download",
                        "reason": "modified",
                        "backup_target_before_apply": True,
                        "source": _item(source_file, "Changed.gba"),
                        "target": target_item,
                    }
                ],
            }

            with self.assertRaises(ApplyError):
                apply_plan_to_local_target(plan, target_root=target_root)

    def test_apply_modified_case_rename_to_local_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store_root = root / "store"
            target_root = root / "target"
            trash_root = root / "trash"
            store_saves = store_root / "systems/gba/saves"
            store_saves.mkdir(parents=True)
            target_root.mkdir()

            source_file = store_saves / "Pokemon.sav"
            source_file.write_bytes(b"new")
            (target_root / "pokemon.sav").write_bytes(b"old")

            plan = {
                "mode": "download",
                "source": "s3",
                "target": "thor",
                "actions": [
                    {
                        "operation": "download",
                        "reason": "modified_renamed",
                        "backup_target_before_apply": True,
                        "rename_target_before_copy": True,
                        "source": _item(source_file, "Pokemon.sav", kind="saves"),
                        "target": _item(target_root / "pokemon.sav", "pokemon.sav", kind="saves"),
                    }
                ],
            }

            apply_plan_to_local_target(
                plan,
                target_root=target_root,
                trash_root=trash_root,
                timestamp_utc="2026-04-26T21-45-00Z",
            )

            self.assertEqual((target_root / "Pokemon.sav").read_bytes(), b"new")
            self.assertNotIn("pokemon.sav", {path.name for path in target_root.iterdir()})
            self.assertTrue(
                (
                    trash_root
                    / "backups/2026-04-26T21-45-00Z/s3/pokemon.sav"
                ).exists()
            )

    def test_download_save_uses_target_native_extension_from_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store_root = root / "store"
            target_root = root / "target"
            store_saves = store_root / "systems/gba/saves"
            store_saves.mkdir(parents=True)
            target_root.mkdir()

            source_file = store_saves / "Golden Sun.sav"
            source_file.write_bytes(b"save")
            plan = {
                "mode": "download",
                "source": "s3",
                "target": "thor",
                "actions": [
                    {
                        "operation": "download",
                        "reason": "added",
                        "source": _item(source_file, "Golden Sun.sav", kind="saves"),
                    }
                ],
            }

            apply_plan_to_local_target(
                plan,
                target_root=target_root,
                config=_config(),
            )

            self.assertTrue((target_root / "Golden Sun.srm").exists())
            self.assertFalse((target_root / "Golden Sun.sav").exists())
            self.assertEqual(
                (target_root / "Golden Sun.srm").read_bytes(),
                b"save",
            )

    def test_download_psx_save_runs_structural_conversion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store_root = root / "store"
            target_root = root / "target"
            store_saves = store_root / "systems/psx/saves"
            store_saves.mkdir(parents=True)
            target_root.mkdir()

            source_file = store_saves / "Final Fantasy 9 (FR).sav"
            source_file.write_bytes(_raw_psx_card())
            plan = {
                "mode": "download",
                "source": "s3",
                "target": "thor",
                "actions": [
                    {
                        "operation": "download",
                        "reason": "added",
                        "source": _item(
                            source_file,
                            "Final Fantasy 9 (FR).sav",
                            kind="saves",
                            system="psx",
                        ),
                    }
                ],
            }
            config = _config()
            config["systems"]["psx"] = {
                "save_conversion": {
                    "strategy": "psx_raw_memory_card",
                    "expected_raw_card_size": 131072,
                    "mister_to_thor": {
                        "accepted_input_extensions": [".sav"],
                        "output_extension": ".srm",
                        "validate_raw_card_size": True,
                    },
                    "thor_to_mister": {
                        "accepted_input_extensions": [".srm", ".mcr", ".mcd"],
                        "output_extension": ".sav",
                        "validate_raw_card_size": True,
                    },
                }
            }
            config["save_mappings"] = {
                "psx": [
                    {
                        "mister_game_folder": "Final Fantasy 9 (FR)",
                        "retroarch_game_file": "Final Fantasy IX.chd",
                    }
                ]
            }

            result = apply_plan_to_local_target(
                plan,
                target_root=target_root,
                config=config,
            )

            self.assertTrue((target_root / "Final Fantasy IX.srm").exists())
            self.assertEqual(
                (target_root / "Final Fantasy IX.srm").read_bytes(),
                source_file.read_bytes(),
            )
            self.assertEqual(
                result["applied"][0]["conversion"]["input_format"],
                "raw_psx_memory_card",
            )

    def test_download_psx_save_refuses_canonical_conversion_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store_root = root / "store"
            target_root = root / "target"
            store_saves = store_root / "systems/psx/saves"
            store_saves.mkdir(parents=True)
            target_root.mkdir()

            source_file = store_saves / "Final Fantasy 9 (FR).sav"
            source_file.write_bytes(_raw_psx_card())
            source_item = _item(
                source_file,
                "Final Fantasy 9 (FR).sav",
                kind="saves",
                system="psx",
            )
            source_item["canonical_sha256"] = "0" * 64
            source_item["canonical_format"] = "psx_raw_memory_card_v2"
            plan = {
                "mode": "download",
                "source": "s3",
                "target": "thor",
                "actions": [
                    {
                        "operation": "download",
                        "reason": "added",
                        "source": source_item,
                    }
                ],
            }
            config = _config()
            config["systems"]["psx"] = {
                "save_conversion": {
                    "strategy": "psx_raw_memory_card",
                    "expected_raw_card_size": 131072,
                    "mister_to_thor": {
                        "accepted_input_extensions": [".sav"],
                        "output_extension": ".srm",
                        "validate_raw_card_size": True,
                    },
                    "thor_to_mister": {
                        "accepted_input_extensions": [".srm"],
                        "output_extension": ".sav",
                        "validate_raw_card_size": True,
                    },
                }
            }

            with self.assertRaises(ApplyError):
                apply_plan_to_local_target(
                    plan,
                    target_root=target_root,
                    config=config,
                )


def _item(
    path: Path,
    content_path: str,
    kind: str = "games",
    system: str = "gba",
) -> dict:
    size = path.stat().st_size if path.exists() else 0
    sha256 = _sha256(path) if path.exists() else "missing"
    return {
        "device": "test",
        "system": system,
        "type": kind,
        "absolute_path": str(path),
        "relative_path": content_path,
        "content_path": content_path,
        "sync_key": f"systems/{system}/{kind}/{content_path}",
        "size": size,
        "native_size": size,
        "canonical_size": size,
        "modified_ns": 1,
        "sha256": sha256,
        "native_sha256": sha256,
        "canonical_sha256": sha256,
    }


def _raw_psx_card() -> bytes:
    data = bytearray(131072)
    data[0:2] = b"MC"
    for entry in range(15):
        offset = (entry + 1) * 128
        data[offset] = 0xA0
        data[offset + 8 : offset + 10] = (0xFFFF).to_bytes(2, byteorder="little")
    offset = 128
    data[offset] = 0x51
    data[offset + 4 : offset + 8] = (8192).to_bytes(4, byteorder="little")
    data[offset + 8 : offset + 10] = (0xFFFF).to_bytes(2, byteorder="little")
    data[offset + 10 : offset + 26] = b"BASCUS-00000SAVE"
    data[8192:8194] = b"SC"
    for frame_index in range(16, 64):
        start = frame_index * 128
        data[start : start + 4] = b"\xFF\xFF\xFF\xFF"
        data[start + 8 : start + 10] = (0xFFFF).to_bytes(2, byteorder="little")
    for frame_index in range(16):
        start = frame_index * 128
        checksum = 0
        for byte in data[start : start + 127]:
            checksum ^= byte
        data[start + 127] = checksum
    return bytes(data)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        digest.update(handle.read())
    return digest.hexdigest()


def _config() -> dict:
    return {
        "devices": {
            "mister": {"local": {"root": "/media/fat"}},
            "thor": {"local": {"root": "/storage/emulated/0"}},
        },
        "systems": {
            "gba": {
                "save_conversion": {
                    "strategy": "raw_same_content",
                    "mister_to_thor": {"rename_extension_to": ".srm"},
                    "thor_to_mister": {"rename_extension_to": ".sav"},
                }
            }
        },
    }


if __name__ == "__main__":
    unittest.main()
