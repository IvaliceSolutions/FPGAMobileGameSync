from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fpgmobilegamesync.doctor import run_doctor


class DoctorTests(unittest.TestCase):
    def test_doctor_accepts_minimal_local_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = _config(root)

            result = run_doctor(config=config)

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["summary"]["error"], 0)

    def test_doctor_reports_missing_s3_environment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = _config(Path(tmp))

            with patch.dict("os.environ", {}, clear=True):
                result = run_doctor(config=config, backend="s3")

            self.assertEqual(result["status"], "error")
            self.assertEqual(result["summary"]["error"], 3)
            self.assertEqual(
                {check["context"]["env"] for check in result["checks"] if check["code"] == "missing_s3_env"},
                {"FPGMS_ENDPOINT", "FPGMS_KEY", "FPGMS_SECRET"},
            )

    def test_doctor_reports_missing_sftp_remote_environment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = _config(Path(tmp))
            config["devices"]["mister"]["remote"] = _remote_config("mister")
            config["devices"]["thor"]["remote"] = _remote_config("thor")

            with patch.dict("os.environ", {}, clear=True):
                result = run_doctor(config=config, check_remote=True)

            self.assertEqual(result["status"], "error")
            self.assertTrue(result["remote_checked"])
            self.assertEqual(
                {
                    (check["context"]["device"], check["context"]["env"])
                    for check in result["checks"]
                    if check["code"] == "missing_sftp_username_env"
                },
                {("mister", "MISTER_USER"), ("thor", "THOR_USER")},
            )
            self.assertEqual(
                {
                    (check["context"]["device"], tuple(check["context"]["envs"]))
                    for check in result["checks"]
                    if check["code"] == "missing_sftp_auth_env"
                },
                {
                    ("mister", ("MISTER_PASSWORD", "MISTER_PRIVATE_KEY")),
                    ("thor", ("THOR_PASSWORD", "THOR_PRIVATE_KEY")),
                },
            )

    def test_doctor_accepts_sftp_remote_environment_when_one_auth_method_is_set(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = _config(Path(tmp))
            config["devices"]["mister"]["remote"] = _remote_config("mister")
            config["devices"]["thor"]["remote"] = _remote_config("thor")

            with patch.dict(
                "os.environ",
                {
                    "MISTER_USER": "root",
                    "MISTER_PRIVATE_KEY": "/keys/mister",
                    "THOR_USER": "android",
                    "THOR_PASSWORD": "secret",
                },
                clear=True,
            ):
                result = run_doctor(config=config, check_remote=True)

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["summary"]["error"], 0)

    def test_doctor_path_check_warns_for_unmounted_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = _config(root)
            config["devices"]["mister"]["local"]["root"] = str(root / "missing")

            result = run_doctor(config=config, check_paths=True)

            self.assertEqual(result["status"], "warning")
            self.assertGreater(result["summary"]["warning"], 0)
            self.assertTrue(
                any(check["code"] == "device_root_missing" for check in result["checks"])
            )

    def test_doctor_validates_sync_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = _config(Path(tmp))
            config["sync_profiles"] = {
                "bad-profile": {
                    "direction": "missing-direction",
                    "backend": "ftp",
                    "source_backend": "adb",
                    "systems": ["unknown-system"],
                    "types": ["unknown-type"],
                }
            }

            result = run_doctor(config=config)

            self.assertEqual(result["status"], "error")
            self.assertEqual(
                {
                    check["code"]
                    for check in result["checks"]
                    if check["code"].startswith("invalid_sync_profile")
                },
                {
                    "invalid_sync_profile_direction",
                    "invalid_sync_profile_backend",
                    "invalid_sync_profile_device_backend",
                    "invalid_sync_profile_list_value",
                },
            )

    def test_doctor_reports_missing_optional_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = _config(Path(tmp))
            config["devices"]["mister"]["remote"] = _remote_config("mister")
            config["devices"]["thor"]["remote"] = _remote_config("thor")

            with patch("fpgmobilegamesync.doctor.find_spec", return_value=None):
                result = run_doctor(
                    config=config,
                    backend="s3",
                    check_remote=True,
                    check_dependencies=True,
                )

            self.assertTrue(result["dependencies_checked"])
            self.assertEqual(result["status"], "error")
            self.assertEqual(
                {
                    check["context"]["module"]
                    for check in result["checks"]
                    if check["code"] == "missing_python_dependency"
                },
                {"boto3", "paramiko"},
            )

    def test_doctor_accepts_available_optional_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = _config(Path(tmp))
            config["devices"]["mister"]["remote"] = _remote_config("mister")

            with patch("fpgmobilegamesync.doctor.find_spec", return_value=object()):
                with patch.dict(
                    "os.environ",
                    {
                        "MISTER_USER": "root",
                        "MISTER_PRIVATE_KEY": "/keys/mister",
                    },
                    clear=True,
                ):
                    result = run_doctor(
                        config=config,
                        devices=["mister"],
                        check_remote=True,
                        check_dependencies=True,
                    )

            self.assertEqual(result["status"], "ok")
            self.assertTrue(
                any(
                    check["code"] == "python_dependency_available"
                    and check["context"]["module"] == "paramiko"
                    for check in result["checks"]
                )
            )

    def test_doctor_infers_backend_and_remote_device_from_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = _config(Path(tmp))
            config["devices"]["mister"]["remote"] = _remote_config("mister")
            config["devices"]["thor"]["remote"] = _remote_config("thor")

            with patch.dict("os.environ", {}, clear=True):
                result = run_doctor(config=config, profiles=["thor-pull"])

            self.assertEqual(result["backend"], "s3")
            self.assertEqual(result["profiles"], ["thor-pull"])
            self.assertEqual(result["devices"], ["mister", "thor"])
            self.assertEqual(result["remote_devices"], ["mister"])
            self.assertTrue(result["remote_checked"])
            self.assertEqual(
                {
                    check["context"]["env"]
                    for check in result["checks"]
                    if check["code"] == "missing_sftp_username_env"
                },
                {"MISTER_USER"},
            )
            self.assertEqual(
                {
                    check["context"]["env"]
                    for check in result["checks"]
                    if check["code"] == "missing_s3_env"
                },
                {"FPGMS_ENDPOINT", "FPGMS_KEY", "FPGMS_SECRET"},
            )

    def test_doctor_reports_unknown_selected_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = run_doctor(config=_config(Path(tmp)), profiles=["missing"])

            self.assertEqual(result["status"], "error")
            self.assertTrue(
                any(check["code"] == "unknown_sync_profile" for check in result["checks"])
            )

    def test_cli_doctor_returns_non_zero_on_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.json"
            config_path.write_text(json.dumps(_config(root)), encoding="utf-8")

            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "fpgmobilegamesync.cli",
                    "--config",
                    str(config_path),
                    "doctor",
                    "--backend",
                    "s3",
                ],
                cwd=Path.cwd(),
                text=True,
                capture_output=True,
            )

            self.assertEqual(completed.returncode, 1)
            self.assertEqual(json.loads(completed.stdout)["status"], "error")

    def test_cli_doctor_profile_uses_profile_backend(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = _config(root)
            config["devices"]["mister"]["remote"] = _remote_config("mister")
            config["devices"]["thor"]["remote"] = _remote_config("thor")
            config_path = root / "config.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")

            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "fpgmobilegamesync.cli",
                    "--config",
                    str(config_path),
                    "doctor",
                    "--profile",
                    "thor-pull",
                ],
                cwd=Path.cwd(),
                text=True,
                capture_output=True,
            )

            result = json.loads(completed.stdout)
            self.assertEqual(completed.returncode, 1)
            self.assertEqual(result["backend"], "s3")
            self.assertEqual(result["remote_devices"], ["mister"])


def _config(root: Path) -> dict:
    return {
        "defaults": {
            "systems": ["gba"],
            "types": ["saves"],
        },
        "s3": {
            "bucket": "bucket",
            "endpoint_url_env": "FPGMS_ENDPOINT",
            "access_key_id_env": "FPGMS_KEY",
            "secret_access_key_env": "FPGMS_SECRET",
        },
        "devices": {
            "mister": {
                "local": {
                    "root": str(root),
                    "trash": str(root / ".sync_trash"),
                }
            },
            "thor": {
                "local": {
                    "root": str(root),
                    "trash": str(root / "RetroArch/.sync_trash"),
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
        "sync_profiles": {
            "thor-pull": {
                "direction": "mister-to-thor",
                "backend": "s3",
                "source_backend": "sftp",
                "target_backend": "local",
                "systems": ["gba"],
                "types": ["saves"],
            },
            "third-mister-to-thor": {
                "direction": "mister-to-thor",
                "backend": "s3",
                "scan_backend": "sftp",
            },
        },
        "systems": {
            "gba": {
                "paths": {
                    "mister": {
                        "saves": "saves/GBA",
                    },
                    "thor": {
                        "saves": "RetroArch/saves/mGBA",
                    },
                },
                "file_extensions": {
                    "saves": {
                        "mister": [".sav"],
                        "thor": [".srm"],
                    },
                },
            }
        },
    }


def _remote_config(prefix: str) -> dict:
    upper = prefix.upper()
    return {
        "protocol": "sftp",
        "host": f"{prefix}.local",
        "port": 22,
        "username_env": f"{upper}_USER",
        "password_env": f"{upper}_PASSWORD",
        "private_key_env": f"{upper}_PRIVATE_KEY",
        "root": "/remote/root",
        "trash": "/remote/root/.sync_trash",
    }


if __name__ == "__main__":
    unittest.main()
