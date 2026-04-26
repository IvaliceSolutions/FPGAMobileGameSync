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
                    },
                },
            }
        },
    }


if __name__ == "__main__":
    unittest.main()
